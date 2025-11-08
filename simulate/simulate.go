package simulate

import (
	"fmt"
	"sync"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type Simulation struct {
	runtime *async.SimulateRuntime
	now     paxos.TimestampMilli

	stateMap map[paxos.NodeID]*NodeState
}

func NewSimulation(
	allNodes []paxos.NodeID,
	initNodes []paxos.NodeID,
) *Simulation {
	s := &Simulation{}
	s.runtime = async.NewSimulateRuntime()
	s.now = 100_000

	s.stateMap = map[paxos.NodeID]*NodeState{}

	initNodeSet := map[paxos.NodeID]struct{}{}
	for _, id := range initNodes {
		initNodeSet[id] = struct{}{}
	}

	for _, id := range allNodes {
		_, withInitMember := initNodeSet[id]
		state := NewNodeState(s, id, withInitMember, initNodes)
		s.stateMap[id] = state
	}

	return s
}

type NodeState struct {
	currentID paxos.NodeID
	sim       *Simulation

	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *RunnerFake

	core     paxos.CoreLogic
	acceptor paxos.AcceptorLogic
}

func NewNodeState(
	sim *Simulation, id paxos.NodeID,
	withInitMember bool, initNodes []paxos.NodeID,
) *NodeState {
	s := &NodeState{
		currentID: id,
		sim:       sim,
	}

	s.persistent = &fake.PersistentStateFake{
		NodeID:   id,
		LastTerm: initTerm,
	}

	s.log = fake.NewLogStorageFake()
	// init log
	if withInitMember {
		initMembersEntry := paxos.NewMembershipLogEntry(
			1,
			paxos.InfiniteTerm{},
			[]paxos.MemberInfo{
				{CreatedAt: 1, Nodes: initNodes},
			},
		)
		s.log.UpsertEntries([]paxos.LogEntry{initMembersEntry}, nil)
	}

	s.runner = NewRunnerFake(
		id,
		sim.runtime,
		s.voteRunnerFunc,
		s.acceptRunnerFunc,
		s.fetchFollowerRunnerFunc,
		s.stateMachineFunc,
		s.startElectionFunc,
	)

	initKeyWaiterFunc := func(mut *sync.Mutex) async.KeyWaiter[paxos.NodeID] {
		return async.NewSimulateKeyWaiter[paxos.NodeID](s.sim.runtime, func(key paxos.NodeID) string {
			return key.String()[:4]
		})
	}

	s.core = paxos.NewCoreLogic(
		s.persistent,
		s.log,
		s.runner,
		func() paxos.TimestampMilli {
			return sim.now
		},
		initKeyWaiterFunc,
		sim.runtime.AddNext,
		5,
		true,
		5000,
		0,
	)

	s.acceptor = paxos.NewAcceptorLogic(
		id,
		s.log,
		initKeyWaiterFunc,
		3,
	)

	return s
}

func (s *NodeState) voteRunnerFunc(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
	input, err := s.core.GetVoteRequest(term, nodeID)
	if err != nil {
		return
	}

	destState := s.sim.stateMap[nodeID]
	rt := s.sim.runtime

	detailKey := buildVoteDetail(s.currentID, nodeID)
	seqID := rt.NewSequence()

	handleRequestFunc := func(ctx async.Context) {
		getFunc, err := destState.acceptor.HandleRequestVoteAsync(input)
		if err != nil {
			return
		}

		var callback func(ctx async.Context)
		callback = func(ctx async.Context) {
			output, isFinal := getFunc()

			rt.SequenceAddNextDetail(ctx, seqID, detailKey+"::handle-response", func(ctx async.Context) {
				// TODO use async instead
				_ = s.core.HandleVoteResponse(ctx, nodeID, output)
			})

			if isFinal {
				return
			}

			rt.AddNextDetail(ctx, detailKey+"::get-next", callback)
		}
		callback(ctx)
	}

	rt.AddNextDetail(ctx, detailKey+"::follower-recv", func(ctx async.Context) {
		s.core.FollowerReceiveTermNum(input.Term)
		rt.AddNextDetail(ctx, detailKey+"::handle-request", handleRequestFunc)
	})
}

func (s *NodeState) acceptRunnerFunc(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
	rt := s.sim.runtime
	detailKey := buildAcceptDetail(s.currentID, nodeID)

	rt.AddNextDetail(ctx, detailKey+"::setup-accept", func(ctx async.Context) {
		s.doSendAcceptRequest(ctx, nodeID, term)
	})

	rt.AddNextDetail(ctx, detailKey+"::setup-replicate", func(ctx async.Context) {
		s.doSendReplicateRequest(ctx, nodeID, term)
	})
}

func (s *NodeState) doSendAcceptRequest(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
	rt := s.sim.runtime
	destState := s.sim.stateMap[nodeID]

	reqSeqID := rt.NewSequence()
	respSeqID := rt.NewSequence()

	detailKey := buildAcceptDetail(s.currentID, nodeID)

	var getCallback func(ctx async.Context)
	fromPos := paxos.LogPos(0)
	lastCommitted := paxos.LogPos(0)

	getRespCallback := func(input paxos.AcceptEntriesInput, err error) {
		if err != nil {
			return
		}

		rt.SequenceAddNextDetail(ctx, reqSeqID, detailKey+"::follower-recv", func(ctx async.Context) {
			destState.core.FollowerReceiveTermNum(input.Term)
		})

		rt.SequenceAddNextDetail(ctx, reqSeqID, detailKey+"::handle-request", func(ctx async.Context) {
			output, err := destState.acceptor.AcceptEntries(input)
			if err != nil {
				return
			}

			rt.SequenceAddNextDetail(ctx, respSeqID, detailKey+"::handle-response", func(ctx async.Context) {
				_ = s.core.HandleAcceptEntriesResponse(nodeID, output)
			})
		})

		fromPos = input.NextPos
		lastCommitted = input.Committed
		rt.AddNextDetail(ctx, detailKey+"::get-next", getCallback)
	}

	getCallback = func(ctx async.Context) {
		s.core.GetAcceptEntriesRequestAsync(
			ctx, term, nodeID,
			fromPos, lastCommitted,
			getRespCallback,
		)
	}

	getCallback(ctx)
}

func (s *NodeState) doSendReplicateRequest(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
	destState := s.sim.stateMap[nodeID]
	rt := s.sim.runtime
	detailKey := buildAcceptDetail(s.currentID, nodeID)

	requestSeqID := rt.NewSequence()
	responseSeqID := rt.NewSequence()

	fromPos := paxos.LogPos(0)
	lastReplicated := paxos.LogPos(0)

	var rootCallback func(ctx async.Context)

	asyncCallback := func(input paxos.NeedReplicatedInput, err error) {
		if err != nil {
			return
		}

		fromPos = input.NextPos
		lastReplicated = input.FullyReplicated

		rt.SequenceAddNextDetail(ctx, requestSeqID, detailKey+"::get-pos-list", func(ctx async.Context) {
			acceptInput, err := s.core.GetNeedReplicatedLogEntries(input)
			if err != nil {
				return
			}

			rt.SequenceAddNextDetail(ctx, responseSeqID, detailKey+"::replicate-accept", func(ctx async.Context) {
				_, _ = s.acceptor.AcceptEntries(acceptInput)
			})
		})

		rt.AddNextDetail(ctx, detailKey+"::acceptor-get-need-replicate", rootCallback)
	}

	rootCallback = func(ctx async.Context) {
		destState.acceptor.GetNeedReplicatedPosAsync(
			ctx, term,
			fromPos, lastReplicated,
			asyncCallback,
		)
	}
	rootCallback(ctx)
}

func (s *NodeState) fetchFollowerRunnerFunc(
	ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum, generation paxos.FollowerGeneration,
) {
	destState := s.sim.stateMap[nodeID]
	info := destState.core.GetChoosingLeaderInfo()

	rt := s.sim.runtime
	rt.AddNextDetail(ctx, buildFetchDetail(s.currentID, nodeID)+"::handle", func(ctx async.Context) {
		_ = s.core.HandleChoosingLeaderInfo(nodeID, term, generation, info)
	})
}

func (s *NodeState) stateMachineFunc(
	ctx async.Context, term paxos.TermNum, info paxos.StateMachineRunnerInfo,
) {
}

func (s *NodeState) startElectionFunc(
	ctx async.Context, chosen paxos.NodeID, termValue paxos.TermValue,
) {
	destState := s.sim.stateMap[chosen]
	rt := s.sim.runtime

	detail := buildStartElectionDetail(s.currentID, chosen) + "::handle"
	rt.AddNextDetail(ctx, detail, func(ctx async.Context) {
		_, _ = destState.core.StartElection(termValue)
	})
}

func (s *Simulation) printAllActions() {
	fmt.Println("============================================================")
	for index, detail := range s.runtime.GetQueueDetails() {
		fmt.Printf("(%02d) %s\n", index, detail)
	}
}

func (s *Simulation) runActionIndex(index int) {
	s.runtime.RunRandomAction(func(n int) int {
		if index >= n {
			panic("Invalid action")
		}
		return index
	})
}
