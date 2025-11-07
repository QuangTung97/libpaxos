package simulate

import (
	"fmt"

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
	sim *Simulation

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
		sim: sim,
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

	s.core = paxos.NewCoreLogic(
		s.persistent,
		s.log,
		s.runner,
		func() paxos.TimestampMilli {
			return sim.now
		},
		sim.runtime.AddNext,
		5,
		true,
		5000,
		0,
	)

	s.acceptor = paxos.NewAcceptorLogic(
		id,
		s.log,
		3,
	)

	return s
}

func (s *NodeState) voteRunnerFunc(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
}

func (s *NodeState) acceptRunnerFunc(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
}

func (s *NodeState) fetchFollowerRunnerFunc(
	ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum, generation paxos.FollowerGeneration,
) {
}

func (s *NodeState) stateMachineFunc(
	ctx async.Context, term paxos.TermNum, info paxos.StateMachineRunnerInfo,
) {
}

func (s *NodeState) startElectionFunc(
	ctx async.Context, chosen paxos.NodeID, termValue paxos.TermValue,
) {
}

func (s *Simulation) printAllActions() {
	fmt.Println("============================================================")
	for index, detail := range s.runtime.GetQueueDetails() {
		fmt.Printf("(%02d) %s\n", index+1, detail)
	}
}
