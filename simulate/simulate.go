package simulate

import (
	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type Simulation struct {
	runtime *async.SimulateRuntime
	now     paxos.TimestampMilli
}

type NodeState struct {
	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *RunnerFake

	core     paxos.CoreLogic
	acceptor paxos.AcceptorLogic
}

func NewNodeState(sim *Simulation, id paxos.NodeID) *NodeState {
	s := &NodeState{}

	s.persistent = &fake.PersistentStateFake{
		NodeID:   id,
		LastTerm: initTerm,
	}
	s.log = fake.NewLogStorageFake()

	s.runner = NewRunnerFake(
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
