package simulate

import (
	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type Simulation struct {
}

type NodeState struct {
	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	core       paxos.CoreLogic
	now        paxos.TimestampMilli
}

func NewNodeState(id paxos.NodeID) *NodeState {
	s := &NodeState{
		now: 180_000,
	}

	s.persistent = &fake.PersistentStateFake{
		NodeID:   id,
		LastTerm: initTerm,
	}
	s.log = &fake.LogStorageFake{}

	s.core = paxos.NewCoreLogic(
		s.persistent,
		s.log,
		nil, // TODO
		func() paxos.TimestampMilli {
			return s.now
		},
		async.SimpleAddNextFunc,
		5,
		true,
		5000,
		0,
	)

	return s
}
