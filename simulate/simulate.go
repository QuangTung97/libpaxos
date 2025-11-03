package simulate

import (
	"github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

var (
	nodeID1 = fake.NewNodeID(1)
	nodeID2 = fake.NewNodeID(2)
	nodeID3 = fake.NewNodeID(3)
	nodeID4 = fake.NewNodeID(4)
	nodeID5 = fake.NewNodeID(5)
	nodeID6 = fake.NewNodeID(6)

	initTerm = paxos.TermNum{
		Num:    20,
		NodeID: nodeID3,
	}
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
		5,
		true,
		5000,
		0,
	)

	return s
}
