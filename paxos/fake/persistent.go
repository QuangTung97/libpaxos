package fake

import "github.com/QuangTung97/libpaxos/paxos"

func NewNodeID(num int) paxos.NodeID {
	return paxos.NodeID{100, byte(num)}
}

type PersistentStateFake struct {
	LastValue paxos.TermValue
	NodeID    paxos.NodeID
}

var _ paxos.PersistentState = &PersistentStateFake{}

func (s *PersistentStateFake) NextProposeTerm() paxos.TermNum {
	s.LastValue++
	return paxos.TermNum{
		Num:    s.LastValue,
		NodeID: s.NodeID,
	}
}

func (s *PersistentStateFake) RecordLastTermValue(lastValue paxos.TermValue) {
	if lastValue > s.LastValue {
		s.LastValue = lastValue
	}
}

func (s *PersistentStateFake) GetNodeID() paxos.NodeID {
	return s.NodeID
}
