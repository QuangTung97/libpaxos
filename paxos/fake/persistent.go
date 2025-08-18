package fake

import "github.com/QuangTung97/libpaxos/paxos"

func NewNodeID(num int) paxos.NodeID {
	return paxos.NodeID{100, byte(num)}
}

type PersistentStateFake struct {
	NodeID              paxos.NodeID
	HighestProposeValue paxos.TermValue
	LastTerm            paxos.TermNum
}

var _ paxos.PersistentState = &PersistentStateFake{}

func (s *PersistentStateFake) NextProposeTerm() paxos.TermNum {
	s.HighestProposeValue++
	return paxos.TermNum{
		Num:    s.HighestProposeValue,
		NodeID: s.NodeID,
	}
}

func (s *PersistentStateFake) RecordLastTerm(lastTerm paxos.TermNum) {
	if lastTerm.Num > s.HighestProposeValue {
		s.HighestProposeValue = lastTerm.Num
	}

	// input > existed
	if paxos.CompareTermNum(lastTerm, s.LastTerm) > 0 {
		s.LastTerm = lastTerm
	}
}

func (s *PersistentStateFake) GetLastTerm() paxos.TermNum {
	return s.LastTerm
}

func (s *PersistentStateFake) GetNodeID() paxos.NodeID {
	return s.NodeID
}
