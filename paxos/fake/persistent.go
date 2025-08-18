package fake

import "github.com/QuangTung97/libpaxos/paxos"

func NewNodeID(num int) paxos.NodeID {
	return paxos.NodeID{100, byte(num)}
}

type PersistentStateFake struct {
	NodeID   paxos.NodeID
	LastTerm paxos.TermNum
}

var _ paxos.PersistentState = &PersistentStateFake{}

func (s *PersistentStateFake) NextProposeTerm() paxos.TermNum {
	s.LastTerm.Num++
	s.LastTerm.NodeID = s.NodeID
	return s.LastTerm
}

func (s *PersistentStateFake) RecordLastTerm(lastTerm paxos.TermNum) bool {
	// input <= existed => do nothing
	if paxos.CompareTermNum(lastTerm, s.LastTerm) <= 0 {
		return false
	}
	s.LastTerm = lastTerm
	return true
}

func (s *PersistentStateFake) GetLastTerm() paxos.TermNum {
	return s.LastTerm
}
