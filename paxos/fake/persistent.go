package fake

import "github.com/QuangTung97/libpaxos/paxos"

func NewNodeID(num int) paxos.NodeID {
	return paxos.NodeID{100, byte(num)}
}

type PersistentStateFake struct {
	NodeID   paxos.NodeID
	LastTerm paxos.TermNum

	StayMaxPos     paxos.LogPos
	StayAsFollower bool
}

var _ paxos.PersistentState = &PersistentStateFake{}

func (s *PersistentStateFake) UpdateLastTerm(lastTerm paxos.TermNum) {
	s.LastTerm = lastTerm
}

func (s *PersistentStateFake) GetLastTerm() paxos.TermNum {
	return s.LastTerm
}

func (s *PersistentStateFake) GetNodeID() paxos.NodeID {
	return s.NodeID
}

func (s *PersistentStateFake) UpdateForceStayAsFollower(maxPos paxos.LogPos, stayed bool) {
	s.StayMaxPos = maxPos
	s.StayAsFollower = stayed
}

func (s *PersistentStateFake) GetForceStayAsFollower() (paxos.LogPos, bool) {
	return s.StayMaxPos, s.StayAsFollower
}
