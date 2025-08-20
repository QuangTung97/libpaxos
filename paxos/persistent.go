package paxos

// PersistentState does not have to be thread safe
type PersistentState interface {
	UpdateLastTerm(lastTerm TermNum)
	GetLastTerm() TermNum

	GetNodeID() NodeID

	UpdateForceStayAsFollower(maxPos LogPos, stayed bool)
	GetForceStayAsFollower() (LogPos, bool)
}
