package paxos

// PersistentState does not have to be thread safe
type PersistentState interface {
	NextProposeTerm() TermNum
	RecordLastTerm(lastTerm TermNum) bool
	GetLastTerm() TermNum
	GetNodeID() NodeID
}
