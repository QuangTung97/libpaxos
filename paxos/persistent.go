package paxos

type PersistentState interface {
	NextProposeTerm() TermNum

	RecordLastTerm(lastTerm TermNum)
	GetLastTerm() TermNum

	GetNodeID() NodeID
}
