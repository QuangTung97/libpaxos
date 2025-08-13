package paxos

type PersistentState interface {
	NextProposeTerm() TermNum
	RecordLastTermValue(lastValue TermValue)
	GetNodeID() NodeID
}
