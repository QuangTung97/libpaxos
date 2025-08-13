package paxos

type ReplicatedLog interface {
	HandleRequestVote(input RequestVoteInput) RequestVoteOutput
	AcceptEntries(input AcceptEntriesInput)
}

type RequestVoteInput struct {
	ToNode  NodeID
	Term    TermNum
	FromPos LogPos
	Limit   int
}

type RequestVoteOutput struct {
	Success bool
	Term    TermNum
	Entries []VoteLogEntry
}

type VoteLogEntry struct {
	Pos   LogPos
	Entry LogEntry
	More  bool
}

type AcceptEntriesInput struct {
	ToNode  NodeID
	Term    TermNum
	Entries []AcceptLogEntry
}

type AcceptLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}

type AcceptEntriesOutput struct {
	Success bool
	Term    TermNum
	PosList []LogPos
}
