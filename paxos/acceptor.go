package paxos

type AcceptorLogic interface {
	HandleRequestVote(input RequestVoteInput) RequestVoteOutput
	AcceptEntries(input AcceptEntriesInput)

	GetCommittedInfo() CommittedInfo
}

type RequestVoteInput struct {
	ToNode  NodeID
	Term    TermNum
	FromPos LogPos
}

type RequestVoteOutput struct {
	Success bool
	Term    TermNum
	Entries []VoteLogEntry
}

type VoteLogEntry struct {
	Pos   LogPos
	More  bool
	Entry LogEntry
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

type CommittedInfo struct {
	Members []MemberInfo
	Pos     LogPos
}
