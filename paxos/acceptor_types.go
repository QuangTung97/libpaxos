package paxos

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
	Pos     LogPos
	IsFinal bool // when true => it's the final log entry marker and Entry is null
	Entry   LogEntry
}

type AcceptEntriesInput struct {
	ToNode    NodeID
	Term      TermNum
	Entries   []AcceptLogEntry
	Committed LogPos
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
	Members            []MemberInfo
	FullyReplicatedPos LogPos
}
