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
	IsFinal bool     // when true => it's the final log entry marker and Entry is null
	Entry   LogEntry // must be a null log entry when IsFinal = true
}

type AcceptEntriesInput struct {
	ToNode    NodeID
	Term      TermNum
	Entries   []LogEntry
	NextPos   LogPos // only to determine the next listening pos
	Committed LogPos
}

type AcceptEntriesOutput struct {
	Success bool
	Term    TermNum
	PosList []LogPos
}

type CommittedInfo struct {
	Members         []MemberInfo
	FullyReplicated LogPos
	PrevPointer     PreviousPointer
}

type NeedReplicatedInput struct {
	Term     TermNum
	FromNode NodeID
	PosList  []LogPos
	NextPos  LogPos // only to determine the next listening pos

	FullyReplicated LogPos
}
