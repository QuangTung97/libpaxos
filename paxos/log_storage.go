package paxos

import "context"

// LeaderLogGetter must be a thread safe object
type LeaderLogGetter interface {
	GetCommittedInfo() CommittedInfo

	// GetEntries does not wait when not found any entries from log pos
	GetEntries(from LogPos, limit int) []LogEntry

	GetEntriesWithPos(posList ...LogPos) []LogEntry
}

// StateMachineLogGetter is implemented by both CoreLogic & AcceptorLogic
type StateMachineLogGetter interface {
	GetCommittedEntriesWithWait(
		ctx context.Context, term TermNum,
		fromPos LogPos, limit int,
	) (GetCommittedEntriesOutput, error)
}

type GetCommittedEntriesOutput struct {
	Entries []LogEntry
	NextPos LogPos
}

type LogStorage interface {
	LeaderLogGetter

	// ----------------------------------------------------------------------
	// Follower functions are only required to not race with LeaderLogGetter
	// ----------------------------------------------------------------------

	UpsertEntries(entries []LogEntry, markCommitted []LogPos)
	SetTerm(term TermNum)

	// GetTerm is not required to be thread safe
	GetTerm() TermNum

	// GetFullyReplicated is not required to be thread safe
	GetFullyReplicated() LogPos
}
