package paxos

import "context"

type LeaderLogGetter interface {
	GetCommittedInfo() CommittedInfo

	// GetEntries does not wait when not found any entries from log pos
	GetEntries(from LogPos, limit int) []PosLogEntry
}

// StateMachineLogGetter is implemented by both CoreLogic & AcceptorLogic
type StateMachineLogGetter interface {
	GetCommittedEntriesWithWait(
		ctx context.Context, term TermNum,
		fromPos LogPos, limit int,
	) ([]PosLogEntry, error)
}

type LogStorage interface {
	LeaderLogGetter

	// -----------------------------------------------------
	// Follower functions are not required to be thread safe
	// -----------------------------------------------------

	UpsertEntries(entries []PosLogEntry)

	MarkCommitted(posList ...LogPos)

	SetTerm(term TermNum)
	GetTerm() TermNum
	GetFullyReplicated() LogPos
}

type PosLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}
