package paxos

import "context"

type LeaderLogGetter interface {
	GetCommittedInfo() CommittedInfo

	// GetEntries does not wait when not found any entries from log pos
	GetEntries(from LogPos, limit int) []PosLogEntry
}

type StateMachineLogGetter interface {
	GetCommittedEntriesWithWait(
		ctx context.Context, fromPos LogPos, limit int,
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
}

type PosLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}
