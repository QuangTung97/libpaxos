package paxos

import "context"

// LeaderLogGetter must be a thread safe object
type LeaderLogGetter interface {
	GetCommittedInfo() CommittedInfo

	// GetEntries does not wait when not found any entries from log pos
	GetEntries(from LogPos, limit int) []PosLogEntry

	GetEntriesWithPos(posList ...LogPos) []PosLogEntry
}

// StateMachineLogGetter is implemented by both CoreLogic & AcceptorLogic
type StateMachineLogGetter interface {
	GetCommittedEntriesWithWait(
		ctx context.Context, term TermNum,
		fromPos LogPos, limit int,
	) (GetCommittedEntriesOutput, error)
}

type GetCommittedEntriesOutput struct {
	Entries []PosLogEntry
	NextPos LogPos
}

type LogStorage interface {
	LeaderLogGetter

	// ----------------------------------------------------------------------
	// Follower functions are only required to not race with LeaderLogGetter
	// ----------------------------------------------------------------------

	UpsertEntries(entries []PosLogEntry, markCommitted []LogPos)
	SetTerm(term TermNum)

	// GetTerm is not required to be thread safe
	GetTerm() TermNum

	// GetFullyReplicated is not required to be thread safe
	GetFullyReplicated() LogPos
}

type PosLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}

func PosLogEntryEqual(a, b PosLogEntry) bool {
	if a.Pos != b.Pos {
		return false
	}
	return LogEntryEqual(a.Entry, b.Entry)
}
