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

	// UpsertEntriesV1 TODO update
	UpsertEntriesV1(entries []PosLogEntry, markCommitted []LogPos)
	SetTerm(term TermNum)

	// GetTerm is not required to be thread safe
	GetTerm() TermNum

	// GetFullyReplicated is not required to be thread safe
	GetFullyReplicated() LogPos
}

// PosLogEntry TODO remove
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

func NewPosLogEntryListValues(entries ...LogEntry) []PosLogEntry {
	return NewPosLogEntryList(entries)
}

func NewPosLogEntryList(entries []LogEntry) []PosLogEntry {
	if entries == nil {
		return nil
	}

	result := make([]PosLogEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, PosLogEntry{
			Pos:   e.Pos,
			Entry: e,
		})
	}
	return result
}

func UnwrapPosLogEntryList(entries []PosLogEntry) []LogEntry {
	if entries == nil {
		return nil
	}
	result := make([]LogEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, e.Entry)
	}
	return result
}
