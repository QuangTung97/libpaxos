package paxos

type LeaderLogGetter interface {
	GetCommittedInfo() CommittedInfo
}

type StateMachineLogGetter interface {
	GetCommittedEntriesWithWait(fromPos LogPos, limit int) []PosLogEntry
}

type LogStorage interface {
	LeaderLogGetter

	UpsertEntries(entries []PosLogEntry)

	MarkCommitted(posList ...LogPos)

	// GetEntries does not wait when not found any entries from log pos
	GetEntries(from LogPos, limit int) []PosLogEntry
}

type PosLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}
