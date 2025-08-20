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
	GetEntries(from LogPos, limit int) []PosLogEntry
}

type PosLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}
