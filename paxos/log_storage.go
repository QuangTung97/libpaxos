package paxos

type LogStorage interface {
	UpsertEntries(entries []PosLogEntry)
	GetCommittedInfo() CommittedInfo
}

type PosLogEntry struct {
	Pos   LogPos
	Entry LogEntry
}
