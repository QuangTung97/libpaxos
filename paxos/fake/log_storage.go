package fake

import "github.com/QuangTung97/libpaxos/paxos"

type LogStorageFake struct {
	Entries []paxos.LogEntry
}

var _ paxos.LogStorage = &LogStorageFake{}

func (s *LogStorageFake) UpsertEntries(entries []paxos.PosLogEntry) {
	newLen := len(s.Entries)

	maxPos := paxos.LogPos(0)
	for _, e := range entries {
		maxPos = max(maxPos, e.Pos)
	}

	if newLen < int(maxPos) {
		newLen = int(maxPos)
	}

	newEntries := make([]paxos.LogEntry, newLen)
	copy(newEntries, s.Entries)

	for _, e := range entries {
		index := e.Pos - 1
		newEntries[index] = e.Entry
	}

	s.Entries = newEntries
}

func (s *LogStorageFake) GetCommittedInfo() paxos.CommittedInfo {
	return paxos.CommittedInfo{}
}
