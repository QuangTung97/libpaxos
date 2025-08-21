package fake

import "github.com/QuangTung97/libpaxos/paxos"

type LogStorageFake struct {
	Entries       []paxos.LogEntry
	lastCommitted paxos.LogPos
	lastMembers   []paxos.MemberInfo
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

	s.increaseLastCommitted()
}

func (s *LogStorageFake) increaseLastCommitted() {
	maxPos := paxos.LogPos(len(s.Entries))

	for pos := s.lastCommitted + 1; pos <= maxPos; pos++ {
		index := pos - 1
		entry := s.Entries[index]

		if entry.Type == paxos.LogTypeNull {
			break
		}
		if entry.Term.IsFinite {
			break
		}

		s.lastCommitted = pos

		if entry.Type == paxos.LogTypeMembership {
			s.lastMembers = entry.Members
		}
	}
}

func (s *LogStorageFake) MarkCommitted(posList ...paxos.LogPos) {
	for _, pos := range posList {
		index := pos - 1
		s.Entries[index].Term = paxos.InfiniteTerm{}
	}
	s.increaseLastCommitted()
}

func (s *LogStorageFake) GetCommittedInfo() paxos.CommittedInfo {
	return paxos.CommittedInfo{
		Members: s.lastMembers,
		Pos:     s.lastCommitted,
	}
}

func (s *LogStorageFake) GetEntries(from paxos.LogPos, limit int) []paxos.PosLogEntry {
	maxPos := paxos.LogPos(len(s.Entries))

	num := maxPos - from + 1
	if num > paxos.LogPos(limit) {
		num = paxos.LogPos(limit)
	}

	maxPos = from + num - 1

	result := make([]paxos.PosLogEntry, 0)
	for pos := from; pos <= maxPos; pos++ {
		index := pos - 1
		result = append(result, paxos.PosLogEntry{
			Pos:   pos,
			Entry: s.Entries[index],
		})
	}

	return result
}
