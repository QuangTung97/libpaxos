package fake

import "github.com/QuangTung97/libpaxos/paxos"

type LogStorageFake struct {
	logEntries    []paxos.LogEntry
	lastCommitted paxos.LogPos
	lastMembers   []paxos.MemberInfo
	Term          paxos.TermNum
}

var _ paxos.LogStorage = &LogStorageFake{}

func (s *LogStorageFake) UpsertEntries(entries []paxos.PosLogEntry) {
	newLen := len(s.logEntries)

	maxPos := paxos.LogPos(0)
	for _, e := range entries {
		maxPos = max(maxPos, e.Pos)
	}

	if newLen < int(maxPos) {
		newLen = int(maxPos)
	}

	newEntries := make([]paxos.LogEntry, newLen)
	copy(newEntries, s.logEntries)

	for _, e := range entries {
		index := e.Pos - 1
		newEntries[index] = e.Entry
	}
	s.logEntries = newEntries

	s.increaseLastCommitted()
}

func (s *LogStorageFake) increaseLastCommitted() {
	maxPos := paxos.LogPos(len(s.logEntries))

	for pos := s.lastCommitted + 1; pos <= maxPos; pos++ {
		index := pos - 1
		entry := s.logEntries[index]

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
		s.logEntries[index].Term = paxos.InfiniteTerm{}
	}
	s.increaseLastCommitted()
}

func (s *LogStorageFake) GetCommittedInfo() paxos.CommittedInfo {
	return paxos.CommittedInfo{
		Members:            s.lastMembers,
		FullyReplicatedPos: s.lastCommitted,
	}
}

func (s *LogStorageFake) GetEntries(from paxos.LogPos, limit int) []paxos.PosLogEntry {
	maxPos := paxos.LogPos(len(s.logEntries))

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
			Entry: s.logEntries[index],
		})
	}

	return result
}

func (s *LogStorageFake) SetTerm(term paxos.TermNum) {
	s.Term = term
}

func (s *LogStorageFake) GetTerm() paxos.TermNum {
	return s.Term
}
