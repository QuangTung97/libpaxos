package fake

import (
	"sync"

	"github.com/QuangTung97/libpaxos/paxos"
)

type LogStorageFake struct {
	mut                sync.Mutex
	logEntries         []paxos.LogEntry
	lastMembers        []paxos.MemberInfo
	fullyReplicatedPos paxos.LogPos

	Term paxos.TermNum

	UpsertList []UpsertInput
}

type UpsertInput struct {
	PutList  []paxos.LogPos
	MarkList []paxos.LogPos
}

var _ paxos.LogStorage = &LogStorageFake{}

func (s *LogStorageFake) UpsertEntries(entries []paxos.PosLogEntry, markCommitted []paxos.LogPos) {
	s.mut.Lock()
	defer s.mut.Unlock()

	var putList []paxos.LogPos
	for _, e := range entries {
		putList = append(putList, e.Pos)
	}
	s.UpsertList = append(s.UpsertList, UpsertInput{
		PutList:  putList,
		MarkList: markCommitted,
	})

	// ----------------------------------------------
	// Implementation
	// ----------------------------------------------

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
		if e.Pos <= s.fullyReplicatedPos {
			continue
		}
		index := e.Pos - 1
		newEntries[index] = e.Entry
	}
	s.logEntries = newEntries

	s.doMarkCommitted(markCommitted)
	s.increaseFullyReplicated()
}

func (s *LogStorageFake) increaseFullyReplicated() {
	maxPos := paxos.LogPos(len(s.logEntries))

	for pos := s.fullyReplicatedPos + 1; pos <= maxPos; pos++ {
		index := pos - 1
		entry := s.logEntries[index]

		if entry.Type == paxos.LogTypeNull {
			break
		}
		if entry.Term.IsFinite {
			break
		}

		s.fullyReplicatedPos = pos

		if entry.Type == paxos.LogTypeMembership {
			s.lastMembers = entry.Members
		}
	}
}

func (s *LogStorageFake) doMarkCommitted(posList []paxos.LogPos) {
	for _, pos := range posList {
		if pos <= s.fullyReplicatedPos {
			return // TODO testing
		}
		index := pos - 1
		s.logEntries[index].Term = paxos.InfiniteTerm{}
	}
}

func (s *LogStorageFake) GetCommittedInfo() paxos.CommittedInfo {
	s.mut.Lock()
	defer s.mut.Unlock()

	return paxos.CommittedInfo{
		Members:         s.lastMembers,
		FullyReplicated: s.fullyReplicatedPos,
	}
}

func (s *LogStorageFake) GetEntries(from paxos.LogPos, limit int) []paxos.PosLogEntry {
	s.mut.Lock()
	defer s.mut.Unlock()

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

func (s *LogStorageFake) GetEntriesWithPos(posList ...paxos.LogPos) []paxos.PosLogEntry {
	s.mut.Lock()
	defer s.mut.Unlock()

	maxPos := paxos.LogPos(len(s.logEntries))

	result := make([]paxos.PosLogEntry, 0)
	for _, pos := range posList {
		if pos > maxPos {
			panic("Outside of log range")
		}

		index := pos - 1
		result = append(result, paxos.PosLogEntry{
			Pos:   pos,
			Entry: s.logEntries[index],
		})
	}

	return result
}

func (s *LogStorageFake) SetTerm(term paxos.TermNum) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.Term = term
}

func (s *LogStorageFake) GetTerm() paxos.TermNum {
	return s.Term
}

func (s *LogStorageFake) GetFullyReplicated() paxos.LogPos {
	return s.fullyReplicatedPos
}
