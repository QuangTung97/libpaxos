package fake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/paxos"
)

var testCreatedTerm = paxos.TermNum{
	Num:    17,
	NodeID: nodeID3,
}

func newLogList(entries ...paxos.LogEntry) []paxos.LogEntry {
	return entries
}

func TestLogStorageFake(t *testing.T) {
	s := &LogStorageFake{}

	entry1 := paxos.NewCmdLogEntry(
		2,
		paxos.InfiniteTerm{},
		[]byte("hello01"),
		testCreatedTerm,
	)
	entry2 := paxos.NewCmdLogEntry(
		4,
		paxos.InfiniteTerm{},
		[]byte("hello02"),
		testCreatedTerm,
	)

	s.UpsertEntries([]paxos.LogEntry{
		entry1,
		entry2,
	}, nil)

	assert.Equal(t, []paxos.LogEntry{
		{},
		entry1,
		{},
		entry2,
	}, s.logEntries)

	assert.Equal(t, paxos.CommittedInfo{}, s.GetCommittedInfo())
}

func TestLogStorageFake_Membership(t *testing.T) {
	s := &LogStorageFake{}

	members := []paxos.MemberInfo{
		{
			Nodes: []paxos.NodeID{
				NewNodeID(1),
				NewNodeID(2),
				NewNodeID(3),
			},
			CreatedAt: 1,
		},
	}
	entry1 := paxos.NewMembershipLogEntry(
		1,
		paxos.InfiniteTerm{},
		members,
	)

	entry2 := paxos.NewCmdLogEntry(
		2,
		paxos.InfiniteTerm{},
		[]byte("hello01"),
		testCreatedTerm,
	)

	entry3 := paxos.NewCmdLogEntry(
		3,
		paxos.TermNum{
			Num:    41,
			NodeID: NewNodeID(2),
		}.ToInf(),
		[]byte("hello02"),
		testCreatedTerm,
	)

	s.UpsertEntries([]paxos.LogEntry{
		entry1,
		entry2,
		entry3,
	}, nil)

	assert.Equal(t, []paxos.LogEntry{
		entry1, entry2, entry3,
	}, s.logEntries)

	// check committed info
	assert.Equal(t, paxos.CommittedInfo{
		Members:         members,
		FullyReplicated: 2,
		PrevPointer: paxos.PreviousPointer{
			Pos:  2,
			Term: testCreatedTerm,
		},
	}, s.GetCommittedInfo())

	assert.Equal(t, paxos.LogPos(2), s.GetFullyReplicated())
}

func newCmdLog(pos paxos.LogPos, term paxos.TermNum, cmd string) paxos.LogEntry {
	return paxos.NewCmdLogEntry(
		pos,
		term.ToInf(),
		[]byte(cmd),
		testCreatedTerm,
	)
}

func newMembershipLog(pos paxos.LogPos, term paxos.TermNum, nodes ...paxos.NodeID) paxos.LogEntry {
	members := []paxos.MemberInfo{
		{Nodes: nodes, CreatedAt: 1},
	}
	return paxos.NewMembershipLogEntry(
		pos,
		term.ToInf(),
		members,
	)
}

func TestLogStorageFake_MarkCommitted(t *testing.T) {
	s := &LogStorageFake{}

	term := paxos.TermNum{
		Num:    21,
		NodeID: NewNodeID(1),
	}
	entry1 := newCmdLog(2, term, "cmd test 01")
	entry2 := newCmdLog(4, term, "cmd test 02")
	entry2.PrevPointer = paxos.PreviousPointer{
		Pos:  3,
		Term: testCreatedTerm,
	}

	s.UpsertEntries([]paxos.LogEntry{
		entry1,
		entry2,
	}, nil)

	assert.Equal(t, []paxos.LogEntry{
		{},
		entry1,
		{},
		entry2,
	}, s.logEntries)
	assert.Equal(t, paxos.CommittedInfo{}, s.GetCommittedInfo())

	entry3 := newMembershipLog(1, term,
		NewNodeID(1),
		NewNodeID(2),
	)

	// upsert member log entry
	s.UpsertEntries([]paxos.LogEntry{entry3}, nil)
	assert.Equal(t, []paxos.LogEntry{
		entry3,
		entry1,
		{},
		entry2,
	}, s.logEntries)

	// mark committed
	s.UpsertEntries(nil, []paxos.LogPos{1})
	assert.Equal(t, paxos.CommittedInfo{
		FullyReplicated: 1,
		Members:         entry3.Members,
		PrevPointer:     paxos.PreviousPointer{},
	}, s.GetCommittedInfo())

	// mark 2 4 as committed
	s.UpsertEntries(nil, []paxos.LogPos{2, 4})
	assert.Equal(t, paxos.CommittedInfo{
		FullyReplicated: 2,
		Members:         entry3.Members,
		PrevPointer: paxos.PreviousPointer{
			Pos:  2,
			Term: testCreatedTerm,
		},
	}, s.GetCommittedInfo())

	// add committed entry
	entry4 := newCmdLog(3, term, "cmd test 04")
	entry4.Term = paxos.InfiniteTerm{}
	entry4.PrevPointer = entry1.NextPreviousPointer()
	s.UpsertEntries([]paxos.LogEntry{entry4}, nil)
	assert.Equal(t, paxos.CommittedInfo{
		FullyReplicated: 4,
		Members:         entry3.Members,
		PrevPointer:     entry2.NextPreviousPointer(),
	}, s.GetCommittedInfo())

	// get entries
	entries := s.GetEntries(1, 100)

	entry1.Term = paxos.InfiniteTerm{}
	entry2.Term = paxos.InfiniteTerm{}
	entry3.Term = paxos.InfiniteTerm{}

	assert.Equal(t, newLogList(
		entry3, entry1,
		entry4, entry2,
	), entries)

	// get entries, from 3
	entries = s.GetEntries(3, 100)
	assert.Equal(t, newLogList(
		entry4,
		entry2,
	), entries)

	// get entries, with limit
	entries = s.GetEntries(1, 3)
	assert.Equal(t, newLogList(
		entry3,
		entry1,
		entry4,
	), entries)
}

func TestLogStorageFake_Add_Committed_Entries__With_Disconnected_Prev_Pointer(t *testing.T) {
	s := &LogStorageFake{}

	term := paxos.TermNum{
		Num:    21,
		NodeID: NewNodeID(1),
	}

	entry1 := newMembershipLog(1, term,
		NewNodeID(1),
		NewNodeID(2),
	)
	entry1.Term = paxos.InfiniteTerm{}

	entry2 := newCmdLog(2, term, "cmd test 02")

	entry3 := newCmdLog(3, term, "cmd test 03")
	entry3.PrevPointer = entry2.NextPreviousPointer()

	nullEntry := paxos.NewNoOpLogEntry(3)
	nullEntry.Term = term.ToInf()

	entry4 := newCmdLog(4, term, "cmd test 04")
	entry4.PrevPointer = entry3.NextPreviousPointer()

	// insert
	s.UpsertEntries([]paxos.LogEntry{entry1, entry2, nullEntry, entry4}, nil)
	assert.Equal(t, paxos.CommittedInfo{
		Members:         entry1.Members,
		FullyReplicated: 1,
	}, s.GetCommittedInfo())

	// mark committed
	s.UpsertEntries(nil, []paxos.LogPos{2, 3, 4})
	assert.Equal(t, paxos.CommittedInfo{
		Members:         entry1.Members,
		FullyReplicated: 4,
		PrevPointer: paxos.PreviousPointer{
			Pos:  2,
			Term: testCreatedTerm,
		},
	}, s.GetCommittedInfo())
}

func TestLogStorageFake_SetTerm(t *testing.T) {
	s := &LogStorageFake{}

	term := paxos.TermNum{
		Num:    21,
		NodeID: NewNodeID(1),
	}
	s.SetTerm(term)

	assert.Equal(t, term, s.GetTerm())
}

func TestLogStorageFake_GetEntriesWithPos(t *testing.T) {
	s := &LogStorageFake{}

	term := paxos.TermNum{
		Num:    21,
		NodeID: NewNodeID(1),
	}
	entry1 := newCmdLog(2, term, "cmd test 01")
	entry2 := newCmdLog(4, term, "cmd test 02")
	entry3 := newCmdLog(5, term, "cmd test 03")

	s.UpsertEntries([]paxos.LogEntry{
		entry1,
		entry2,
		entry3,
	}, nil)

	entries := s.GetEntriesWithPos(2, 3, 4)
	assert.Equal(t, []paxos.LogEntry{
		entry1,
		paxos.NewNullEntry(3),
		entry2,
	}, entries)

	// with outside of max
	assert.PanicsWithValue(t, "Outside of log range", func() {
		s.GetEntriesWithPos(6)
	})
}
