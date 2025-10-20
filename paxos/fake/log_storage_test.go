package fake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/paxos"
)

func TestLogStorageFake(t *testing.T) {
	s := &LogStorageFake{}

	entry1 := paxos.LogEntry{
		Type:    paxos.LogTypeCmd,
		CmdData: []byte("hello01"),
	}
	entry2 := paxos.LogEntry{
		Type:    paxos.LogTypeCmd,
		CmdData: []byte("hello02"),
	}

	s.UpsertEntries([]paxos.PosLogEntry{
		{
			Pos:   2,
			Entry: entry1,
		},
		{
			Pos:   4,
			Entry: entry2,
		},
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

	entry1 := paxos.LogEntry{
		Type:    paxos.LogTypeMembership,
		Term:    paxos.InfiniteTerm{},
		Members: members,
	}
	entry2 := paxos.LogEntry{
		Type:    paxos.LogTypeCmd,
		CmdData: []byte("hello01"),
	}
	entry3 := paxos.LogEntry{
		Type: paxos.LogTypeCmd,
		Term: paxos.TermNum{
			Num:    41,
			NodeID: NewNodeID(2),
		}.ToInf(),
		CmdData: []byte("hello02"),
	}

	s.UpsertEntries([]paxos.PosLogEntry{
		{Pos: 1, Entry: entry1},
		{Pos: 2, Entry: entry2},
		{Pos: 3, Entry: entry3},
	}, nil)

	assert.Equal(t, []paxos.LogEntry{
		entry1, entry2, entry3,
	}, s.logEntries)

	// check committed info
	assert.Equal(t, paxos.CommittedInfo{
		Members:         members,
		FullyReplicated: 2,
	}, s.GetCommittedInfo())

	assert.Equal(t, paxos.LogPos(2), s.GetFullyReplicated())
}

func newCmdLog(pos paxos.LogPos, term paxos.TermNum, cmd string) paxos.LogEntry {
	return paxos.LogEntry{
		Pos:     pos,
		Type:    paxos.LogTypeCmd,
		Term:    term.ToInf(),
		CmdData: []byte(cmd),
	}
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

	s.UpsertEntries([]paxos.PosLogEntry{
		{Pos: 2, Entry: entry1},
		{Pos: 4, Entry: entry2},
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
	s.UpsertEntries([]paxos.PosLogEntry{
		{Pos: 1, Entry: entry3},
	}, nil)
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
	}, s.GetCommittedInfo())

	s.UpsertEntries(nil, []paxos.LogPos{2, 4})
	assert.Equal(t, paxos.CommittedInfo{
		FullyReplicated: 2,
		Members:         entry3.Members,
	}, s.GetCommittedInfo())

	// add committed entry
	entry4 := newCmdLog(3, term, "cmd test 04")
	entry4.Term = paxos.InfiniteTerm{}
	s.UpsertEntries([]paxos.PosLogEntry{
		{Pos: 3, Entry: entry4},
	}, nil)
	assert.Equal(t, paxos.CommittedInfo{
		FullyReplicated: 4,
		Members:         entry3.Members,
	}, s.GetCommittedInfo())

	// get entries
	entries := s.GetEntries(1, 100)

	entry1.Term = paxos.InfiniteTerm{}
	entry2.Term = paxos.InfiniteTerm{}
	entry3.Term = paxos.InfiniteTerm{}

	assert.Equal(t, []paxos.PosLogEntry{
		{Pos: 1, Entry: entry3},
		{Pos: 2, Entry: entry1},
		{Pos: 3, Entry: entry4},
		{Pos: 4, Entry: entry2},
	}, entries)

	// get entries, from 3
	entries = s.GetEntries(3, 100)
	assert.Equal(t, []paxos.PosLogEntry{
		{Pos: 3, Entry: entry4},
		{Pos: 4, Entry: entry2},
	}, entries)

	// get entries, with limit
	entries = s.GetEntries(1, 3)
	assert.Equal(t, []paxos.PosLogEntry{
		{Pos: 1, Entry: entry3},
		{Pos: 2, Entry: entry1},
		{Pos: 3, Entry: entry4},
	}, entries)
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

	s.UpsertEntries([]paxos.PosLogEntry{
		{Pos: 2, Entry: entry1},
		{Pos: 4, Entry: entry2},
		{Pos: 5, Entry: entry3},
	}, nil)

	entries := s.GetEntriesWithPos(2, 3, 4)
	assert.Equal(t, []paxos.PosLogEntry{
		{Pos: 2, Entry: entry1},
		{Pos: 3, Entry: paxos.LogEntry{}},
		{Pos: 4, Entry: entry2},
	}, entries)

	// with outside of max
	assert.PanicsWithValue(t, "Outside of log range", func() {
		s.GetEntriesWithPos(6)
	})
}
