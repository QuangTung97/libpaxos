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
	})

	assert.Equal(t, []paxos.LogEntry{
		{},
		entry1,
		{},
		entry2,
	}, s.Entries)

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
			ActiveFrom: 2,
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
		Term: paxos.InfiniteTerm{
			IsFinite: true,
			Term: paxos.TermNum{
				Num:    41,
				NodeID: NewNodeID(2),
			},
		},
		CmdData: []byte("hello02"),
	}

	s.UpsertEntries([]paxos.PosLogEntry{
		{Pos: 1, Entry: entry1},
		{Pos: 2, Entry: entry2},
		{Pos: 3, Entry: entry3},
	})

	assert.Equal(t, []paxos.LogEntry{
		entry1, entry2, entry3,
	}, s.Entries)

	// check committed info
	assert.Equal(t, paxos.CommittedInfo{
		Members: members,
		Pos:     2,
	}, s.GetCommittedInfo())
}
