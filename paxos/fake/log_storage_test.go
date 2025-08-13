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
}
