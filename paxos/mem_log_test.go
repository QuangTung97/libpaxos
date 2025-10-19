package paxos_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

func TestMemLog(t *testing.T) {
	newEntry := func(pos LogPos, cmd string) LogEntry {
		return LogEntry{
			Pos:  pos,
			Type: LogTypeCmd,
			Term: TermNum{
				Num:    15,
				NodeID: nodeID3,
			}.ToInf(),
			CmdData: []byte(cmd),
		}
	}

	t.Run("normal", func(t *testing.T) {
		lastCommitted := LogPos(20)

		m := NewMemLog(&lastCommitted, 2)
		assert.Equal(t, LogPos(20), m.MaxLogPos())
		assert.Equal(t, 0, m.GetQueueSize())

		entry1 := newEntry(21, "cmd 01")
		entry2 := newEntry(22, "cmd 02")
		entry3 := newEntry(24, "cmd 03")

		m.PutV2(entry1)
		m.PutV2(entry2)
		m.PutV2(entry3)

		assert.Equal(t, LogPos(24), m.MaxLogPos())
		assert.Equal(t, 4, m.GetQueueSize())

		entry := m.Get(21)
		assert.Equal(t, entry1, entry)

		entry = m.Get(22)
		assert.Equal(t, entry2, entry)

		entry = m.Get(23)
		assert.Equal(t, LogEntry{}, entry)

		entry = m.Get(24)
		assert.Equal(t, entry3, entry)

		entry = m.Get(25)
		assert.Equal(t, LogEntry{}, entry)

		// panic when < last committed
		assert.PanicsWithValue(t, "Invalid log pos in mem log", func() {
			m.Get(20)
		})
	})

	t.Run("queue cap extended", func(t *testing.T) {
		lastCommitted := LogPos(20)

		m := NewMemLog(&lastCommitted, 2)

		entry1 := newEntry(21, "cmd 01")
		entry2 := newEntry(22, "cmd 02")
		entry3 := newEntry(27, "cmd 03")

		m.PutV2(entry1)
		m.PutV2(entry2)
		m.PutV2(entry3)

		assert.Equal(t, LogPos(27), m.MaxLogPos())

		entry := m.Get(21)
		assert.Equal(t, entry1, entry)

		entry = m.Get(22)
		assert.Equal(t, entry2, entry)

		entry = m.Get(26)
		assert.Equal(t, LogEntry{}, entry)

		entry = m.Get(27)
		assert.Equal(t, entry3, entry)
	})

	t.Run("remove head of the log", func(t *testing.T) {
		lastCommitted := LogPos(20)

		m := NewMemLog(&lastCommitted, 2)

		entry1 := newEntry(21, "cmd 01")
		entry2 := newEntry(22, "cmd 02")
		entry3 := newEntry(27, "cmd 03")
		entry4 := newEntry(28, "cmd 04")

		m.PutV2(entry1)
		m.PutV2(entry2)
		assert.Equal(t, 2, m.GetQueueSize())

		popEntry := m.PopFront()
		assert.Equal(t, entry1, popEntry)

		popEntry = m.PopFront()
		assert.Equal(t, entry2, popEntry)
		assert.Equal(t, 0, m.GetQueueSize())

		m.PutV2(entry3)
		m.PutV2(entry4)

		assert.Equal(t, LogPos(22), lastCommitted)
		assert.Equal(t, LogPos(28), m.MaxLogPos())

		entry := m.Get(23)
		assert.Equal(t, LogEntry{}, entry)
		entry = m.Get(24)
		assert.Equal(t, LogEntry{}, entry)

		entry = m.Get(26)
		assert.Equal(t, LogEntry{}, entry)

		entry = m.Get(27)
		assert.Equal(t, entry3, entry)

		entry = m.Get(28)
		assert.Equal(t, entry4, entry)

		assert.Equal(t, 6, m.GetQueueSize())
	})

	t.Run("get voted", func(t *testing.T) {
		lastCommitted := LogPos(20)
		m := NewMemLog(&lastCommitted, 2)

		entry1 := newEntry(21, "cmd 01")
		entry2 := newEntry(22, "cmd 02")
		entry3 := newEntry(24, "cmd 03")

		m.PutV2(entry1)
		m.PutV2(entry2)
		m.PutV2(entry3)

		assert.Equal(t, LogPos(24), m.MaxLogPos())

		assert.Equal(t, map[NodeID]struct{}{}, m.GetVoted(21))
		assert.Equal(t, map[NodeID]struct{}{}, m.GetVoted(22))
		assert.Equal(t, map[NodeID]struct{}{}, m.GetVoted(23))

		// add nodes and check
		node1 := fake.NewNodeID(1)
		node2 := fake.NewNodeID(2)

		addVote := func(pos LogPos, n NodeID) {
			voted := m.GetVoted(pos)
			voted[n] = struct{}{}
		}

		addVote(21, node1)
		addVote(21, node2)
		addVote(22, node2)

		entry1.Term = InfiniteTerm{}
		m.PutV2(entry1)

		assert.Equal(t, map[NodeID]struct{}{
			node1: {},
			node2: {},
		}, m.GetVoted(21))

		assert.Equal(t, map[NodeID]struct{}{
			node2: {},
		}, m.GetVoted(22))

		assert.Equal(t, map[NodeID]struct{}{}, m.GetVoted(23))

		// get front
		term := m.GetFrontVoted()
		assert.Equal(t, map[NodeID]struct{}{
			node1: {},
			node2: {},
		}, term)
	})

	t.Run("wrap around, check voted", func(t *testing.T) {
		lastCommitted := LogPos(20)

		m := NewMemLog(&lastCommitted, 2)

		entry1 := newEntry(21, "cmd 01")
		entry2 := newEntry(22, "cmd 02")
		entry3 := newEntry(23, "cmd 03")
		entry4 := newEntry(24, "cmd 04")

		m.PutV2(entry1)
		m.PutV2(entry2)
		m.PutV2(entry3)
		m.PutV2(entry4)

		m.PopFront()

		entry5 := newEntry(25, "cmd 05")
		m.PutV2(entry5)

		assert.Equal(t, entry5, m.Get(25))
		assert.Equal(t, map[NodeID]struct{}{}, m.GetVoted(25))
	})
}
