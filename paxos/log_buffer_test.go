package paxos_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
)

func newLogList(entries ...LogEntry) []LogEntry {
	return entries
}

func TestLogBuffer(t *testing.T) {
	newCmd := func(pos LogPos, cmd string) LogEntry {
		return NewCmdLogEntryV1(pos, InfiniteTerm{}, []byte(cmd))
	}

	t.Run("normal", func(t *testing.T) {
		lastCommitted := LogPos(20)
		b := NewLogBuffer(&lastCommitted, 1)

		assert.Equal(t, 0, b.Size())
		assert.Equal(t, LogPos(21), b.GetFrontPos())

		entry1 := newCmd(19, "test cmd 01")
		entry2 := newCmd(20, "test cmd 01")

		b.Insert(entry1)
		b.Insert(entry2)
		assert.Equal(t, 2, b.Size())
		assert.Equal(t, LogPos(19), b.GetFrontPos())

		// get all
		entries := b.GetEntries(19, 20)
		assert.Equal(t, newLogList(
			entry1, entry2,
		), entries)

		// get single
		entries = b.GetEntries(19)
		assert.Equal(t, newLogList(
			entry1,
		), entries)

		// get not found
		entries = b.GetEntries(18)
		assert.Equal(t, newLogList(
			NewNullEntry(18),
		), entries)

		// get not found
		entries = b.GetEntries(21)
		assert.Equal(t, newLogList(
			NewNullEntry(21),
		), entries)

		// extend queue
		entry3 := newCmd(21, "test cmd 03")
		entry4 := newCmd(22, "test cmd 04")
		lastCommitted += 2
		b.Insert(entry3)
		b.Insert(entry4)

		// get all
		entries = b.GetEntries(19, 20, 21, 22)
		assert.Equal(t, newLogList(
			entry1, entry2,
			entry3, entry4,
		), entries)

		// get half
		entries = b.GetEntries(19, 21)
		assert.Equal(t, newLogList(
			entry1,
			entry3,
		), entries)
	})

	t.Run("push then pop wrap around", func(t *testing.T) {
		lastCommitted := LogPos(20)
		b := NewLogBuffer(&lastCommitted, 2)

		entry1 := newCmd(21, "test cmd 01")
		entry2 := newCmd(22, "test cmd 02")
		entry3 := newCmd(23, "test cmd 03")

		lastCommitted += 3
		b.Insert(entry1)
		b.Insert(entry2)
		b.Insert(entry3)

		b.PopFront()

		// get all
		entries := b.GetEntries(21, 22, 23)
		assert.Equal(t, newLogList(
			NewNullEntry(21),
			entry2,
			entry3,
		), entries)

		entries = b.GetEntries(21)
		assert.Equal(t, newLogList(
			NewNullEntry(21),
		), entries)

		entries = b.GetEntries(24)
		assert.Equal(t, newLogList(
			NewNullEntry(24),
		), entries)

		// insert wrap around
		entry4 := newCmd(24, "test cmd 04")
		entry5 := newCmd(25, "test cmd 05")
		entry6 := newCmd(26, "test cmd 06")
		lastCommitted += 3
		b.Insert(entry4)
		b.Insert(entry5)
		b.Insert(entry6)

		// get all
		entries = b.GetEntries(21, 22, 23, 24, 25, 26, 27)
		assert.Equal(t, newLogList(
			NewNullEntry(21),
			entry2,
			entry3,
			entry4,
			entry5,
			entry6,
			NewNullEntry(27),
		), entries)

		assert.Equal(t, LogPos(22), b.GetFrontPos())

		// pop
		b.PopFront()
		b.PopFront()
		assert.Equal(t, LogPos(24), b.GetFrontPos())

		// get all
		entries = b.GetEntries(23, 24, 25, 26, 27)
		assert.Equal(t, newLogList(
			NewNullEntry(23),
			entry4,
			entry5,
			entry6,
			NewNullEntry(27),
		), entries)
	})

	t.Run("pop with panic", func(t *testing.T) {
		lastCommitted := LogPos(20)
		b := NewLogBuffer(&lastCommitted, 2)

		entry1 := newCmd(19, "test cmd 01")
		entry2 := newCmd(20, "test cmd 02")

		lastCommitted += 2
		b.Insert(entry1)
		b.Insert(entry2)

		b.PopFront()
		b.PopFront()

		assert.PanicsWithValue(t, "queue size must not be empty", func() {
			b.PopFront()
		})
	})
}
