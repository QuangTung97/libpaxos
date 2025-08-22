package paxos_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
)

func TestLogBuffer(t *testing.T) {
	newCmd := func(cmd string) LogEntry {
		return NewCmdLogEntry(InfiniteTerm{}, []byte(cmd))
	}

	t.Run("normal", func(t *testing.T) {
		lastCommitted := LogPos(20)
		b := NewLogBuffer(&lastCommitted, 1)

		assert.Equal(t, 0, b.Size())
		assert.Equal(t, LogPos(21), b.GetFrontPos())

		entry1 := newCmd("test cmd 01")
		entry2 := newCmd("test cmd 01")

		b.Insert(entry1)
		b.Insert(entry2)
		assert.Equal(t, 2, b.Size())
		assert.Equal(t, LogPos(19), b.GetFrontPos())

		// get all
		entries := b.GetEntries(19, 20)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 19, Entry: entry1},
			{Pos: 20, Entry: entry2},
		}, entries)

		// get single
		entries = b.GetEntries(19)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 19, Entry: entry1},
		}, entries)

		// get not found
		entries = b.GetEntries(18)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 18},
		}, entries)

		// get not found
		entries = b.GetEntries(21)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 21},
		}, entries)

		// extend queue
		entry3 := newCmd("test cmd 03")
		entry4 := newCmd("test cmd 04")
		lastCommitted += 2
		b.Insert(entry3)
		b.Insert(entry4)

		// get all
		entries = b.GetEntries(19, 20, 21, 22)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 19, Entry: entry1},
			{Pos: 20, Entry: entry2},
			{Pos: 21, Entry: entry3},
			{Pos: 22, Entry: entry4},
		}, entries)

		// get half
		entries = b.GetEntries(19, 21)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 19, Entry: entry1},
			{Pos: 21, Entry: entry3},
		}, entries)
	})

	t.Run("push then pop wrap around", func(t *testing.T) {
		lastCommitted := LogPos(20)
		b := NewLogBuffer(&lastCommitted, 2)

		entry1 := newCmd("test cmd 01")
		entry2 := newCmd("test cmd 02")
		entry3 := newCmd("test cmd 03")

		lastCommitted += 3
		b.Insert(entry1)
		b.Insert(entry2)
		b.Insert(entry3)

		b.PopFront()

		// get all
		entries := b.GetEntries(21, 22, 23)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 21},
			{Pos: 22, Entry: entry2},
			{Pos: 23, Entry: entry3},
		}, entries)

		entries = b.GetEntries(21)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 21},
		}, entries)

		entries = b.GetEntries(24)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 24},
		}, entries)

		// insert wrap around
		entry4 := newCmd("test cmd 04")
		entry5 := newCmd("test cmd 05")
		entry6 := newCmd("test cmd 06")
		lastCommitted += 3
		b.Insert(entry4)
		b.Insert(entry5)
		b.Insert(entry6)

		// get all
		entries = b.GetEntries(21, 22, 23, 24, 25, 26, 27)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 21},
			{Pos: 22, Entry: entry2},
			{Pos: 23, Entry: entry3},
			{Pos: 24, Entry: entry4},
			{Pos: 25, Entry: entry5},
			{Pos: 26, Entry: entry6},
			{Pos: 27},
		}, entries)

		assert.Equal(t, LogPos(22), b.GetFrontPos())

		// pop
		b.PopFront()
		b.PopFront()
		assert.Equal(t, LogPos(24), b.GetFrontPos())

		// get all
		entries = b.GetEntries(23, 24, 25, 26, 27)
		assert.Equal(t, []AcceptLogEntry{
			{Pos: 23},
			{Pos: 24, Entry: entry4},
			{Pos: 25, Entry: entry5},
			{Pos: 26, Entry: entry6},
			{Pos: 27},
		}, entries)
	})

	t.Run("pop with panic", func(t *testing.T) {
		lastCommitted := LogPos(20)
		b := NewLogBuffer(&lastCommitted, 2)

		entry1 := newCmd("test cmd 01")
		entry2 := newCmd("test cmd 02")

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
