package paxos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemLog(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		lastCommitted := LogPos(20)

		m := NewMemLog(&lastCommitted, 2)
		assert.Equal(t, LogPos(20), m.MaxLogPos())

		entry1 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 01"),
		}
		entry2 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 02"),
		}
		entry3 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 03"),
		}
		m.Put(21, entry1)
		m.Put(22, entry2)
		m.Put(24, entry3)

		assert.Equal(t, LogPos(24), m.MaxLogPos())

		entry := m.Get(21)
		assert.Equal(t, entry1, entry)

		entry = m.Get(22)
		assert.Equal(t, entry2, entry)

		entry = m.Get(23)
		assert.Equal(t, LogEntry{}, entry)

		// panic when < last committed
		assert.PanicsWithValue(t, "Invalid log pos in mem log", func() {
			m.Get(20)
		})
		// panic when > max pos
		assert.PanicsWithValue(t, "Exceeded mem log size", func() {
			m.Get(25)
		})
	})

	t.Run("queue cap extended", func(t *testing.T) {
		lastCommitted := LogPos(20)

		m := NewMemLog(&lastCommitted, 2)

		entry1 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 01"),
		}
		entry2 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 02"),
		}
		entry3 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 03"),
		}
		m.Put(21, entry1)
		m.Put(22, entry2)
		m.Put(27, entry3)

		assert.Equal(t, LogPos(27), m.MaxLogPos())

		entry := m.Get(21)
		assert.Equal(t, entry1, entry)

		// get front
		entry = m.Front()
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

		entry1 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 01"),
		}
		entry2 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 02"),
		}
		entry3 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 03"),
		}
		entry4 := LogEntry{
			Type:    LogTypeCmd,
			CmdData: []byte("cmd 04"),
		}
		m.Put(21, entry1)
		m.Put(22, entry2)

		m.PopFront()
		m.PopFront()

		m.Put(27, entry3)
		m.Put(28, entry4)

		assert.Equal(t, LogPos(22), lastCommitted)
		assert.Equal(t, LogPos(28), m.MaxLogPos())

		entry := m.Get(26)
		assert.Equal(t, LogEntry{}, entry)

		entry = m.Get(27)
		assert.Equal(t, entry3, entry)

		entry = m.Get(28)
		assert.Equal(t, entry4, entry)
	})
}
