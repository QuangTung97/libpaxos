package paxos_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

func TestCompareTermNum(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		a := TermNum{
			Num:    19,
			NodeID: fake.NewNodeID(1),
		}
		b := TermNum{
			Num:    18,
			NodeID: fake.NewNodeID(2),
		}

		assert.Equal(t, 1, CompareTermNum(a, b))
		assert.Equal(t, -1, CompareTermNum(b, a))

		// equal
		assert.Equal(t, 0, CompareTermNum(a, a))
		assert.Equal(t, 0, CompareTermNum(b, b))
	})

	t.Run("same term num value", func(t *testing.T) {
		a := TermNum{
			Num:    19,
			NodeID: fake.NewNodeID(1),
		}
		b := TermNum{
			Num:    19,
			NodeID: fake.NewNodeID(2),
		}

		assert.Equal(t, -1, CompareTermNum(a, b))
		assert.Equal(t, 1, CompareTermNum(b, a))

		// equal
		assert.Equal(t, 0, CompareTermNum(a, a))
		assert.Equal(t, 0, CompareTermNum(b, b))
	})
}

func TestCompareInfiniteTerm(t *testing.T) {
	t.Run("both non inf", func(t *testing.T) {
		a := TermNum{
			Num:    19,
			NodeID: fake.NewNodeID(1),
		}
		b := TermNum{
			Num:    18,
			NodeID: fake.NewNodeID(2),
		}

		assert.Equal(t, 1, CompareInfiniteTerm(a.ToInf(), b.ToInf()))
		assert.Equal(t, -1, CompareInfiniteTerm(b.ToInf(), a.ToInf()))

		// equal
		assert.Equal(t, 0, CompareInfiniteTerm(a.ToInf(), a.ToInf()))
		assert.Equal(t, 0, CompareInfiniteTerm(b.ToInf(), b.ToInf()))
	})

	t.Run("inf vs non inf", func(t *testing.T) {
		a := TermNum{
			Num:    19,
			NodeID: fake.NewNodeID(1),
		}

		// non-inf < inf
		assert.Equal(t, -1, CompareInfiniteTerm(a.ToInf(), InfiniteTerm{}))

		// inf > non-inf
		assert.Equal(t, 1, CompareInfiniteTerm(InfiniteTerm{}, a.ToInf()))

		// inf == inf
		assert.Equal(t, 0, CompareInfiniteTerm(InfiniteTerm{}, InfiniteTerm{}))
	})
}

func TestLogEntryEqual(t *testing.T) {
	t.Run("membership", func(t *testing.T) {
		a := LogEntry{
			Pos:  11,
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			Members: []MemberInfo{
				{Nodes: []NodeID{nodeID1, nodeID2}, CreatedAt: 2},
			},
		}

		b := LogEntry{
			Pos:  11,
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			Members: []MemberInfo{
				{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 2},
			},
		}

		assert.Equal(t, true, LogEntryEqual(a, a))
		assert.Equal(t, false, LogEntryEqual(a, b))
		assert.Equal(t, false, LogEntryEqual(b, a))
		assert.Equal(t, true, LogEntryEqual(b, b))

		c := LogEntry{
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			Members: []MemberInfo{
				{Nodes: []NodeID{nodeID1, nodeID2}, CreatedAt: 5},
			},
		}
		assert.Equal(t, false, LogEntryEqual(a, c))
	})

	t.Run("cmd", func(t *testing.T) {
		a := LogEntry{
			Pos:  11,
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			CmdData: []byte("cmd test 01"),
		}
		b := LogEntry{
			Pos:  11,
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			CmdData: []byte("cmd test 02"),
		}

		assert.Equal(t, true, LogEntryEqual(a, a))
		assert.Equal(t, false, LogEntryEqual(a, b))
		assert.Equal(t, false, LogEntryEqual(b, a))
		assert.Equal(t, true, LogEntryEqual(b, b))
	})

	t.Run("cmd, pos not equal", func(t *testing.T) {
		a := LogEntry{
			Pos:  11,
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			CmdData: []byte("cmd test 01"),
		}
		b := LogEntry{
			Pos:  12,
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			CmdData: []byte("cmd test 01"),
		}

		assert.Equal(t, true, LogEntryEqual(a, a))
		assert.Equal(t, false, LogEntryEqual(a, b))
		assert.Equal(t, false, LogEntryEqual(b, a))
		assert.Equal(t, true, LogEntryEqual(b, b))
	})

	t.Run("term not equal", func(t *testing.T) {
		a := LogEntry{
			Pos:  11,
			Type: LogTypeCmd,
			Term: TermNum{
				Num:    21,
				NodeID: nodeID1,
			}.ToInf(),
			CmdData: []byte("cmd test 01"),
		}
		b := LogEntry{
			Pos:  11,
			Type: LogTypeCmd,
			Term: TermNum{
				Num:    22,
				NodeID: nodeID1,
			}.ToInf(),
			CmdData: []byte("cmd test 01"),
		}

		assert.Equal(t, true, LogEntryEqual(a, a))
		assert.Equal(t, false, LogEntryEqual(a, b))
		assert.Equal(t, false, LogEntryEqual(b, a))
		assert.Equal(t, true, LogEntryEqual(b, b))
	})

	t.Run("type not equal", func(t *testing.T) {
		a := LogEntry{
			Pos:     11,
			Type:    LogTypeCmd,
			CmdData: []byte("cmd test 01"),
		}
		b := LogEntry{
			Pos:     11,
			Type:    LogTypeMembership,
			CmdData: []byte("cmd test 01"),
		}

		assert.Equal(t, true, LogEntryEqual(a, a))
		assert.Equal(t, false, LogEntryEqual(a, b))
		assert.Equal(t, false, LogEntryEqual(b, a))
		assert.Equal(t, true, LogEntryEqual(b, b))
	})
}
