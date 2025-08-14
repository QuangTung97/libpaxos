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
