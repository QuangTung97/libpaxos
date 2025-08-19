package paxos_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
)

func TestNodeRunner__Voter_Runners(t *testing.T) {
	currentTerm := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	synctest.Test(t, func(t *testing.T) {
		runningSet := map[NodeID]TermNum{}
		var mut sync.Mutex

		r, finish := NewNodeRunner(
			NodeID{},
			func(ctx context.Context, nodeID NodeID, term TermNum) error {
				mut.Lock()
				runningSet[nodeID] = term
				mut.Unlock()

				<-ctx.Done()

				mut.Lock()
				delete(runningSet, nodeID)
				mut.Unlock()

				return nil
			},
			nil,
			nil,
			nil,
		)

		nodes := map[NodeID]struct{}{
			nodeID1: {},
			nodeID2: {},
			nodeID3: {},
		}
		r.StartVoteRequestRunners(currentTerm, nodes)

		synctest.Wait()

		assert.Equal(t, map[NodeID]TermNum{
			nodeID1: currentTerm,
			nodeID2: currentTerm,
			nodeID3: currentTerm,
		}, runningSet)

		finish()

		assert.Equal(t, map[NodeID]TermNum{}, runningSet)
	})
}

func TestNodeRunner__Voter_Runners__With_Error(t *testing.T) {
	currentTerm := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r, finish := NewNodeRunner(
			NodeID{},
			func(ctx context.Context, nodeID NodeID, term TermNum) error {
				calls.Add(1)
				return errors.New("test error")
			},
			nil,
			nil,
			nil,
		)

		nodes := map[NodeID]struct{}{
			nodeID1: {},
		}
		r.StartVoteRequestRunners(currentTerm, nodes)

		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, int64(1), calls.Load())

		time.Sleep(800 * time.Millisecond)
		assert.Equal(t, int64(2), calls.Load())

		finish()

		assert.Equal(t, int64(2), calls.Load())
	})
}
