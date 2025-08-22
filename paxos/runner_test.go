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

func TestNodeRunner__Acceptor_Runners(t *testing.T) {
	currentTerm := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	synctest.Test(t, func(t *testing.T) {
		runningSet := map[NodeID]TermNum{}
		var mut sync.Mutex

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
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
		)

		nodes := map[NodeID]struct{}{
			nodeID1: {},
			nodeID2: {},
			nodeID3: {},
		}
		r.StartAcceptRequestRunners(currentTerm, nodes)

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

func TestNodeRunner__State_Machine(t *testing.T) {
	currentTerm := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	synctest.Test(t, func(t *testing.T) {
		var numActive atomic.Int64
		var inputTerm TermNum
		var inputIsLeader bool
		var mut sync.Mutex

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
			nil,
			func(ctx context.Context, term TermNum, isLeader bool) error {
				numActive.Add(1)
				mut.Lock()
				inputTerm = term
				inputIsLeader = isLeader
				mut.Unlock()

				<-ctx.Done()

				numActive.Add(-1)
				return nil
			},
			nil,
		)

		r.SetLeader(currentTerm, true)
		synctest.Wait()

		mut.Lock()
		assert.Equal(t, int64(1), numActive.Load())
		assert.Equal(t, currentTerm, inputTerm)
		assert.Equal(t, true, inputIsLeader)
		mut.Unlock()

		r.SetLeader(currentTerm, false)
		synctest.Wait()

		mut.Lock()
		assert.Equal(t, int64(1), numActive.Load())
		assert.Equal(t, currentTerm, inputTerm)
		assert.Equal(t, false, inputIsLeader)
		mut.Unlock()

		finish()
		assert.Equal(t, int64(0), numActive.Load())
	})
}

func TestNodeRunner__Fetching_Followers(t *testing.T) {
	currentTerm := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	synctest.Test(t, func(t *testing.T) {
		runningSet := map[NodeID]TermNum{}
		var mut sync.Mutex

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
			nil,
			nil,
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
		)

		nodes := map[NodeID]struct{}{
			nodeID1: {},
			nodeID2: {},
			nodeID3: {},
		}
		r.StartFetchingFollowerInfoRunners(currentTerm, nodes)

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
