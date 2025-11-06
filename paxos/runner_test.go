package paxos_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/async"
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
			func(ctx async.Context, nodeID NodeID, term TermNum) error {
				mut.Lock()
				runningSet[nodeID] = term
				mut.Unlock()

				<-ctx.ToContext().Done()

				mut.Lock()
				delete(runningSet, nodeID)
				mut.Unlock()

				return nil
			},
			nil,
			nil,
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
			func(ctx async.Context, nodeID NodeID, term TermNum) error {
				calls.Add(1)
				return errors.New("test error")
			},
			nil,
			nil,
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
		replicateSet := map[NodeID]TermNum{}
		var mut sync.Mutex

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
			func(ctx async.Context, nodeID NodeID, term TermNum) error {
				mut.Lock()
				runningSet[nodeID] = term
				mut.Unlock()

				<-ctx.ToContext().Done()

				mut.Lock()
				delete(runningSet, nodeID)
				mut.Unlock()

				return nil
			},
			func(ctx async.Context, nodeID NodeID, term TermNum) error {
				mut.Lock()
				replicateSet[nodeID] = term
				mut.Unlock()

				<-ctx.ToContext().Done()

				mut.Lock()
				delete(replicateSet, nodeID)
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
		r.StartAcceptRequestRunners(currentTerm, nodes)

		synctest.Wait()

		assert.Equal(t, map[NodeID]TermNum{
			nodeID1: currentTerm,
			nodeID2: currentTerm,
			nodeID3: currentTerm,
		}, runningSet)

		assert.Equal(t, map[NodeID]TermNum{
			nodeID1: currentTerm,
			nodeID2: currentTerm,
			nodeID3: currentTerm,
		}, replicateSet)

		finish()

		assert.Equal(t, map[NodeID]TermNum{}, runningSet)
		assert.Equal(t, map[NodeID]TermNum{}, replicateSet)
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
		var inputInfo StateMachineRunnerInfo
		var mut sync.Mutex

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
			nil,
			nil,
			func(ctx async.Context, term TermNum, info StateMachineRunnerInfo) error {
				numActive.Add(1)
				mut.Lock()
				inputTerm = term
				inputInfo = info
				mut.Unlock()

				<-ctx.ToContext().Done()

				numActive.Add(-1)
				return nil
			},
			nil,
			nil,
		)

		r.StartStateMachine(currentTerm, StateMachineRunnerInfo{
			Running:       true,
			IsLeader:      true,
			AcceptCommand: true,
		})
		synctest.Wait()

		mut.Lock()
		assert.Equal(t, int64(1), numActive.Load())
		assert.Equal(t, currentTerm, inputTerm)
		assert.Equal(t, StateMachineRunnerInfo{
			Running:       true,
			IsLeader:      true,
			AcceptCommand: true,
		}, inputInfo)
		mut.Unlock()

		r.StartStateMachine(currentTerm, StateMachineRunnerInfo{})
		synctest.Wait()

		mut.Lock()
		assert.Equal(t, int64(0), numActive.Load())
		assert.Equal(t, currentTerm, inputTerm)
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
			nil,
			func(ctx async.Context, nodeID NodeID, term TermNum) error {
				mut.Lock()
				runningSet[nodeID] = term
				mut.Unlock()

				<-ctx.ToContext().Done()

				mut.Lock()
				delete(runningSet, nodeID)
				mut.Unlock()

				return nil
			},
			nil,
		)

		nodes := map[NodeID]struct{}{
			nodeID1: {},
			nodeID2: {},
			nodeID3: {},
		}
		r.StartFetchingFollowerInfoRunners(currentTerm, nodes, 1)

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

func TestNodeRunner__Start_Election_Runner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runningSet := map[NodeID]TermValue{}
		var mut sync.Mutex
		var runCount atomic.Int64

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
			nil,
			nil,
			nil,
			nil,
			func(ctx async.Context, nodeID NodeID, termVal TermValue) error {
				mut.Lock()
				runningSet[nodeID] = termVal
				mut.Unlock()

				runCount.Add(1)

				<-ctx.ToContext().Done()

				mut.Lock()
				delete(runningSet, nodeID)
				mut.Unlock()

				return nil
			},
		)

		r.StartElectionRunner(21, true, nodeID1, 1)
		synctest.Wait()
		assert.Equal(t, map[NodeID]TermValue{
			nodeID1: 21,
		}, runningSet)
		assert.Equal(t, int64(1), runCount.Load())

		// start again same retry count
		r.StartElectionRunner(21, true, nodeID1, 1)
		synctest.Wait()
		assert.Equal(t, int64(1), runCount.Load())

		// start again different retry count
		r.StartElectionRunner(21, true, nodeID1, 2)
		synctest.Wait()
		assert.Equal(t, int64(2), runCount.Load())

		finish()

		assert.Equal(t, map[NodeID]TermValue{}, runningSet)
	})
}

func TestNodeRunner__Start_Election_Runner__Start_Then_Stop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runningSet := map[NodeID]TermValue{}
		var mut sync.Mutex
		var runCount atomic.Int64

		r, finish := NewNodeRunner(
			NodeID{},
			nil,
			nil,
			nil,
			nil,
			nil,
			func(ctx async.Context, nodeID NodeID, termVal TermValue) error {
				mut.Lock()
				runningSet[nodeID] = termVal
				mut.Unlock()

				runCount.Add(1)

				<-ctx.ToContext().Done()

				mut.Lock()
				delete(runningSet, nodeID)
				mut.Unlock()

				return nil
			},
		)

		r.StartElectionRunner(21, true, nodeID1, 1)
		synctest.Wait()
		assert.Equal(t, map[NodeID]TermValue{
			nodeID1: 21,
		}, runningSet)
		assert.Equal(t, int64(1), runCount.Load())

		// stop
		r.StartElectionRunner(0, false, NodeID{}, 0)
		synctest.Wait()
		assert.Equal(t, map[NodeID]TermValue{}, runningSet)
		assert.Equal(t, int64(1), runCount.Load())

		finish()

		assert.Equal(t, map[NodeID]TermValue{}, runningSet)
	})
}
