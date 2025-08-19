package paxos_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

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
