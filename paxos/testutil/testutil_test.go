package testutil

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/QuangTung97/libpaxos/paxos/waiting"
)

func TestSyncTest_Wait_Group(t *testing.T) {
	for range 100 {
		doSyncTestWithWaitGroup(t)
	}
}

func doSyncTestWithWaitGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		for range 100 {
			go func() {
				simpleWait(ctx)
			}()
		}

		synctest.Wait()
		cancel()
	})
}

func simpleWait(ctx context.Context) {
	// var wg sync.WaitGroup
	wg := waiting.NewWaitGroup()
	for range 3 {
		wg.Go(func() {
			<-ctx.Done()
		})
	}
	wg.Wait()
}
