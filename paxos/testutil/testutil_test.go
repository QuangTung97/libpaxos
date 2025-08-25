package testutil

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
)

func TestSyncTest_With_Chanel(t *testing.T) {
	for range 10 {
		doSyncTestWithChanel(t)
	}
}

func doSyncTestWithChanel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			var wg sync.WaitGroup

			for range 100 {
				wg.Go(func() {
					for {
						select {
						case <-ch:
						case <-ctx.Done():
							return
						}
					}
				})
			}

			wg.Wait()
		}()

		synctest.Wait()
	})
}
