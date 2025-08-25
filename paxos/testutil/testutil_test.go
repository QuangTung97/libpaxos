package testutil

import (
	"sync"
	"testing"
	"testing/synctest"
)

func TestSyncTest_With_Chanel(t *testing.T) {
	for range 100 {
		doSyncTestWithChanel(t)
	}
}

func doSyncTestWithChanel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := make(chan struct{})

		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() {
				<-ch
			})
		}

		synctest.Wait()
		close(ch)
	})
}
