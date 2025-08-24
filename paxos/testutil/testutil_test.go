package testutil

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

func TestSyncTest_With_Chanel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		input := make(chan struct{}, 1)
		output := make(chan struct{}, 1)
		var finished atomic.Bool

		go func() {
			var wg sync.WaitGroup

			wg.Go(func() {
				<-output
				finished.Store(true)
			})

			wg.Go(func() {
				<-input
				close(output)
			})

			wg.Wait()
		}()

		close(input)

		synctest.Wait()
		assert.Equal(t, true, finished.Load())
	})
}
