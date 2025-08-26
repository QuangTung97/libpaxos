package waiting

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroup(t *testing.T) {
	wg := NewWaitGroup()
	var counter atomic.Int64

	for range 10 {
		wg.Go(func() {
			counter.Add(1)
		})
	}

	wg.Wait()
	assert.Equal(t, int64(10), counter.Load())
}

func TestWaitGroup_Not_Init(t *testing.T) {
	var wg WaitGroup
	assert.PanicsWithValue(t, "WaitGroup is not initialized", func() {
		wg.Go(func() {})
	})
}
