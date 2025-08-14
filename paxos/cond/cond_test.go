package cond

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCond_Normal__Without_Context_Cancel(t *testing.T) {
	var mut sync.Mutex
	var waitCount int
	cond := NewCond(&mut)

	var finishCount atomic.Int64

	const numThreads = 16

	waitCount += numThreads

	for range numThreads {
		go func() {
			defer finishCount.Add(1)

			mut.Lock()
			defer mut.Unlock()
			waitCount--
			cond.Broadcast()
		}()
	}

	ctx := context.Background()

	doWaitFinish := func() {
		mut.Lock()
		defer mut.Unlock()

		for waitCount > 0 {
			if err := cond.Wait(ctx); err != nil {
				panic(err)
			}
		}
	}

	doWaitFinish()

	assert.Equal(t, int64(numThreads), finishCount.Load())
}

func TestCond_With_Context_Cancel(t *testing.T) {
	var mut sync.Mutex
	cond := NewCond(&mut)
	waitCount := 10

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	doWaitFinish := func() error {
		mut.Lock()
		defer mut.Unlock()

		for waitCount > 0 {
			if err := cond.Wait(ctx); err != nil {
				return err
			}
		}

		return nil
	}

	err := doWaitFinish()
	assert.Equal(t, context.Canceled, err)

	assert.Equal(t, 0, len(cond.waitList))

	mut.Lock()
	waitCount--
	mut.Unlock()

	assert.Equal(t, 9, waitCount)
}

func TestCond_Multi_Producers_Consumers(t *testing.T) {
	var mut sync.Mutex
	var queueData []int64
	cond := NewCond(&mut)

	var nextElem atomic.Int64
	var totalConsumed atomic.Int64

	const numThreads = 8
	const numLoops = 1000

	var wg sync.WaitGroup

	for range numThreads {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range numLoops {
				elem := nextElem.Add(1)

				mut.Lock()
				queueData = append(queueData, elem)
				cond.Broadcast()
				mut.Unlock()
			}
		}()
	}

	ctx := context.Background()

	fetchElem := func() int64 {
		mut.Lock()
		defer mut.Unlock()

		for len(queueData) == 0 {
			if err := cond.Wait(ctx); err != nil {
				panic(err)
			}
		}

		head := queueData[0]
		queueData = queueData[1:]

		return head
	}

	for range numThreads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numLoops {
				elem := fetchElem()
				totalConsumed.Add(elem)
			}
		}()
	}

	wg.Wait()

	const numInc = numThreads * numLoops

	assert.Equal(t, int64((numInc+1)*numInc/2), totalConsumed.Load())
}

func TestRemoveFromChanList(t *testing.T) {
	a := make(chan struct{})
	b := make(chan struct{})
	c := make(chan struct{})

	result := removeFromChanList([]chan struct{}{a, b, c, a, b, a}, a)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, b, result[0])
	assert.Equal(t, b, result[1])
	assert.Equal(t, c, result[2])
}
