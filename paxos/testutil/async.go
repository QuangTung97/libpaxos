package testutil

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func RunAsync[T any](t *testing.T, fn func() T) func() T {
	var finished atomic.Bool
	resultCh := make(chan T)

	go func() {
		result := fn()
		finished.Store(true)
		resultCh <- result
	}()

	synctest.Wait()

	if finished.Load() {
		t.Error("async function should not have finished")
	}

	return func() T {
		return <-resultCh
	}
}
