package testutil

import (
	"testing"
	"testing/synctest"
)

func RunAsync[T any](_ *testing.T, fn func() T) func() T {
	resultCh := make(chan T)
	go func() {
		result := fn()
		resultCh <- result
	}()

	synctest.Wait()

	return func() T {
		return <-resultCh
	}
}
