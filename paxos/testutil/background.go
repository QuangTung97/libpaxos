package testutil

import "testing"

func RunInBackground(t *testing.T, fn func()) {
	finish := make(chan struct{})
	go func() {
		defer close(finish)
		fn()
	}()
	t.Cleanup(func() {
		<-finish
	})
}
