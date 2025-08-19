package testutil

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

func TestRunAsync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := make(chan bool)

		getFn, _ := RunAsync(t, func() bool {
			return <-ch
		})

		ch <- true

		assert.Equal(t, true, getFn())
	})
}
