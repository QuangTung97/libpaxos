package async

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimulateRuntime(t *testing.T) {
	rt := NewSimulateRuntime()

	var actions []string
	ctx := rt.NewThread(func(ctx Context) {
		actions = append(actions, "new-thread")

		rt.AddNext(ctx, func(ctx Context) {
			actions = append(actions, "next-action")

			rt.AddNext(ctx, func(ctx Context) {
				actions = append(actions, "sub-action")
			})
		})
	})

	assert.Equal(t, ThreadID(1), ctx.GetThreadID())
	assert.Equal(t, []string(nil), actions)

	// run
	rt.RunNext()
	assert.Equal(t, []string{"new-thread"}, actions)

	// run
	rt.RunNext()
	assert.Equal(t, []string{
		"new-thread",
		"next-action",
	}, actions)

	// run
	rt.RunNext()
	assert.Equal(t, []string{
		"new-thread",
		"next-action",
		"sub-action",
	}, actions)
}
