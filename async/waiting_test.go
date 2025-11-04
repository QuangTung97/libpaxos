package async

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type actionListTest struct {
	actions []string
}

func newActionListTest() *actionListTest {
	return &actionListTest{
		actions: make([]string, 0),
	}
}

func (a *actionListTest) add(format string, args ...any) {
	a.actions = append(a.actions, fmt.Sprintf(format, args...))
}

func runAllActions(rt *SimulateRuntime) {
	for rt.RunNext() {
	}
}

func TestKeyWaiter(t *testing.T) {
	t.Run("simple success", func(t *testing.T) {
		rt := NewSimulateRuntime()
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		count := 0

		rt.NewThread(func(ctx Context) {
			actions.add("new-thread")
			w.Run(ctx, "key01", func(ctx Context) WaitStatus {
				count++
				actions.add("wait:%d", count)
				return WaitStatusSuccess
			})
		})
		assert.Equal(t, []string{}, actions.actions)

		assert.Equal(t, true, rt.RunNext())
		assert.Equal(t, []string{"new-thread", "wait:1"}, actions.actions)

		// do nothing
		assert.Equal(t, false, rt.RunNext())
		assert.Equal(t, []string{"new-thread", "wait:1"}, actions.actions)

		// broadcast
		w.Broadcast()

		// do nothing
		assert.Equal(t, false, rt.RunNext())
		assert.Equal(t, []string{"new-thread", "wait:1"}, actions.actions)
	})

	t.Run("with waiting", func(t *testing.T) {
		rt := NewSimulateRuntime()
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		count := 0
		rt.NewThread(func(ctx Context) {
			actions.add("new-thread")
			w.Run(ctx, "key01", func(ctx Context) WaitStatus {
				count++
				actions.add("wait:%d", count)
				if count > 1 {
					return WaitStatusSuccess
				}
				return WaitStatusWaiting
			})
		})
		assert.Equal(t, []string{}, actions.actions)

		assert.Equal(t, true, rt.RunNext())
		assert.Equal(t, []string{"new-thread", "wait:1"}, actions.actions)

		// broadcast
		w.Broadcast()
		assert.Equal(t, true, rt.RunNext())
		assert.Equal(t, []string{"new-thread", "wait:1", "wait:2"}, actions.actions)

		// broadcast again
		w.Broadcast()
		assert.Equal(t, false, rt.RunNext())
		assert.Equal(t, []string{"new-thread", "wait:1", "wait:2"}, actions.actions)
	})

	t.Run("with waiting, 2 keys", func(t *testing.T) {
		rt := NewSimulateRuntime()
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		rt.NewThread(func(ctx Context) {
			actions.add("new-thread01")
			w.Run(ctx, "key01", func(ctx Context) WaitStatus {
				actions.add("wait key01")
				return WaitStatusWaiting
			})
		})
		rt.NewThread(func(ctx Context) {
			actions.add("new-thread02")
			w.Run(ctx, "key02", func(ctx Context) WaitStatus {
				actions.add("wait key02")
				return WaitStatusWaiting
			})
		})
		runAllActions(rt)
		assert.Equal(t, []string{
			"new-thread01",
			"wait key01",
			"new-thread02",
			"wait key02",
		}, actions.actions)

		// signal
		w.Signal("key01")
		runAllActions(rt)
		assert.Equal(t, []string{
			"new-thread01",
			"wait key01",
			"new-thread02",
			"wait key02",
			"wait key01",
		}, actions.actions)
	})

	// TODO add cancel
}
