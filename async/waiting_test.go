package async

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/testutil"
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

func (a *actionListTest) clear() {
	a.actions = a.actions[:0]
}

func runAllActions(rt *SimulateRuntime) {
	for rt.RunNext() {
	}
	rt.CheckInvariant()
}

func TestSimulateKeyWaiter(t *testing.T) {
	t.Run("simple success", func(t *testing.T) {
		rt := NewSimulateRuntime()
		t.Cleanup(rt.CheckInvariant)
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		count := 0

		rt.NewThread(func(ctx Context) {
			actions.add("new-thread")
			w.Run(ctx, "key01", func(ctx Context, err error) WaitStatus {
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
		t.Cleanup(rt.CheckInvariant)
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		count := 0
		rt.NewThread(func(ctx Context) {
			actions.add("new-thread")
			w.Run(ctx, "key01", func(ctx Context, err error) WaitStatus {
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
		t.Cleanup(rt.CheckInvariant)
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		rt.NewThread(func(ctx Context) {
			actions.add("new-thread01")
			w.Run(ctx, "key01", func(ctx Context, err error) WaitStatus {
				actions.add("wait key01")
				return WaitStatusWaiting
			})
		})
		rt.NewThread(func(ctx Context) {
			actions.add("new-thread02")
			w.Run(ctx, "key02", func(ctx Context, err error) WaitStatus {
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

		// check num waiting
		assert.Equal(t, 2, w.NumWaitKeys())

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

	t.Run("with cancel", func(t *testing.T) {
		rt := NewSimulateRuntime()
		t.Cleanup(rt.CheckInvariant)
		w := NewSimulateKeyWaiter[string](rt)

		actions := newActionListTest()
		count := 0

		ctx := rt.NewThread(func(ctx Context) {
			actions.add("new-thread")
			w.Run(ctx, "key01", func(ctx Context, err error) WaitStatus {
				if err != nil {
					actions.add("wait error: %v", err)
					return WaitStatusSuccess
				}

				count++
				actions.add("wait:%d", count)
				return WaitStatusWaiting
			})
		})
		runAllActions(rt)
		assert.Equal(t, []string{"new-thread", "wait:1"}, actions.actions)

		// cancel context
		ctx.Cancel()

		actions.clear()
		runAllActions(rt)
		assert.Equal(t, []string{"wait error: context canceled"}, actions.actions)
	})
}

func TestRealKeyWaiter__Broadcast(t *testing.T) {
	var mut sync.Mutex
	finished := false

	w := NewKeyWaiter[string](&mut)
	ctx := NewContext()

	synctest.Test(t, func(t *testing.T) {
		runFn := func(key string) bool {
			w.Run(ctx, key, func(ctx Context, err error) WaitStatus {
				if err != nil {
					return WaitStatusSuccess
				}

				if finished {
					return WaitStatusSuccess
				}
				return WaitStatusWaiting
			})
			return true
		}

		returnFn1, assertNotFinish1 := testutil.RunAsync(t, func() bool {
			return runFn("key01")
		})
		assertNotFinish1()

		returnFn2, assertNotFinish2 := testutil.RunAsync(t, func() bool {
			return runFn("key02")
		})
		assertNotFinish2()

		// check number of keys
		mut.Lock()
		numWait := w.NumWaitKeys()
		mut.Unlock()
		assert.Equal(t, 2, numWait)

		mut.Lock()
		finished = true
		w.Broadcast()
		mut.Unlock()

		assert.Equal(t, true, returnFn1())
		assert.Equal(t, true, returnFn2())
	})
}

func TestRealKeyWaiter__Signal(t *testing.T) {
	var mut sync.Mutex
	finished := false

	w := NewKeyWaiter[string](&mut)
	ctx := NewContext()

	synctest.Test(t, func(t *testing.T) {
		runFn := func(key string) bool {
			w.Run(ctx, key, func(ctx Context, err error) WaitStatus {
				if err != nil {
					return WaitStatusSuccess
				}

				if finished {
					return WaitStatusSuccess
				}
				return WaitStatusWaiting
			})
			return true
		}

		returnFn1, assertNotFinish1 := testutil.RunAsync(t, func() bool {
			return runFn("key01")
		})
		assertNotFinish1()

		returnFn2, assertNotFinish2 := testutil.RunAsync(t, func() bool {
			return runFn("key02")
		})
		assertNotFinish2()

		mut.Lock()
		finished = true
		w.Signal("key01")
		mut.Unlock()

		assert.Equal(t, true, returnFn1())
		assertNotFinish2()

		mut.Lock()
		w.Broadcast()
		mut.Unlock()

		assert.Equal(t, true, returnFn2())
	})
}

func TestRealKeyWaiter__Context_Cancel(t *testing.T) {
	var mut sync.Mutex
	finished := false

	w := NewKeyWaiter[string](&mut)
	ctx := NewContext()

	synctest.Test(t, func(t *testing.T) {
		runFn := func(key string) error {
			var outputErr error
			w.Run(ctx, key, func(ctx Context, err error) WaitStatus {
				if err != nil {
					outputErr = err
					return WaitStatusSuccess
				}

				if finished {
					return WaitStatusSuccess
				}
				return WaitStatusWaiting
			})
			return outputErr
		}

		returnFn1, assertNotFinish1 := testutil.RunAsync(t, func() error {
			return runFn("key01")
		})
		assertNotFinish1()

		returnFn2, assertNotFinish2 := testutil.RunAsync(t, func() error {
			return runFn("key02")
		})
		assertNotFinish2()

		ctx.Cancel()
		synctest.Wait()

		assert.Equal(t, context.Canceled, returnFn1())
		assert.Equal(t, context.Canceled, returnFn2())
	})
}
