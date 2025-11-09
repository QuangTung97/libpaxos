package async

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertTrue(t *testing.T) {
	assert.PanicsWithValue(t, "must be true here", func() {
		AssertTrue(false)
	})
}

func TestSimpleAddNextFunc(t *testing.T) {
	ctx := NewContext()
	var calls int
	SimpleAddNextFunc(ctx, "", func(ctx Context) {
		calls++
	})
	assert.Equal(t, 1, calls)
}

func TestSimulateRuntime(t *testing.T) {
	rt := NewSimulateRuntime()
	// check function signature
	var _ AddNextFunc = rt.AddNext

	var actions []string
	ctx := rt.NewThread("thread01", func(ctx Context) {
		actions = append(actions, "new-thread")

		rt.AddNext(ctx, "action01", func(ctx Context) {
			actions = append(actions, "next-action")

			rt.AddNext(ctx, "action02", func(ctx Context) {
				actions = append(actions, "sub-action")
			})
		})
	})

	assert.Equal(t, nil, ctx.ToContext())
	assert.Equal(t, []string(nil), actions)

	// run
	assert.True(t, rt.RunNext())
	assert.Equal(t, []string{"new-thread"}, actions)

	// run
	assert.True(t, rt.RunNext())
	assert.Equal(t, []string{
		"new-thread",
		"next-action",
	}, actions)

	// run
	assert.True(t, rt.RunNext())
	assert.Equal(t, []string{
		"new-thread",
		"next-action",
		"sub-action",
	}, actions)

	// run no action
	assert.False(t, rt.RunNext())
}

func TestSimulateRuntime__Restart_Thread(t *testing.T) {
	rt := NewSimulateRuntime()
	actions := newActionListTest()

	ctx := rt.NewThread("thread01", func(ctx Context) {
		actions.add("new thread 01")
		rt.AddNext(ctx, "action01", func(ctx Context) {
			actions.add("action 01")
		})
	})

	rt.NewThread("thread02", func(ctx Context) {
		actions.add("new thread 02")
		rt.AddNext(ctx, "action02", func(ctx Context) {
			actions.add("action 02")
		})
	})

	rt.RunNext()
	rt.RunNext()
	assert.Equal(t, []string{
		"new thread 01",
		"new thread 02",
	}, actions.getList())

	// restart
	rt.RestartThread(ctx)
	rt.RunNext()
	rt.RunNext()
	assert.Equal(t, []string{
		"action 02",
		"new thread 01",
	}, actions.getList())

	// run all
	runAllActions(rt)
	assert.Equal(t, []string{
		"action 01",
	}, actions.getList())

	// restart again
	rt.RestartThread(ctx)
	runAllActions(rt)
	assert.Equal(t, []string{
		"new thread 01",
		"action 01",
	}, actions.getList())
}

func TestSimulateRuntime_Sequence(t *testing.T) {
	rt := NewSimulateRuntime()
	actions := newActionListTest()

	ctx := rt.NewThread("thread01", func(ctx Context) {
		actions.add("new thread 01")
		seqID := rt.NewSequence()

		rt.SequenceAddNext(ctx, seqID, func(ctx Context) {
			actions.add("action 01")
		})
		rt.SequenceAddNext(ctx, seqID, func(ctx Context) {
			actions.add("action 02")
		})
		rt.SequenceAddNext(ctx, seqID, func(ctx Context) {
			actions.add("action 03")
		})
	})
	assert.Equal(t, 1, rt.GetQueueSize())

	rt.RunNext()
	assert.Equal(t, 1, rt.GetQueueSize())
	assert.Equal(t, []string{
		"new thread 01",
	}, actions.getList())

	rt.RunNext()
	assert.Equal(t, 1, rt.GetQueueSize())
	assert.Equal(t, []string{
		"action 01",
	}, actions.getList())

	// run all
	runAllActions(rt)
	assert.Equal(t, 0, rt.GetQueueSize())
	assert.Equal(t, []string{
		"action 02",
		"action 03",
	}, actions.getList())

	// check size of active seq map
	assert.Equal(t, 0, len(ctx.(*simulateContext).internalSeqMap))
}

func TestSimulateRuntime_Sequence__And_Restart_Thread(t *testing.T) {
	rt := NewSimulateRuntime()
	actions := newActionListTest()

	ctx := rt.NewThread("thread01", func(ctx Context) {
		actions.add("new thread 01")
		seqID := rt.NewSequence()

		rt.SequenceAddNext(ctx, seqID, func(ctx Context) {
			actions.add("action 01")
		})
		rt.SequenceAddNext(ctx, seqID, func(ctx Context) {
			actions.add("action 02")
		})
		rt.SequenceAddNext(ctx, seqID, func(ctx Context) {
			actions.add("action 03")
		})
	})
	assert.Equal(t, 1, rt.GetQueueSize())

	rt.RunNext()
	assert.Equal(t, 1, rt.GetQueueSize())
	assert.Equal(t, []string{
		"new thread 01",
	}, actions.getList())

	rt.RunNext()
	assert.Equal(t, 1, rt.GetQueueSize())
	assert.Equal(t, []string{
		"action 01",
	}, actions.getList())

	// restart
	rt.RestartThread(ctx)
	rt.RunNext()
	assert.Equal(t, 1, rt.GetQueueSize())
	assert.Equal(t, []string{
		"new thread 01",
	}, actions.getList())

	runAllActions(rt)
	assert.Equal(t, []string{
		"action 01",
		"action 02",
		"action 03",
	}, actions.getList())

	// check size of active seq map
	assert.Equal(t, 0, len(ctx.(*simulateContext).internalSeqMap))
}

func TestSimulateRuntime_RandomAction(t *testing.T) {
	rt := NewSimulateRuntime()
	actions := newActionListTest()

	rt.NewThread("thread01", func(ctx Context) {
		actions.add("action01")
	})
	rt.NewThread("thread02", func(ctx Context) {
		actions.add("action02")
	})
	rt.NewThread("thread03", func(ctx Context) {
		actions.add("action03")
	})

	var inputNum int
	rt.RunRandomAction(func(n int) int {
		inputNum = n
		return 1
	})
	assert.Equal(t, 3, inputNum)
	assert.Equal(t, []string{"action02"}, actions.getList())

	// run again
	rt.RunRandomAction(func(n int) int {
		inputNum = n
		return 0
	})
	assert.Equal(t, 2, inputNum)
	assert.Equal(t, []string{"action01"}, actions.getList())

	// run all
	runAllActions(rt)
	assert.Equal(t, []string{"action03"}, actions.getList())
}

func TestSimulateRuntime__Remove_All_Canceled(t *testing.T) {
	rt := NewSimulateRuntime()

	ctx := rt.NewThread("thread01", func(ctx Context) {
		rt.AddNext(ctx, "sub01", func(ctx Context) {
		})
	})
	rt.NewThread("thread02", func(ctx Context) {
	})

	assert.Equal(t, []string{
		"thread01::init",
		"thread02::init",
	}, rt.GetQueueDetails())

	ctx.Cancel()
	assert.Equal(t, []string{
		"thread01::init:<canceled>",
		"thread02::init",
	}, rt.GetQueueDetails())

	rt.RemoveAllCanceled()
	assert.Equal(t, []string{
		"thread02::init",
	}, rt.GetQueueDetails())
}
