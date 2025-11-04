package async

import "context"

type ThreadID int64

type Context interface {
	ToContext() context.Context
	Cancel()
}

// ================================================================
// Read Context
// ================================================================

func NewContext() Context {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	return &realContext{
		ctx:      ctx,
		cancelFn: cancel,
	}
}

type realContext struct {
	ctx      context.Context
	cancelFn func()
}

func (c *realContext) ToContext() context.Context {
	return c.ctx
}

func (c *realContext) Cancel() {
	c.cancelFn()
}

// ================================================================
// Simulate Context
// ================================================================

type simulateContext struct {
	tid           ThreadID
	startCallback func(ctx Context)
	err           error
	broadcastSet  map[Broadcaster]struct{}
}

func newSimulateContext(tid ThreadID) *simulateContext {
	return &simulateContext{
		tid:          tid,
		broadcastSet: map[Broadcaster]struct{}{},
	}
}

var _ Context = &simulateContext{}

func (c *simulateContext) Cancel() {
	c.err = context.Canceled
	for fn := range c.broadcastSet {
		fn.Broadcast()
	}
	c.broadcastSet = map[Broadcaster]struct{}{}
}

func (c *simulateContext) ToContext() context.Context {
	return nil
}
