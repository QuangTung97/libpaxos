package async

import "context"

type Context interface {
	ToContext() context.Context
	Cancel()
	Err() error
}

// ================================================================
// Read Context
// ================================================================

func NewContext() Context {
	return NewContextFrom(context.Background())
}

func NewContextFrom(inputCtx context.Context) Context {
	ctx, cancel := context.WithCancel(inputCtx)
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

func (c *realContext) Err() error {
	return c.ctx.Err()
}

// ================================================================
// Simulate Context
// ================================================================

type threadGeneration int64

type simulateContext struct {
	generation threadGeneration

	threadDetail  string
	startCallback func(ctx Context)
	cancelErr     error
	broadcastSet  map[Broadcaster]struct{}

	internalSeqMap map[SequenceID]*sequenceActionState
}

func newSimulateContext(detail string, startCallback func(ctx Context)) *simulateContext {
	return &simulateContext{
		generation:    1,
		threadDetail:  detail,
		startCallback: startCallback,
		broadcastSet:  map[Broadcaster]struct{}{},
	}
}

var _ Context = &simulateContext{}

func (c *simulateContext) getStartThreadDetail() string {
	return c.threadDetail + "::init"
}

func (c *simulateContext) getSequenceActionMap() map[SequenceID]*sequenceActionState {
	if c.internalSeqMap == nil {
		c.internalSeqMap = map[SequenceID]*sequenceActionState{}
	}
	return c.internalSeqMap
}

func (c *simulateContext) Cancel() {
	c.cancelErr = context.Canceled
	for fn := range c.broadcastSet {
		fn.Broadcast()
	}
	c.broadcastSet = map[Broadcaster]struct{}{}
}

func (c *simulateContext) ToContext() context.Context {
	return nil
}

func (c *simulateContext) Err() error {
	return c.cancelErr
}
