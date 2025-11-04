package async

import "context"

type ThreadID int64

type Context interface {
	Cancel()
}

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
