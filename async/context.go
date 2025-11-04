package async

type ThreadID int64

type Context interface {
	GetThreadID() ThreadID
	Cancel()
}

type simulateContext struct {
	tid           ThreadID
	startCallback func(ctx Context)
	refCount      int
}

func newSimulateContext(tid ThreadID) *simulateContext {
	return &simulateContext{
		tid:      tid,
		refCount: 0,
	}
}

var _ Context = &simulateContext{}

func (c *simulateContext) GetThreadID() ThreadID {
	return c.tid
}

func (c *simulateContext) Cancel() {
}
