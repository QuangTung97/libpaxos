package async

type ThreadID int64

type Context interface {
	GetThreadID() ThreadID
	Cancel()
}

type simulateContext struct {
	tid ThreadID
}

func NewSimulateContext(tid ThreadID) Context {
	return &simulateContext{tid: tid}
}

var _ Context = &simulateContext{}

func (c *simulateContext) GetThreadID() ThreadID {
	return c.tid
}

func (c *simulateContext) Cancel() {
}
