package async

type Runtime interface {
	NewThread(callback func(ctx Context)) Context
	AddNext(ctx Context, callback func(ctx Context))
	RunNext()
}

func NewSimulateRuntime() *SimulateRuntime {
	return &SimulateRuntime{}
}

type SimulateRuntime struct {
	currentTid  ThreadID
	activeQueue []nextActionInfo
}

var _ Runtime = &SimulateRuntime{}

type nextActionInfo struct {
	ctx      Context
	callback func(ctx Context)
}

func (r *SimulateRuntime) NewThread(callback func(ctx Context)) Context {
	r.currentTid++

	ctx := NewSimulateContext(r.currentTid)

	r.activeQueue = append(r.activeQueue, nextActionInfo{
		ctx:      ctx,
		callback: callback,
	})

	return ctx
}

func (r *SimulateRuntime) AddNext(ctx Context, callback func(ctx Context)) {
	r.activeQueue = append(r.activeQueue, nextActionInfo{
		ctx:      ctx,
		callback: callback,
	})
}

func (r *SimulateRuntime) RunNext() {
	if len(r.activeQueue) == 0 {
		return
	}

	action := r.activeQueue[0]
	r.activeQueue = r.activeQueue[1:]

	action.callback(action.ctx)
}
