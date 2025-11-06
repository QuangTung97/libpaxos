package async

type AddNextFunc func(ctx Context, callback func(ctx Context))

func SimpleAddNextFunc(ctx Context, callback func(ctx Context)) {
	callback(ctx)
}

func NewSimulateRuntime() *SimulateRuntime {
	return &SimulateRuntime{}
}

type SimulateRuntime struct {
	currentTid  ThreadID
	activeQueue []nextActionInfo
}

type nextActionInfo struct {
	ctx      *simulateContext
	callback func(ctx Context)
}

func (r *SimulateRuntime) NewThread(callback func(ctx Context)) Context {
	r.currentTid++
	tid := r.currentTid

	ctx := newSimulateContext(tid)
	r.doAddNext(ctx, callback)

	ctx.startCallback = callback

	return ctx
}

func (r *SimulateRuntime) AddNext(ctx Context, callback func(ctx Context)) {
	r.doAddNext(ctx.(*simulateContext), callback)
}

func (r *SimulateRuntime) doAddNext(ctx *simulateContext, callback func(ctx Context)) {
	action := nextActionInfo{
		ctx:      ctx,
		callback: callback,
	}
	r.activeQueue = append(r.activeQueue, action)
}

func (r *SimulateRuntime) RunNext() bool {
	if len(r.activeQueue) == 0 {
		return false
	}

	action := r.activeQueue[0]
	r.activeQueue = r.activeQueue[1:]

	action.callback(action.ctx)

	return true
}

func (r *SimulateRuntime) RestartThread(inputCtx Context) {
	ctx := inputCtx.(*simulateContext)
	tid := ctx.tid

	newActions := make([]nextActionInfo, 0, len(r.activeQueue))
	for _, action := range r.activeQueue {
		if action.ctx.tid == tid {
			continue
		}
		newActions = append(newActions, action)
	}
	r.activeQueue = newActions

	r.AddNext(ctx, ctx.startCallback)
}

func (r *SimulateRuntime) CheckInvariant() {
}
