package async

type Runtime interface {
	AddNext(ctx Context, callback func(ctx Context))
}

func NewSimulateRuntime() *SimulateRuntime {
	return &SimulateRuntime{
		threadMap: map[ThreadID]*simulateContext{},
	}
}

type SimulateRuntime struct {
	currentTid  ThreadID
	activeQueue []nextActionInfo
	threadMap   map[ThreadID]*simulateContext
}

var _ Runtime = &SimulateRuntime{}

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
	r.threadMap[tid] = ctx

	return ctx
}

func (r *SimulateRuntime) AddNext(ctx Context, callback func(ctx Context)) {
	r.doAddNext(ctx.(*simulateContext), callback)
}

func (r *SimulateRuntime) doAddNext(ctx *simulateContext, callback func(ctx Context)) {
	ctx.refCount++
	action := nextActionInfo{
		ctx:      ctx,
		callback: callback,
	}
	r.activeQueue = append(r.activeQueue, action)
}

func (r *SimulateRuntime) decreaseRefCount(ctx *simulateContext) {
	ctx.refCount--
	if ctx.refCount <= 0 {
		delete(r.threadMap, ctx.tid)
	}
}

func (r *SimulateRuntime) RunNext() bool {
	if len(r.activeQueue) == 0 {
		return false
	}

	action := r.activeQueue[0]
	r.activeQueue = r.activeQueue[1:]

	action.callback(action.ctx)

	r.decreaseRefCount(action.ctx)

	return true
}

func (r *SimulateRuntime) RestartThread(tid ThreadID) {
	newActions := make([]nextActionInfo, 0, len(r.activeQueue))
	for _, action := range r.activeQueue {
		if action.ctx.tid == tid {
			continue
		}
		newActions = append(newActions, action)
	}
	r.activeQueue = newActions

	ctx := r.threadMap[tid]
	r.AddNext(ctx, ctx.startCallback)
}

func (r *SimulateRuntime) CheckInvariant() {
	allTidSet := map[ThreadID]struct{}{}
	for _, action := range r.activeQueue {
		tid := action.ctx.tid
		allTidSet[tid] = struct{}{}
	}

	AssertTrue(len(allTidSet) == len(r.threadMap))
}

func AssertTrue(b bool) {
	if !b {
		panic("must be true here")
	}
}
