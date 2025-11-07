package async

type AddNextFunc func(ctx Context, callback func(ctx Context))

func SimpleAddNextFunc(ctx Context, callback func(ctx Context)) {
	callback(ctx)
}

type SequenceID int64

func NewSimulateRuntime() *SimulateRuntime {
	return &SimulateRuntime{}
}

type SimulateRuntime struct {
	activeQueue []nextActionInfo
	lastSeqID   SequenceID
}

type nextActionInfo struct {
	ctx        *simulateContext
	generation threadGeneration
	callback   func(ctx Context)
}

func newNextActionInfo(ctx *simulateContext, callback func(ctx Context)) nextActionInfo {
	return nextActionInfo{
		ctx:        ctx,
		generation: ctx.generation,
		callback:   callback,
	}
}

type sequenceActionState struct {
	seqID          SequenceID
	isRunning      bool
	pendingActions []nextActionInfo // TODO use real queue
}

func (r *SimulateRuntime) NewThread(callback func(ctx Context)) Context {
	ctx := newSimulateContext(callback)
	r.doAddNext(ctx, callback)
	return ctx
}

func (r *SimulateRuntime) AddNext(ctx Context, callback func(ctx Context)) {
	r.doAddNext(ctx.(*simulateContext), callback)
}

func (r *SimulateRuntime) SequenceAddNext(
	inputCtx Context, seqID SequenceID, callback func(ctx Context),
) {
	AssertTrue(seqID > 0)
	ctx := inputCtx.(*simulateContext)

	seqMap := ctx.getSequenceActionMap()

	state, ok := seqMap[seqID]
	if !ok {
		state = &sequenceActionState{
			seqID: seqID,
		}
		seqMap[seqID] = state
	}

	state.pendingActions = append(state.pendingActions, newNextActionInfo(ctx, callback))
	r.startIfNotRunning(ctx, state)
}

func (r *SimulateRuntime) startIfNotRunning(threadCtx *simulateContext, state *sequenceActionState) {
	if state.isRunning {
		return
	}

	state.isRunning = true
	action := state.pendingActions[0]
	state.pendingActions = state.pendingActions[1:]

	r.doAddNext(action.ctx, func(ctx Context) {
		action.callback(ctx)
		state.isRunning = false

		if len(state.pendingActions) == 0 {
			delete(threadCtx.getSequenceActionMap(), state.seqID)
		} else {
			r.startIfNotRunning(threadCtx, state)
		}
	})
}

func (r *SimulateRuntime) doAddNext(ctx *simulateContext, callback func(ctx Context)) {
	r.activeQueue = append(r.activeQueue, newNextActionInfo(ctx, callback))
}

func (r *SimulateRuntime) RunNext() bool {
	return r.doRunWithChooseFunc(func() nextActionInfo {
		action := r.activeQueue[0]
		r.activeQueue = r.activeQueue[1:]
		return action
	})
}

func (r *SimulateRuntime) RunRandomAction(randFunc func(n int) int) bool {
	return r.doRunWithChooseFunc(func() nextActionInfo {
		index := randFunc(len(r.activeQueue))
		action := r.activeQueue[index]

		lastIndex := len(r.activeQueue) - 1
		r.activeQueue[index] = r.activeQueue[lastIndex]
		r.activeQueue = r.activeQueue[:lastIndex]

		return action
	})
}

func (r *SimulateRuntime) doRunWithChooseFunc(chooseFunc func() nextActionInfo) bool {
	for len(r.activeQueue) > 0 {
		action := chooseFunc()
		if action.generation == action.ctx.generation {
			action.callback(action.ctx)
			return true
		}
	}
	return false
}

func (r *SimulateRuntime) RestartThread(inputCtx Context) {
	ctx := inputCtx.(*simulateContext)
	ctx.generation++
	ctx.internalSeqMap = nil
	r.doAddNext(ctx, ctx.startCallback)
}

func (r *SimulateRuntime) NewSequence() SequenceID {
	r.lastSeqID++
	return r.lastSeqID
}

func (r *SimulateRuntime) GetQueueSize() int {
	return len(r.activeQueue)
}

func AssertTrue(b bool) {
	if !b {
		panic("must be true here")
	}
}
