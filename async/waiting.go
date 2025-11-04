package async

type WaitStatus int

const (
	WaitStatusSuccess WaitStatus = iota + 1
	WaitStatusWaiting
)

type Broadcaster interface {
	Broadcast()
}

type KeyWaiter[T comparable] interface {
	Broadcaster

	Run(ctx Context, key T, callback func(ctx Context, err error) WaitStatus)
	Signal(key T)
}

// ================================================================
// Real Waiter
// ================================================================

// ================================================================
// Simulate Waiter
// ================================================================

func NewSimulateKeyWaiter[T comparable](rt *SimulateRuntime) KeyWaiter[T] {
	return &simulateKeyWaiter[T]{
		rt:      rt,
		waitMap: map[T][]nextActionInfo{},
	}
}

type simulateKeyWaiter[T comparable] struct {
	rt      *SimulateRuntime
	waitMap map[T][]nextActionInfo
}

func (w *simulateKeyWaiter[T]) Run(
	ctx Context, key T, callback func(ctx Context, err error) WaitStatus,
) {
	var actionCallback func(ctx Context)

	actionCallback = func(inputCtx Context) {
		ctx := inputCtx.(*simulateContext)

		status := callback(ctx, ctx.err)
		if ctx.err != nil {
			return
		}
		if status == WaitStatusSuccess {
			return
		}

		w.waitMap[key] = append(w.waitMap[key], nextActionInfo{
			ctx:      ctx,
			callback: actionCallback,
		})
		ctx.broadcastSet[w] = struct{}{}
	}

	actionCallback(ctx)
}

func (w *simulateKeyWaiter[T]) Signal(key T) {
	list := w.waitMap[key]
	for _, action := range list {
		w.rt.AddNext(action.ctx, action.callback)
	}
	delete(w.waitMap, key)
}

func (w *simulateKeyWaiter[T]) Broadcast() {
	for key := range w.waitMap {
		w.Signal(key)
	}
}
