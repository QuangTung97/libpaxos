package async

type WaitStatus int

const (
	WaitStatusSuccess WaitStatus = iota + 1
	WaitStatusWaiting
)

type KeyWaiter[T comparable] interface {
	Run(ctx Context, key T, callback func(ctx Context) WaitStatus)
	Signal(key T)
	Broadcast()
}

func NewSimulateKeyWaiter[T comparable](rt Runtime) KeyWaiter[T] {
	return &simulateKeyWaiter[T]{
		rt:      rt,
		waitMap: map[T][]nextActionInfo{},
	}
}

type simulateKeyWaiter[T comparable] struct {
	rt      Runtime
	waitMap map[T][]nextActionInfo
}

func (w *simulateKeyWaiter[T]) Run(
	ctx Context, key T, callback func(ctx Context) WaitStatus,
) {
	var actionCallback func(ctx Context)

	actionCallback = func(ctx Context) {
		status := callback(ctx)
		if status == WaitStatusSuccess {
			return
		}

		w.waitMap[key] = append(w.waitMap[key], nextActionInfo{
			ctx:      ctx.(*simulateContext),
			callback: actionCallback,
		})
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
