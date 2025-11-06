package async

import (
	"sync"

	"github.com/QuangTung97/libpaxos/cond"
)

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

	Run(ctx Context, key T, callback func(ctx Context, err error) (WaitStatus, error))
	Signal(key T)

	NumWaitKeys() int
}

// ================================================================
// Real Waiter
// ================================================================

func NewKeyWaiter[T comparable](mut *sync.Mutex) KeyWaiter[T] {
	return &realKeyWaiter[T]{
		mut:  mut,
		cond: cond.NewKeyCond[T](mut),
	}
}

type realKeyWaiter[T comparable] struct {
	mut  *sync.Mutex
	cond *cond.KeyCond[T]
}

func (w *realKeyWaiter[T]) Run(
	ctx Context, key T, callback func(ctx Context, err error) (WaitStatus, error),
) {
	w.mut.Lock()
	defer w.mut.Unlock()

	for {
		status, err := callback(ctx, nil)
		if err != nil {
			return
		}

		if status == WaitStatusSuccess {
			return
		}

		if err := w.cond.Wait(ctx.ToContext(), key); err != nil {
			_, _ = callback(ctx, err)
			return
		}
	}
}

// Signal must be used in mutex
func (w *realKeyWaiter[T]) Signal(key T) {
	w.cond.Signal(key)
}

// Broadcast must be used in mutex
func (w *realKeyWaiter[T]) Broadcast() {
	w.cond.Broadcast()
}

func (w *realKeyWaiter[T]) NumWaitKeys() int {
	return w.cond.NumWaitKeys()
}

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
	ctx Context, key T, callback func(ctx Context, err error) (WaitStatus, error),
) {
	var actionCallback func(ctx Context)

	actionCallback = func(inputCtx Context) {
		ctx := inputCtx.(*simulateContext)

		status, err := callback(ctx, ctx.cancelErr)
		if err != nil {
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

func (w *simulateKeyWaiter[T]) NumWaitKeys() int {
	return len(w.waitMap)
}
