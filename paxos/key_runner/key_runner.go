package key_runner

import (
	"sync"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos/waiting"
)

// New creates an object that manages a set of runners based on keys and values.
// When a new key is added / removed => a runner will be started / cancelled accordingly.
// When the value of a key is changed => the runner that is associated with that key will also be restarted.
func New[K comparable, V comparable](
	getKey func(V) K,
	handler func(ctx async.Context, val V),
) *KeyRunner[K, V] {
	return &KeyRunner[K, V]{
		getKey:  getKey,
		handler: handler,

		activeKeys: map[K]struct{}{},
		running:    map[K]*runThread[V]{},

		wg: waiting.NewWaitGroup(),
	}
}

type KeyRunner[K comparable, V comparable] struct {
	getKey  func(V) K
	handler func(ctx async.Context, val V)

	mut        sync.Mutex
	activeKeys map[K]struct{}      // expected set
	running    map[K]*runThread[V] // running set

	wg *waiting.WaitGroup
}

// ==================================
// Public Methods
// ==================================

func (r *KeyRunner[K, V]) Upsert(values []V) bool {
	startList, updated := r.upsertInternal(values)

	for _, tmp := range startList {
		entry := tmp
		r.wg.Go(func() {
			r.doRunHandler(entry)
		})
	}

	return updated
}

func (r *KeyRunner[K, V]) Shutdown() {
	r.upsertInternal(nil)
	r.wg.Wait()
}

// ==================================
// Private Methods
// ==================================

func (r *KeyRunner[K, V]) doRunHandler(entry startEntry[V]) {
	for {
		r.handler(entry.ctx, entry.val)

		key := r.getKey(entry.val)

		var continued bool
		entry, continued = r.finishInternal(key)
		if !continued {
			return
		}
	}
}

type runThread[V comparable] struct {
	val    V
	cancel func()
}

type startEntry[V comparable] struct {
	val V
	ctx async.Context
}

func (r *KeyRunner[K, V]) upsertInternal(values []V) ([]startEntry[V], bool) {
	r.mut.Lock()
	defer r.mut.Unlock()

	var updated bool
	var startList []startEntry[V]

	newSet := map[K]struct{}{}
	for _, val := range values {
		key := r.getKey(val)
		newSet[key] = struct{}{}
	}

	var deleteKeys []K
	for key := range r.activeKeys {
		_, ok := newSet[key]
		if ok {
			continue
		}

		updated = true
		deleteKeys = append(deleteKeys, key)
		thread := r.running[key]
		thread.cancel()
	}

	for _, key := range deleteKeys {
		delete(r.activeKeys, key)
	}

	for _, val := range values {
		key := r.getKey(val)

		_, existed := r.activeKeys[key]
		if existed {
			// update on changed
			thread := r.running[key]
			if thread.val != val {
				updated = true
				thread.val = val
				thread.cancel()
			}
			continue
		}

		r.activeKeys[key] = struct{}{}
		updated = true

		thread, ok := r.running[key]
		if ok {
			thread.val = val
			continue
		}

		// do insert new
		ctx := async.NewContext()
		r.running[key] = &runThread[V]{
			val:    val,
			cancel: ctx.Cancel,
		}

		startList = append(startList, startEntry[V]{
			val: val,
			ctx: ctx,
		})
	}

	return startList, updated
}

func (r *KeyRunner[K, V]) finishInternal(key K) (startEntry[V], bool) {
	r.mut.Lock()
	defer r.mut.Unlock()

	_, ok := r.activeKeys[key]
	if !ok {
		delete(r.running, key)
		return startEntry[V]{}, false
	}

	thread := r.running[key]

	ctx := async.NewContext()
	thread.cancel = ctx.Cancel

	return startEntry[V]{
		val: thread.val,
		ctx: ctx,
	}, true
}
