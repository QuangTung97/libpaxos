package waiting

import (
	"sync"
)

type WaitGroup struct {
	mut     sync.Mutex
	cond    *sync.Cond
	numWait int
}

func NewWaitGroup() *WaitGroup {
	wg := &WaitGroup{}
	wg.cond = sync.NewCond(&wg.mut)
	return wg
}

func (wg *WaitGroup) Go(fn func()) {
	if wg.cond == nil {
		panic("WaitGroup is not initialized")
	}

	wg.mut.Lock()
	wg.numWait++
	wg.mut.Unlock()

	go func() {
		defer func() {
			wg.mut.Lock()
			wg.numWait--
			if wg.numWait <= 0 {
				wg.cond.Signal()
			}
			wg.mut.Unlock()
		}()

		fn()
	}()
}

func (wg *WaitGroup) Wait() {
	wg.mut.Lock()
	for wg.numWait > 0 {
		wg.cond.Wait()
	}
	wg.mut.Unlock()
}
