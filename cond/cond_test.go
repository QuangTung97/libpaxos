package cond

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/testutil"
)

const (
	key1 = "key01"
	key2 = "key02"
)

func TestNoCopy(t *testing.T) {
	var cond noCopy
	var x int
	cond.Lock()
	x++
	cond.Unlock()
}

func TestKeyCond__Waiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mut sync.Mutex
		var finished bool

		cond := NewKeyCond[string](&mut)

		ct := testutil.NewConcurrentTest(t)

		fn1 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key1); err != nil {
					return err
				}
			}
			return nil
		})

		fn1.AssertNotFinished(t)
	})
}

func TestKeyCond__Multi_Keys__Signal_One(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mut sync.Mutex
		var finished bool

		cond := NewKeyCond[string](&mut)

		ct := testutil.NewConcurrentTest(t)

		fn1 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key1); err != nil {
					return err
				}
			}
			return nil
		})

		fn2 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key2); err != nil {
					return err
				}
			}
			return nil
		})

		fn1.AssertNotFinished(t)
		fn2.AssertNotFinished(t)

		mut.Lock()
		finished = true
		cond.Signal(key1)
		mut.Unlock()

		synctest.Wait()

		fn1.AssertFinished(t, nil)
		fn2.AssertNotFinished(t)
	})
}

func TestKeyCond__Multi_Nodes__Broadcast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mut sync.Mutex
		var finished bool

		cond := NewKeyCond[string](&mut)

		ct := testutil.NewConcurrentTest(t)

		fn1 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key1); err != nil {
					return err
				}
			}
			return nil
		})

		fn2 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key2); err != nil {
					return err
				}
			}
			return nil
		})

		fn1.AssertNotFinished(t)
		fn2.AssertNotFinished(t)

		// check num wait
		mut.Lock()
		numWait := cond.NumWaitKeys()
		mut.Unlock()
		assert.Equal(t, 2, numWait)

		mut.Lock()
		finished = true
		cond.Broadcast()
		mut.Unlock()

		synctest.Wait()

		fn1.AssertFinished(t, nil)
		fn2.AssertFinished(t, nil)

		// signal again
		mut.Lock()
		cond.Signal(key1)
		mut.Unlock()
	})
}

func TestKeyCond__Multi_Wait_Same_Key(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mut sync.Mutex
		var finished bool

		cond := NewKeyCond[string](&mut)
		ct := testutil.NewConcurrentTest(t)

		fn1 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key1); err != nil {
					return err
				}
			}
			return nil
		})

		fn2 := ct.Go(func(ctx context.Context) error {
			mut.Lock()
			defer mut.Unlock()
			for !finished {
				if err := cond.Wait(ctx, key1); err != nil {
					return err
				}
			}
			return nil
		})

		mut.Lock()
		finished = true
		cond.Signal(key1)
		mut.Unlock()

		synctest.Wait()

		fn1.AssertFinished(t, nil)
		fn2.AssertFinished(t, nil)
	})
}
