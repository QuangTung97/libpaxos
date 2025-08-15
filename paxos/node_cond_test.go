package paxos_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
	"github.com/QuangTung97/libpaxos/paxos/testutil"
)

func TestNodeCond__Waiting(t *testing.T) {
	var mut sync.Mutex
	var finished bool

	cond := NewNodeCond(&mut)
	node1 := fake.NewNodeID(1)

	ct := testutil.NewConcurrentTest(t)

	fn1 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node1); err != nil {
				return err
			}
		}
		return nil
	})

	fn1.AssertNotFinished(t)
}

func TestNodeCond__Multi_Nodes__Signal_One(t *testing.T) {
	var mut sync.Mutex
	var finished bool

	cond := NewNodeCond(&mut)

	node1 := fake.NewNodeID(1)
	node2 := fake.NewNodeID(2)

	ct := testutil.NewConcurrentTest(t)

	fn1 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node1); err != nil {
				return err
			}
		}
		return nil
	})

	fn2 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node2); err != nil {
				return err
			}
		}
		return nil
	})

	time.Sleep(10 * time.Millisecond)

	mut.Lock()
	finished = true
	cond.Signal(node1)
	mut.Unlock()

	fn1.AssertFinished(t, nil)
	fn2.AssertNotFinished(t)
}

func TestNodeCond__Multi_Nodes__Broadcast(t *testing.T) {
	var mut sync.Mutex
	var finished bool

	cond := NewNodeCond(&mut)

	node1 := fake.NewNodeID(1)
	node2 := fake.NewNodeID(2)

	ct := testutil.NewConcurrentTest(t)

	fn1 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node1); err != nil {
				return err
			}
		}
		return nil
	})

	fn2 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node2); err != nil {
				return err
			}
		}
		return nil
	})

	time.Sleep(10 * time.Millisecond)

	mut.Lock()
	finished = true
	cond.Broadcast()
	mut.Unlock()

	fn1.AssertFinished(t, nil)
	fn2.AssertFinished(t, nil)

	// signal again
	mut.Lock()
	cond.Signal(node1)
	mut.Unlock()
}

func TestNodeCond__Multi_Wait_Same_Node(t *testing.T) {
	var mut sync.Mutex
	var finished bool

	cond := NewNodeCond(&mut)

	node1 := fake.NewNodeID(1)

	ct := testutil.NewConcurrentTest(t)

	fn1 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node1); err != nil {
				return err
			}
		}
		return nil
	})

	fn2 := ct.Go(func(ctx context.Context) error {
		mut.Lock()
		defer mut.Unlock()
		for !finished {
			if err := cond.Wait(ctx, node1); err != nil {
				return err
			}
		}
		return nil
	})

	time.Sleep(10 * time.Millisecond)

	mut.Lock()
	finished = true
	cond.Signal(node1)
	mut.Unlock()

	fn1.AssertFinished(t, nil)
	fn2.AssertFinished(t, nil)
}
