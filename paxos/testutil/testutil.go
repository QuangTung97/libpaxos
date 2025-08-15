package testutil

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func NewConcurrentTest(t *testing.T) *ConcurrentTest {
	c := &ConcurrentTest{}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	t.Cleanup(func() {
		c.cancel()
		c.wg.Wait()
	})

	return c
}

type ConcurrentTest struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	shortWaited bool
}

func (c *ConcurrentTest) Go(fn func(ctx context.Context) error) *RunHandle {
	h := &RunHandle{
		root: c,
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := fn(c.ctx)
		h.finished.Store(&errorInfo{err: err})
	}()

	return h
}

func (c *ConcurrentTest) shortWaitBeforeAssert() {
	if c.shortWaited {
		return
	}
	c.shortWaited = true
	time.Sleep(10 * time.Millisecond)
}

type errorInfo struct {
	err error
}

type RunHandle struct {
	root     *ConcurrentTest
	finished atomic.Pointer[errorInfo]
}

func (h *RunHandle) AssertNotFinished(t *testing.T) {
	h.root.shortWaitBeforeAssert()
	t.Helper()

	if h.finished.Load() != nil {
		t.Error("Function should not be finished")
	}
}

func (h *RunHandle) AssertFinished(t *testing.T, finishErr error) {
	h.root.shortWaitBeforeAssert()
	t.Helper()

	info := h.finished.Load()
	if info == nil {
		t.Error("Function should be finished")
		return
	}
	assert.Equal(t, finishErr, info.err)
}
