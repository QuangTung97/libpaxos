package cond

import (
	"context"
	"sync"
)

// KeyCond is a condition variable for a set of keys
type KeyCond[T comparable] struct {
	_ noCopy

	mut     *sync.Mutex
	waitSet map[T][]chan struct{}
}

func NewKeyCond[T comparable](mut *sync.Mutex) *KeyCond[T] {
	return &KeyCond[T]{
		mut:     mut,
		waitSet: map[T][]chan struct{}{},
	}
}

// Wait must be used in mutex
func (c *KeyCond[T]) Wait(ctx context.Context, key T) error {
	signalCh := make(chan struct{})
	prev := c.waitSet[key]
	c.waitSet[key] = append(prev, signalCh)

	c.mut.Unlock()

	select {
	case <-signalCh:
		c.mut.Lock()
		return nil

	case <-ctx.Done():
		c.mut.Lock()
		c.Signal(key)
		return ctx.Err()
	}
}

// Signal must be used in mutex
func (c *KeyCond[T]) Signal(key T) {
	allChannels := c.waitSet[key]
	delete(c.waitSet, key)
	for _, ch := range allChannels {
		close(ch)
	}
}

// Broadcast must be used in mutex
func (c *KeyCond[T]) Broadcast() {
	for key := range c.waitSet {
		c.Signal(key)
	}
}

// -----------------------------------------------------

type noCopy struct {
}

var _ sync.Locker = &noCopy{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
