package paxos

import (
	"context"
	"sync"
)

// NodeCond is a condition variable for a set of nodes
type NodeCond struct {
	_ noCopy

	mut     *sync.Mutex
	waitSet map[NodeID][]chan struct{}
}

func NewNodeCond(mut *sync.Mutex) *NodeCond {
	return &NodeCond{
		mut:     mut,
		waitSet: map[NodeID][]chan struct{}{},
	}
}

// Wait must be used in mutex
func (c *NodeCond) Wait(ctx context.Context, nodeID NodeID) error {
	signalCh := make(chan struct{})
	prev := c.waitSet[nodeID]
	c.waitSet[nodeID] = append(prev, signalCh)

	c.mut.Unlock()

	select {
	case <-signalCh:
		c.mut.Lock()
		return nil

	case <-ctx.Done():
		c.mut.Lock()
		c.Signal(nodeID)
		return ctx.Err()
	}
}

// Signal must be used in mutex
func (c *NodeCond) Signal(nodeID NodeID) {
	allChannels := c.waitSet[nodeID]
	delete(c.waitSet, nodeID)
	for _, ch := range allChannels {
		close(ch)
	}
}

// Broadcast must be used in mutex
func (c *NodeCond) Broadcast() {
	for nodeID := range c.waitSet {
		c.Signal(nodeID)
	}
}

// -----------------------------------------------------

type noCopy struct {
}

var _ sync.Locker = &noCopy{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
