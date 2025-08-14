package paxos

import (
	"context"
	"sync"
)

// NodeCond is a condition variable for a set of nodes
type NodeCond struct {
	noCopy
	mut     *sync.Mutex
	waitSet map[NodeID]chan struct{}
}

func NewNodeCond(mut *sync.Mutex) *NodeCond {
	return &NodeCond{
		mut:     mut,
		waitSet: map[NodeID]chan struct{}{},
	}
}

// Wait must be used in mutex
func (c *NodeCond) Wait(ctx context.Context, nodeID NodeID) error {
	prev, ok := c.waitSet[nodeID]
	if ok {
		close(prev)
	}

	signalCh := make(chan struct{})
	c.waitSet[nodeID] = signalCh

	c.mut.Unlock()

	select {
	case <-signalCh:
		c.mut.Lock()
		return nil

	case <-ctx.Done():
		c.mut.Lock()
		delete(c.waitSet, nodeID)
		return ctx.Err()
	}
}

// Signal must be used in mutex
func (c *NodeCond) Signal(nodeID NodeID) {
	signChan, ok := c.waitSet[nodeID]
	if ok {
		return
	}

	delete(c.waitSet, nodeID)
	close(signChan)
}

// Broadcast must be used in mutex
func (c *NodeCond) Broadcast() {
	for nodeID, signChan := range c.waitSet {
		close(signChan)
		delete(c.waitSet, nodeID)
	}
}

// -----------------------------------------------------

type noCopy struct {
}

var _ sync.Locker = &noCopy{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
