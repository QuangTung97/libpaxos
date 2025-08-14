package cond

import (
	"context"
	"sync"
)

type Cond struct {
	noCopy
	locker sync.Locker

	waitList []chan struct{}
}

func NewCond(locker sync.Locker) *Cond {
	return &Cond{
		locker: locker,
	}
}

func (c *Cond) Wait(ctx context.Context) error {
	signalChan := make(chan struct{})
	c.waitList = append(c.waitList, signalChan)
	c.locker.Unlock()

	select {
	case <-signalChan:
		c.locker.Lock()
		return nil

	case <-ctx.Done():
		c.locker.Lock()

		c.waitList = removeFromChanList(c.waitList, signalChan)
		return ctx.Err()
	}
}

// Broadcast must be called when the mutex is locked
func (c *Cond) Broadcast() {
	for _, ch := range c.waitList {
		close(ch)
	}
	c.waitList = nil
}

type noCopy struct {
}

var _ sync.Locker = &noCopy{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

func removeFromChanList(chanList []chan struct{}, ch chan struct{}) []chan struct{} {
	n := len(chanList)
	for i := 0; i < n; {
		if chanList[i] == ch {
			n--
			chanList[i] = chanList[n]
		} else {
			i++
		}
	}
	return chanList[:n]
}
