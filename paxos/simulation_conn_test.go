package paxos_test

import (
	"context"
	"fmt"
	"iter"
	"sync"

	. "github.com/QuangTung97/libpaxos/paxos"
)

type SimulationConn interface {
	CloseConn()
	Print()
	GetContext() context.Context
}

type simulateConn[Req, Resp any] struct {
	root       *simulationTestCase
	actionType simulateActionType
	fromNode   NodeID
	toNode     NodeID

	ctx    context.Context
	cancel func()

	sendChan chan Req
	recvChan chan Resp
	wg       sync.WaitGroup
}

func newSimulateConn[Req, Resp any](
	ctx context.Context,
	handlerState *simulationHandlers,
	toNode NodeID,
	actionType simulateActionType,
	requestHandler func(req Req) (iter.Seq[Resp], error),
	responseHandler func(resp Resp) error,
) *simulateConn[Req, Resp] {
	c := &simulateConn[Req, Resp]{
		root:       handlerState.root,
		actionType: actionType,
		fromNode:   handlerState.current,
		toNode:     toNode,

		sendChan: make(chan Req, 1),
		recvChan: make(chan Resp, 1),
	}

	ctx, cancel := context.WithCancel(ctx)
	c.ctx = ctx
	c.cancel = cancel

	key := c.computeActionKey()
	c.root.mut.Lock()
	c.root.activeConn[key] = c
	c.root.mut.Unlock()

	c.wg.Go(func() {
		defer cancel()
		for {
			select {
			case req := <-c.sendChan:
				err := c.doHandleRequest(ctx, handlerState, requestHandler, req)
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	})

	c.wg.Go(func() {
		defer cancel()
		for {
			select {
			case resp := <-c.recvChan:
				err := c.doHandleResponse(ctx, resp, handlerState, responseHandler)
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	})

	return c
}

func (c *simulateConn[Req, Resp]) doHandleRequest(
	ctx context.Context,
	handlerState *simulationHandlers,
	requestHandler func(req Req) (iter.Seq[Resp], error),
	req Req,
) error {
	if err := c.root.waitOnKey(ctx, c.actionType, phaseHandleRequest, handlerState.current, c.toNode); err != nil {
		return err
	}

	respIter, err := requestHandler(req)
	if err != nil {
		return err
	}

	for resp := range respIter {
		select {
		case c.recvChan <- resp:

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (c *simulateConn[Req, Resp]) doHandleResponse(
	ctx context.Context,
	resp Resp,
	handlerState *simulationHandlers,
	responseHandler func(resp Resp) error,
) error {
	if err := c.root.waitOnKey(ctx, c.actionType, phaseHandleResponse, handlerState.current, c.toNode); err != nil {
		return err
	}
	return responseHandler(resp)
}

func (c *simulateConn[Req, Resp]) WaitBeforeSend(ctx context.Context) error {
	return c.root.waitOnKey(ctx, c.actionType, phaseBeforeRequest, c.fromNode, c.toNode)
}

func (c *simulateConn[Req, Resp]) SendRequest(req Req) {
	c.sendChan <- req
}

func (c *simulateConn[Req, Resp]) Shutdown() {
	c.wg.Wait()

	key := c.computeActionKey()
	c.root.mut.Lock()
	delete(c.root.activeConn, key)
	c.root.mut.Unlock()
}

func (c *simulateConn[Req, Resp]) CloseConn() {
	c.cancel()
}

func (c *simulateConn[Req, Resp]) Print() {
	fmt.Printf(
		"\tActive Conn: %s, %s -> %s\n",
		c.actionType.String(),
		c.fromNode.String()[:6],
		c.toNode.String()[:6],
	)
}

func (c *simulateConn[Req, Resp]) computeActionKey() simulateActionKey {
	return simulateActionKey{
		actionType: c.actionType,
		fromNode:   c.fromNode,
		toNode:     c.toNode,
	}
}

func (c *simulateConn[Req, Resp]) GetContext() context.Context {
	return c.ctx
}
