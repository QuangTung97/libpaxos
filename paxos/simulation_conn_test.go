package paxos_test

import (
	"context"
	"iter"
	"sync"

	. "github.com/QuangTung97/libpaxos/paxos"
)

type simulateConn[Req, Resp any] struct {
	root     *simulationTestCase
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
		root:     handlerState.root,
		sendChan: make(chan Req, 1),
		recvChan: make(chan Resp, 1),
	}

	ctx, cancel := context.WithCancel(ctx)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()

		for {
			select {
			case req := <-c.sendChan:
				err := c.doHandleRequest(ctx, handlerState, requestHandler, actionType, toNode, req)
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()

		for {
			select {
			case resp := <-c.recvChan:
				err := c.doHandleResponse(ctx, resp, handlerState, responseHandler, actionType, toNode)
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return c
}

func (c *simulateConn[Req, Resp]) doHandleRequest(
	ctx context.Context,
	handlerState *simulationHandlers,
	requestHandler func(req Req) (iter.Seq[Resp], error),
	actionType simulateActionType,
	toNode NodeID,
	req Req,
) error {
	if err := c.root.waitOnKey(ctx, actionType, false, handlerState.current, toNode); err != nil {
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
	actionType simulateActionType,
	toNode NodeID,
) error {
	if err := c.root.waitOnKey(ctx, actionType, true, handlerState.current, toNode); err != nil {
		return err
	}
	return responseHandler(resp)
}

func (c *simulateConn[Req, Resp]) sendReq(req Req) {
	c.sendChan <- req
}

func (c *simulateConn[Req, Resp]) shutdown() {
	c.wg.Wait()
}
