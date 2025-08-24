package paxos_test

import (
	"context"
	"iter"

	. "github.com/QuangTung97/libpaxos/paxos"
)

type simulateConn[Req, Resp any] struct {
	root     *simulationTestCase
	sendChan chan Req
	recvChan chan Resp
}

func newSimulateConn[Req, Resp any](
	ctx context.Context,
	handlerState *simulationHandlers,
	toNode NodeID,
	requestHandler func(req Req) (iter.Seq[Resp], error),
	requestAction simulateActionType,
	responseHandler func(resp Resp) error,
	responseAction simulateActionType,
) *simulateConn[Req, Resp] {
	c := &simulateConn[Req, Resp]{
		root:     handlerState.root,
		sendChan: make(chan Req, 1),
		recvChan: make(chan Resp, 1),
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer cancel()

		for {
			select {
			case req := <-c.sendChan:
				err := c.doHandleRequest(ctx, handlerState, requestHandler, requestAction, toNode, req)
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
	requestAction simulateActionType,
	toNode NodeID,
	req Req,
) error {
	if err := c.root.waitOnKey(ctx, requestAction, handlerState.current, toNode); err != nil {
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

func (c *simulateConn[Req, Resp]) sendReq(req Req) {

}
