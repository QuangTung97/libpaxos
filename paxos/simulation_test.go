package paxos_test

import (
	"context"
	"fmt"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type simulateActionType int

const (
	simulateActionFetchFollower simulateActionType = iota + 1
	simulateActionHandleFollowerInfo
	simulateActionStartElection
	simulateActionHandleStartElection
)

func (at simulateActionType) String() string {
	switch at {
	case simulateActionFetchFollower:
		return "fetch_follower"
	case simulateActionHandleFollowerInfo:
		return "handle_follower"
	default:
		return "unknown"
	}
}

type simulateActionKey struct {
	actionType simulateActionType
	fromNode   NodeID
	toNode     NodeID
}

type simulationTestCase struct {
	now     atomic.Int64
	nodeMap map[NodeID]*simulateNodeState

	mut             sync.Mutex
	waitMap         map[simulateActionKey]chan struct{}
	shutdownWaitMap map[simulateActionKey]chan struct{}
}

type simulateNodeState struct {
	acceptor   AcceptorLogic
	log        *fake.LogStorageFake
	persistent *fake.PersistentStateFake

	runner       NodeRunner
	runnerFinish func()

	core CoreLogic
}

type simulationTestConfig struct {
	maxBufferLen  int
	acceptorLimit int
}

func defaultSimulationConfig() simulationTestConfig {
	return simulationTestConfig{
		maxBufferLen:  5,
		acceptorLimit: 3,
	}
}

func newSimulationTestCase(
	t *testing.T,
	allNodes []NodeID,
	initNodes []NodeID,
	conf simulationTestConfig,
) *simulationTestCase {
	s := &simulationTestCase{}
	s.now.Store(10_000)

	s.waitMap = map[simulateActionKey]chan struct{}{}
	s.shutdownWaitMap = map[simulateActionKey]chan struct{}{}

	initNodeSet := map[NodeID]struct{}{}
	for _, id := range initNodes {
		initNodeSet[id] = struct{}{}
	}

	initMembersEntry := NewMembershipLogEntry(
		InfiniteTerm{},
		[]MemberInfo{
			{CreatedAt: 1, Nodes: initNodes},
		},
	)

	nodeMap := map[NodeID]*simulateNodeState{}
	for _, id := range allNodes {
		state := &simulateNodeState{}
		nodeMap[id] = state
		s.initNodeState(state, id, initNodeSet, initMembersEntry, conf)
	}

	s.nodeMap = nodeMap

	t.Cleanup(func() {
		s.mut.Lock()
		for id, ch := range s.shutdownWaitMap {
			delete(s.shutdownWaitMap, id)
			close(ch)
		}
		s.mut.Unlock()

		fmt.Println("FINISH CLEANUP")
		for _, state := range s.nodeMap {
			state.runnerFinish()
		}
	})

	synctest.Wait()
	return s
}

func (s *simulationTestCase) initNodeState(
	state *simulateNodeState,
	id NodeID,
	initNodeSet map[NodeID]struct{},
	initMembersEntry LogEntry,
	conf simulationTestConfig,
) {
	state.persistent = &fake.PersistentStateFake{
		NodeID: id,
		LastTerm: TermNum{
			Num:    20,
			NodeID: id,
		},
	}

	state.log = &fake.LogStorageFake{}

	if _, ok := initNodeSet[id]; ok {
		state.log.UpsertEntries([]PosLogEntry{
			{Pos: 1, Entry: initMembersEntry},
		}, nil)
	}

	state.acceptor = NewAcceptorLogic(
		id,
		state.log,
		conf.acceptorLimit,
	)

	state.runner, state.runnerFinish = s.newRunnerForNode(state, id)

	state.core = NewCoreLogic(
		state.persistent,
		state.log,
		state.runner,
		func() TimestampMilli {
			return TimestampMilli(s.now.Load())
		},
		LogPos(conf.maxBufferLen),
	)
}

func (s *simulationTestCase) newRunnerForNode(state *simulateNodeState, id NodeID) (NodeRunner, func()) {
	handlers := &simulationHandlers{
		root:    s,
		current: id,
		state:   state,
	}

	return NewNodeRunner(
		id,
		nil,
		nil,
		nil,
		handlers.stateMachineHandler,
		handlers.fetchFollowerHandler,
		handlers.startElectionHandler,
	)
}
func (s *simulationTestCase) waitOnKey(
	ctx context.Context, actionType simulateActionType,
	fromNode NodeID, toNode NodeID,
) error {
	key := simulateActionKey{
		actionType: actionType,
		fromNode:   fromNode,
		toNode:     toNode,
	}

	waitCh := make(chan struct{})

	s.mut.Lock()
	s.waitMap[key] = waitCh
	s.mut.Unlock()

	select {
	case <-waitCh:
		return nil

	case <-ctx.Done():
		// TODO remove from waitMap
		return ctx.Err()
	}
}

func (s *simulationTestCase) waitOnShutdown(
	ctx context.Context,
	actionType simulateActionType,
	fromNode NodeID, toNode NodeID,
	fn func(ctx context.Context) error,
) {
	for ctx.Err() == nil {
		s.internalWaitOnShutdown(ctx, actionType, fromNode, toNode, fn)
	}
}

func (s *simulationTestCase) internalWaitOnShutdown(
	ctx context.Context,
	actionType simulateActionType,
	fromNode NodeID, toNode NodeID,
	fn func(ctx context.Context) error,
) {
	newCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		<-ctx.Done()
		defer cancel()

		key := simulateActionKey{
			actionType: actionType,
			fromNode:   fromNode,
			toNode:     toNode,
		}

		ch := make(chan struct{})

		s.mut.Lock()
		s.shutdownWaitMap[key] = ch
		s.mut.Unlock()

		<-ch
	})

	_ = fn(newCtx)
	wg.Wait()
}

type simulationHandlers struct {
	root    *simulationTestCase
	current NodeID
	state   *simulateNodeState
}

func (h *simulationHandlers) stateMachineHandler(ctx context.Context, term TermNum, isLeader bool) error {
	<-ctx.Done()
	return ctx.Err()
}

func iterSingle[T any](value T) iter.Seq[T] {
	return func(yield func(T) bool) {
		yield(value)
	}
}

func (h *simulationHandlers) fetchFollowerHandler(ctx context.Context, toNode NodeID, term TermNum) error {
	callback := func(ctx context.Context) error {
		conn := newSimulateConn(
			ctx, h, toNode,
			func(req struct{}) (iter.Seq[ChooseLeaderInfo], error) {
				info := h.root.nodeMap[toNode].core.GetChoosingLeaderInfo()
				return iterSingle(info), nil
			},
			simulateActionFetchFollower,
			func(info ChooseLeaderInfo) error {
				return h.state.core.HandleChoosingLeaderInfo(toNode, term, info)
			},
			simulateActionHandleFollowerInfo,
		)

		conn.sendReq(struct{}{})

		conn.shutdown()
		return nil
	}

	h.root.waitOnShutdown(ctx, simulateActionFetchFollower, h.current, toNode, callback)
	return nil
}

func (h *simulationHandlers) startElectionHandler(ctx context.Context, toNode NodeID, termVal TermValue) error {
	h.root.waitOnShutdown(ctx, simulateActionStartElection, h.current, toNode, func(ctx context.Context) error {
		conn := newSimulateConn(
			ctx, h, toNode,
			func(req struct{}) (iter.Seq[struct{}], error) {
				err := h.root.nodeMap[toNode].core.StartElection(termVal)
				if err != nil {
					return nil, err
				}
				return iterSingle(struct{}{}), nil
			},
			simulateActionStartElection,
			func(resp struct{}) error {
				return nil
			},
			simulateActionHandleStartElection,
		)

		conn.sendReq(struct{}{})

		conn.shutdown()
		return nil
	})
	return nil
}

func (s *simulationTestCase) printAllWaiting() {
	_, file, line, _ := runtime.Caller(1)

	s.mut.Lock()
	fmt.Println("--------------------------------------")
	fmt.Printf("%s:%d\n", file, line)
	for key := range s.waitMap {
		fmt.Printf(
			"\tWait On: %s, %s -> %s\n",
			key.actionType.String(),
			key.fromNode.String()[:6],
			key.toNode.String()[:6],
		)
	}
	for key := range s.shutdownWaitMap {
		fmt.Printf(
			"\tWait Shutdown On: %s, %s -> %s\n",
			key.actionType.String(),
			key.fromNode.String()[:6],
			key.toNode.String()[:6],
		)
	}
	fmt.Println("**********")
	s.mut.Unlock()
}

func (s *simulationTestCase) runAction(
	t *testing.T, actionType simulateActionType, fromNode, toNode NodeID,
) {
	t.Helper()

	key := simulateActionKey{
		actionType: actionType,
		fromNode:   fromNode,
		toNode:     toNode,
	}

	s.mut.Lock()
	waitCh, ok := s.waitMap[key]
	if ok {
		delete(s.waitMap, key)
	}
	s.mut.Unlock()

	if ok {
		close(waitCh)
	} else {
		t.Fatalf("Missing wait key: %+v", key)
	}

	synctest.Wait()
}

func TestPaxos__Single_Node(t *testing.T) {
	t.Skip()

	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1},
			[]NodeID{nodeID1},
			defaultSimulationConfig(),
		)

		s.runAction(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runAction(t, simulateActionHandleFollowerInfo, nodeID1, nodeID1)
		s.printAllWaiting()
		//s.runAction(t, simulateActionHandleFollowerInfo, nodeID1, nodeID1)
	})
}
