package paxos_test

import (
	"context"
	"fmt"
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

	mut     sync.Mutex
	waitMap map[simulateActionKey]chan struct{}
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
		nil,
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

type simulationHandlers struct {
	root    *simulationTestCase
	current NodeID
	state   *simulateNodeState
}

func (h *simulationHandlers) stateMachineHandler(ctx context.Context, term TermNum, isLeader bool) error {
	<-ctx.Done()
	return ctx.Err()
}

func (h *simulationHandlers) fetchFollowerHandler(ctx context.Context, id NodeID, term TermNum) error {
	if err := h.root.waitOnKey(ctx, simulateActionFetchFollower, h.current, id); err != nil {
		return err
	}

	info := h.root.nodeMap[id].core.GetChoosingLeaderInfo()

	if err := h.root.waitOnKey(ctx, simulateActionHandleFollowerInfo, h.current, id); err != nil {
		return err
	}

	if err := h.state.core.HandleChoosingLeaderInfo(id, term, info); err != nil {
		return err
	}

	<-ctx.Done()
	return ctx.Err()
}

func (s *simulationTestCase) printAllWaiting() {
	s.mut.Lock()
	fmt.Println("--------------------------------------")
	for key := range s.waitMap {
		fmt.Printf(
			"Wait On: %s, %s -> %s\n",
			key.actionType.String(),
			key.fromNode.String()[:8],
			key.toNode.String()[:8],
		)
	}
	fmt.Println("**********")
	s.mut.Unlock()
}

func (s *simulationTestCase) runAction(
	t *testing.T, actionType simulateActionType, fromNode, toNode NodeID,
) {
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

	if !ok {
		t.Fatalf("Missing wait key: %+v", key)
	}
	close(waitCh)

	synctest.Wait()
}

func TestPaxos__Single_Node(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1},
			[]NodeID{nodeID1},
			defaultSimulationConfig(),
		)

		s.runAction(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runAction(t, simulateActionHandleFollowerInfo, nodeID1, nodeID1)
	})
}
