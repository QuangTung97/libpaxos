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
	simulateActionBeforeFetchFollower simulateActionType = iota + 1
)

func (at simulateActionType) String() string {
	switch at {
	case simulateActionBeforeFetchFollower:
		return "before_fetch_follower"
	default:
		return "unknown"
	}
}

type simulateActionKey struct {
	actionType simulateActionType
	id         NodeID
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
		nodeMap[id] = s.initNodeState(id, initNodeSet, initMembersEntry, conf)
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
	id NodeID,
	initNodeSet map[NodeID]struct{},
	initMembersEntry LogEntry,
	conf simulationTestConfig,
) *simulateNodeState {
	persistent := &fake.PersistentStateFake{
		NodeID: id,
		LastTerm: TermNum{
			Num:    20,
			NodeID: id,
		},
	}

	log := &fake.LogStorageFake{}

	_, ok := initNodeSet[id]
	if ok {
		log.UpsertEntries([]PosLogEntry{
			{Pos: 1, Entry: initMembersEntry},
		}, nil)
	}

	acceptor := NewAcceptorLogic(
		id,
		log,
		conf.acceptorLimit,
	)

	runner, finish := s.newRunnerForNode(id)
	state := &simulateNodeState{
		persistent: persistent,
		log:        log,
		acceptor:   acceptor,

		runner:       runner,
		runnerFinish: finish,
	}

	state.core = NewCoreLogic(
		persistent,
		log,
		runner,
		func() TimestampMilli {
			return TimestampMilli(s.now.Load())
		},
		LogPos(conf.maxBufferLen),
	)

	return state
}

func (s *simulationTestCase) newRunnerForNode(id NodeID) (NodeRunner, func()) {
	return NewNodeRunner(
		id,
		nil,
		nil,
		nil,
		s.stateMachineHandler,
		s.fetchFollowerHandler,
		nil,
	)
}
func (s *simulationTestCase) waitOnKey(actionType simulateActionType, id NodeID) {
	key := simulateActionKey{
		actionType: actionType,
		id:         id,
	}

	waitCh := make(chan struct{})

	s.mut.Lock()
	s.waitMap[key] = waitCh
	s.mut.Unlock()

	<-waitCh
}

func (s *simulationTestCase) stateMachineHandler(ctx context.Context, term TermNum, isLeader bool) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *simulationTestCase) fetchFollowerHandler(ctx context.Context, id NodeID, term TermNum) error {
	s.waitOnKey(simulateActionBeforeFetchFollower, id)

	<-ctx.Done()

	return ctx.Err()
}

func (s *simulationTestCase) printAllWaiting() {
	s.mut.Lock()
	fmt.Println("--------------------------------------")
	for key := range s.waitMap {
		fmt.Printf("Wait On: %s, Node: %s\n", key.actionType.String(), key.id.String())
	}
	fmt.Println("**********")
	s.mut.Unlock()
}

func (s *simulationTestCase) runAction(actionType simulateActionType, id NodeID) {
	key := simulateActionKey{
		actionType: actionType,
		id:         id,
	}

	s.mut.Lock()
	waitCh, ok := s.waitMap[key]
	if ok {
		delete(s.waitMap, key)
	}
	s.mut.Unlock()

	if !ok {
		panic(fmt.Sprintf("Missing wait key: %+v", key))
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

		s.printAllWaiting()
		s.runAction(simulateActionBeforeFetchFollower, nodeID1)
		s.printAllWaiting()
	})
}
