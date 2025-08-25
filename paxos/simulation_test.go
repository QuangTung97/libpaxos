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

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type simulateActionType int

const (
	simulateActionFetchFollower simulateActionType = iota + 1
	simulateActionStartElection
	simulateActionVoteRequest
	simulateActionAcceptRequest
	simulateActionStateMachine
)

func (at simulateActionType) String() string {
	switch at {
	case simulateActionFetchFollower:
		return "fetch_follower"
	case simulateActionStartElection:
		return "start_election"
	case simulateActionVoteRequest:
		return "vote_request"
	case simulateActionAcceptRequest:
		return "accept_request"
	case simulateActionStateMachine:
		return "state_machine"
	default:
		return "unknown"
	}
}

type simulateActionKey struct {
	actionType simulateActionType
	isResponse bool
	fromNode   NodeID
	toNode     NodeID
}

type simulationTestCase struct {
	now     atomic.Int64
	nodeMap map[NodeID]*simulateNodeState

	mut             sync.Mutex
	waitMap         map[simulateActionKey]chan struct{}
	shutdownWaitMap map[simulateActionKey]chan struct{}
	activeConn      map[simulateActionKey]SimulationConn
}

type simulateNodeState struct {
	acceptor   AcceptorLogic
	log        *fake.LogStorageFake
	persistent *fake.PersistentStateFake

	runner       NodeRunner
	runnerFinish func()

	core CoreLogic

	cmdChan chan string

	mut             sync.Mutex
	stateMachineLog []PosLogEntry
	stateLastPos    LogPos
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
	s.activeConn = map[simulateActionKey]SimulationConn{}

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
		state := &simulateNodeState{
			cmdChan: make(chan string, 1000),
		}
		nodeMap[id] = state
		s.initNodeState(state, id, initNodeSet, initMembersEntry, conf)
	}

	s.nodeMap = nodeMap

	t.Cleanup(func() {
		for _, state := range s.nodeMap {
			state.runner.StartAcceptRequestRunners(TermNum{}, nil)
			state.runner.StartStateMachine(TermNum{}, StateMachineRunnerInfo{})
		}

		synctest.Wait()

		s.mut.Lock()
		for id, ch := range s.shutdownWaitMap {
			delete(s.shutdownWaitMap, id)
			close(ch)
		}
		s.mut.Unlock()

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
		handlers.voteRequestHandler,
		handlers.acceptRequestHandler,
		handlers.fullyReplicateHandler,
		handlers.stateMachineHandler,
		handlers.fetchFollowerHandler,
		handlers.startElectionHandler,
	)
}
func (s *simulationTestCase) waitOnKey(
	ctx context.Context, actionType simulateActionType,
	isResponse bool,
	fromNode NodeID, toNode NodeID,
) error {
	key := simulateActionKey{
		actionType: actionType,
		isResponse: isResponse,
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
) error {
	for ctx.Err() == nil {
		s.internalWaitOnShutdown(ctx, actionType, fromNode, toNode, fn)
	}
	return ctx.Err()
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
		select {
		case <-ctx.Done():
		case <-newCtx.Done():
			return
		}

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

	if err := fn(newCtx); err != nil {
		cancel()
	}

	wg.Wait()
}

type simulationHandlers struct {
	root    *simulationTestCase
	current NodeID
	state   *simulateNodeState
}

func (h *simulationHandlers) stateMachineHandler(
	ctx context.Context, term TermNum, info StateMachineRunnerInfo,
) error {
	callback := func(ctx context.Context) error {
		var getter StateMachineLogGetter = h.state.acceptor
		if info.IsLeader {
			getter = h.state.core
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			h.stateMachineConsumeEntries(ctx, term, getter)
		})

		if info.AcceptCommand {
			wg.Go(func() {
				for {
					var newCmd string
					select {
					case <-ctx.Done():
						return
					case newCmd = <-h.state.cmdChan:
						if err := h.state.core.InsertCommand(ctx, term, []byte(newCmd)); err != nil {
							return
						}
					}
				}
			})
		}

		wg.Wait()
		return nil
	}

	return h.root.waitOnShutdown(
		ctx, simulateActionStateMachine,
		h.current, h.current, callback,
	)
}

func (h *simulationHandlers) stateMachineConsumeEntries(
	ctx context.Context, term TermNum, getter StateMachineLogGetter,
) {
	h.state.mut.Lock()
	fromPos := h.state.stateLastPos + 1
	h.state.mut.Unlock()

	for {
		output, err := getter.GetCommittedEntriesWithWait(ctx, term, fromPos, 100)
		if err != nil {
			return
		}

		fromPos = output.NextPos

		h.state.mut.Lock()
		h.state.stateMachineLog = append(h.state.stateMachineLog, output.Entries...)
		h.state.stateLastPos = output.NextPos - 1
		h.state.mut.Unlock()
	}
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
			simulateActionFetchFollower,
			func(req struct{}) (iter.Seq[ChooseLeaderInfo], error) {
				info := h.root.nodeMap[toNode].core.GetChoosingLeaderInfo()
				return iterSingle(info), nil
			},
			func(info ChooseLeaderInfo) error {
				return h.state.core.HandleChoosingLeaderInfo(toNode, term, info)
			},
		)
		defer conn.Shutdown()

		conn.SendRequest(struct{}{})
		return nil
	}

	return h.root.waitOnShutdown(ctx, simulateActionFetchFollower, h.current, toNode, callback)
}

func (h *simulationHandlers) startElectionHandler(ctx context.Context, toNode NodeID, termVal TermValue) error {
	return h.root.waitOnShutdown(ctx, simulateActionStartElection, h.current, toNode, func(ctx context.Context) error {
		conn := newSimulateConn(
			ctx, h, toNode,
			simulateActionStartElection,
			func(req struct{}) (iter.Seq[struct{}], error) {
				err := h.root.nodeMap[toNode].core.StartElection(termVal)
				if err != nil {
					return nil, err
				}
				return iterSingle(struct{}{}), nil
			},
			func(resp struct{}) error {
				return nil
			},
		)
		defer conn.Shutdown()

		conn.SendRequest(struct{}{})
		return nil
	})
}

func (h *simulationHandlers) voteRequestHandler(ctx context.Context, toNode NodeID, term TermNum) error {
	return h.root.waitOnShutdown(ctx, simulateActionVoteRequest, h.current, toNode, func(ctx context.Context) error {
		conn := newSimulateConn(
			ctx, h, toNode,
			simulateActionVoteRequest,
			h.root.nodeMap[toNode].acceptor.HandleRequestVote,
			func(resp RequestVoteOutput) error {
				return h.state.core.HandleVoteResponse(ctx, toNode, resp)
			},
		)

		input, err := h.state.core.GetVoteRequest(term, toNode)
		if err != nil {
			return err
		}
		defer conn.Shutdown()

		conn.SendRequest(input)
		return nil
	})
}

func (h *simulationHandlers) acceptRequestHandler(ctx context.Context, toNode NodeID, term TermNum) error {
	return h.root.waitOnShutdown(ctx, simulateActionAcceptRequest, h.current, toNode, func(ctx context.Context) error {
		conn := newSimulateConn(
			ctx, h, toNode,
			simulateActionAcceptRequest,
			func(req AcceptEntriesInput) (iter.Seq[AcceptEntriesOutput], error) {
				output, err := h.root.nodeMap[toNode].acceptor.AcceptEntries(req)
				if err != nil {
					return nil, err
				}
				return iterSingle(output), nil
			},
			func(resp AcceptEntriesOutput) error {
				return h.state.core.HandleAcceptEntriesResponse(toNode, resp)
			},
		)
		defer conn.Shutdown()

		ctx = conn.GetContext()

		var fromPos LogPos
		var lastCommitted LogPos
		for {
			input, err := h.state.core.GetAcceptEntriesRequest(ctx, term, toNode, fromPos, lastCommitted)
			if err != nil {
				return err
			}
			conn.SendRequest(input)

			fromPos = input.NextPos
			lastCommitted = input.Committed
		}
	})
}

func (h *simulationHandlers) fullyReplicateHandler(ctx context.Context, toNode NodeID, term TermNum) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *simulationTestCase) printAllWaiting() {
	_, file, line, _ := runtime.Caller(1)

	s.mut.Lock()
	fmt.Println("--------------------------------------")
	fmt.Printf("%s:%d\n", file, line)

	for key := range s.waitMap {
		isResp := "Request"
		if key.isResponse {
			isResp = "Response"
		}

		fmt.Printf(
			"\tWait On (%s): %s, %s -> %s\n",
			isResp,
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

	for _, conn := range s.activeConn {
		conn.Print()
	}

	fmt.Println("**********")
	s.mut.Unlock()
}

func (s *simulationTestCase) runAction(
	t *testing.T, actionType simulateActionType, isResp bool, fromNode, toNode NodeID,
) {
	t.Helper()

	key := simulateActionKey{
		actionType: actionType,
		isResponse: isResp,
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

func (s *simulationTestCase) runShutdown(
	t *testing.T, actionType simulateActionType, fromNode, toNode NodeID,
) {
	t.Helper()

	key := simulateActionKey{
		actionType: actionType,
		fromNode:   fromNode,
		toNode:     toNode,
	}

	s.mut.Lock()
	waitCh, ok := s.shutdownWaitMap[key]
	if ok {
		delete(s.shutdownWaitMap, key)
	}
	s.mut.Unlock()

	if ok {
		close(waitCh)
	} else {
		t.Fatalf("Missing shutdown wait key: %+v", key)
	}

	synctest.Wait()
}

func (s *simulationTestCase) closeConn(
	t *testing.T, actionType simulateActionType, fromNode, toNode NodeID,
) {
	t.Helper()

	key := simulateActionKey{
		actionType: actionType,
		fromNode:   fromNode,
		toNode:     toNode,
	}

	s.mut.Lock()
	conn, ok := s.activeConn[key]
	if ok {
		delete(s.activeConn, key)
	}
	s.mut.Unlock()

	if ok {
		conn.CloseConn()
	} else {
		t.Fatalf("Missing active connection key: %+v", key)
	}

	synctest.Wait()
}

func (s *simulationTestCase) insertNewCommand(
	_ *testing.T, id NodeID, cmdList ...string,
) {
	for _, cmd := range cmdList {
		s.nodeMap[id].cmdChan <- cmd
	}
	synctest.Wait()
}

func (s *simulationTestCase) newInfLogEntry(cmdStr string) LogEntry {
	return NewCmdLogEntry(InfiniteTerm{}, []byte(cmdStr))
}

func (s *simulationTestCase) newPosLogEntries(
	from LogPos, entries ...LogEntry,
) []PosLogEntry {
	var result []PosLogEntry
	for _, entry := range entries {
		result = append(result, PosLogEntry{
			Pos:   from,
			Entry: entry,
		})
		from++
	}
	return result
}

func TestPaxos__Single_Node(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1},
			[]NodeID{nodeID1},
			defaultSimulationConfig(),
		)

		s.runAction(t, simulateActionFetchFollower, false, nodeID1, nodeID1)
		s.runAction(t, simulateActionFetchFollower, true, nodeID1, nodeID1)

		s.runAction(t, simulateActionStartElection, false, nodeID1, nodeID1)
		s.runAction(t, simulateActionStartElection, true, nodeID1, nodeID1)

		s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionStartElection, nodeID1, nodeID1)

		s.runAction(t, simulateActionVoteRequest, false, nodeID1, nodeID1)
		s.runAction(t, simulateActionVoteRequest, true, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID1)

		assert.Equal(t, StateLeader, s.nodeMap[nodeID1].core.GetState())
		assert.Equal(t, TermNum{Num: 21, NodeID: nodeID1}, s.nodeMap[nodeID1].persistent.GetLastTerm())

		s.runAction(t, simulateActionAcceptRequest, false, nodeID1, nodeID1)
		s.runAction(t, simulateActionAcceptRequest, true, nodeID1, nodeID1)

		// check log entries
		members := []MemberInfo{
			{Nodes: []NodeID{nodeID1}, CreatedAt: 1},
		}
		assert.Equal(t, []PosLogEntry{
			{Pos: 1, Entry: NewMembershipLogEntry(InfiniteTerm{}, members)},
		}, s.nodeMap[nodeID1].stateMachineLog)
		assert.Equal(t, LogPos(1), s.nodeMap[nodeID1].stateLastPos)

		s.runShutdown(t, simulateActionStateMachine, nodeID1, nodeID1)

		s.insertNewCommand(t, nodeID1,
			"new cmd 02",
			"new cmd 03",
		)

		s.runAction(t, simulateActionAcceptRequest, false, nodeID1, nodeID1)
		s.runAction(t, simulateActionAcceptRequest, true, nodeID1, nodeID1)

		// check log entries again
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
			s.newInfLogEntry("new cmd 02"),
			s.newInfLogEntry("new cmd 03"),
		), s.nodeMap[nodeID1].stateMachineLog)
		assert.Equal(t, LogPos(3), s.nodeMap[nodeID1].stateLastPos)

		s.printAllWaiting()
	})
}
