package paxos_test

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
	"github.com/QuangTung97/libpaxos/paxos/waiting"
)

type simulateActionType int

const (
	simulateActionFetchFollower simulateActionType = iota + 1
	simulateActionStartElection
	simulateActionVoteRequest
	simulateActionAcceptRequest
	simulateActionStateMachine
	simulateActionFullyReplicate
	simulateActionReplicateAcceptRequest
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
	case simulateActionFullyReplicate:
		return "fully_replicate"
	case simulateActionReplicateAcceptRequest:
		return "replicate_accept"
	default:
		return "unknown"
	}
}

type phaseType int

const (
	phaseBeforeRequest phaseType = iota + 1
	phaseHandleRequest
	phaseHandleResponse
)

func getAllPhases() []phaseType {
	return []phaseType{
		phaseBeforeRequest,
		phaseHandleRequest,
		phaseHandleResponse,
	}
}

func (t phaseType) String() string {
	switch t {
	case phaseBeforeRequest:
		return "BeforeReq"
	case phaseHandleRequest:
		return "Request"
	case phaseHandleResponse:
		return "Response"
	default:
		return "Unknown"
	}
}

type simulateActionKey struct {
	actionType simulateActionType
	phase      phaseType
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
			state.core.DisableAlwaysCheckInv()
			state.runner.StartAcceptRequestRunners(TermNum{}, nil)
			state.runner.StartStateMachine(TermNum{}, StateMachineRunnerInfo{})
		}

		synctest.Wait()

		// shutdown all
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
		true,
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
	phase phaseType, fromNode NodeID, toNode NodeID,
) error {
	key := simulateActionKey{
		actionType: actionType,
		phase:      phase,
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
		s.mut.Lock()
		delete(s.waitMap, key)
		s.mut.Unlock()
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

	wg := waiting.NewWaitGroup()
	wg.Go(func() {
		defer cancel()

		select {
		case <-ctx.Done():
		case <-newCtx.Done():
			return
		}

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

	checkIsAssociated(wg)
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
		ctx, cancel := context.WithCancel(ctx)

		var getter StateMachineLogGetter = h.state.acceptor
		if info.IsLeader {
			getter = h.state.core
		}

		wg := waiting.NewWaitGroup()
		wg.Go(func() {
			defer cancel()

			h.stateMachineConsumeEntries(ctx, term, getter)
		})

		if info.AcceptCommand {
			wg.Go(func() {
				defer cancel()

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

		checkIsAssociated(wg)
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
			func(req RequestVoteInput) (iter.Seq[RequestVoteOutput], error) {
				toState := h.root.nodeMap[toNode]
				toState.core.FollowerReceiveTermNum(req.Term)
				return toState.acceptor.HandleRequestVote(req)
			},
			func(resp RequestVoteOutput) error {
				return h.state.core.HandleVoteResponse(ctx, toNode, resp)
			},
		)
		defer conn.Shutdown()

		if err := conn.WaitBeforeSend(ctx); err != nil {
			return err
		}

		input, err := h.state.core.GetVoteRequest(term, toNode)
		if err != nil {
			return err
		}

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
				return h.handleAcceptEntriesRequest(req, toNode)
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
			if err := conn.WaitBeforeSend(ctx); err != nil {
				return err
			}

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

func (h *simulationHandlers) handleAcceptEntriesRequest(
	req AcceptEntriesInput, toNode NodeID,
) (iter.Seq[AcceptEntriesOutput], error) {
	toState := h.root.nodeMap[toNode]
	toState.core.FollowerReceiveTermNum(req.Term)

	output, err := toState.acceptor.AcceptEntries(req)
	if err != nil {
		return nil, err
	}
	return iterSingle(output), nil
}

func (h *simulationHandlers) fullyReplicateHandler(ctx context.Context, toNode NodeID, term TermNum) error {
	callback := func(ctx context.Context) error {
		acceptConn := newSimulateConn(
			ctx, h, toNode,
			simulateActionReplicateAcceptRequest,
			func(req AcceptEntriesInput) (iter.Seq[AcceptEntriesOutput], error) {
				return h.handleAcceptEntriesRequest(req, toNode)
			},
			func(resp AcceptEntriesOutput) error {
				// Do nothing
				return nil
			},
		)
		defer acceptConn.Shutdown()

		handleReqFunc := func(req struct{}) (iter.Seq[NeedReplicatedInput], error) {
			toState := h.root.nodeMap[toNode]

			return func(yield func(NeedReplicatedInput) bool) {
				var fromPos LogPos
				var lastReplicated LogPos

				for {
					input, err := toState.acceptor.GetNeedReplicatedPos(ctx, term, fromPos, lastReplicated)
					if err != nil {
						return
					}

					if !yield(input) {
						return
					}

					fromPos = input.NextPos
					lastReplicated = input.FullyReplicated
				}
			}, nil
		}

		conn := newSimulateConn(
			ctx, h, toNode,
			simulateActionFullyReplicate,
			handleReqFunc,
			func(resp NeedReplicatedInput) error {
				acceptInput, err := h.state.core.GetNeedReplicatedLogEntries(resp)
				if err != nil {
					return err
				}

				if len(acceptInput.Entries) > 0 {
					acceptConn.SendRequest(acceptInput)
				}
				return nil
			},
		)
		defer conn.Shutdown()

		conn.SendRequest(struct{}{})
		return nil
	}
	return h.root.waitOnShutdown(ctx, simulateActionFullyReplicate, h.current, toNode, callback)
}

func (s *simulationTestCase) printAllWaiting() {
	_, file, line, _ := runtime.Caller(1)

	s.mut.Lock()
	fmt.Println("--------------------------------------")
	fmt.Printf("%s:%d\n", file, line)

	for _, key := range getSortWaitKeys(s.waitMap) {
		fmt.Printf(
			"\tWait On (%s): %s, %s -> %s\n",
			key.phase.String(),
			key.actionType.String(),
			key.fromNode.String()[:6],
			key.toNode.String()[:6],
		)
	}

	for _, key := range getSortWaitKeys(s.shutdownWaitMap) {
		fmt.Printf(
			"\tWait Shutdown On: %s, %s -> %s\n",
			key.actionType.String(),
			key.fromNode.String()[:6],
			key.toNode.String()[:6],
		)
	}

	for _, key := range getSortWaitKeys(s.activeConn) {
		conn := s.activeConn[key]
		conn.Print()
	}

	fmt.Println("**********")
	s.mut.Unlock()
}

func (s *simulationTestCase) runAction(
	t *testing.T, actionType simulateActionType, phase phaseType, fromNode, toNode NodeID,
) {
	t.Helper()

	key := simulateActionKey{
		actionType: actionType,
		phase:      phase,
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

func (s *simulationTestCase) runFullPhases(
	t *testing.T, actionType simulateActionType, fromNode, toNode NodeID,
) {
	t.Helper()

	loopCount := 0
	for {
		loopCount++
		runOK := false

		for _, phase := range getAllPhases() {
			key := simulateActionKey{
				actionType: actionType,
				phase:      phase,
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
				runOK = true
			}
			synctest.Wait()
		}

		if !runOK {
			if loopCount <= 1 {
				key := simulateActionKey{
					actionType: actionType,
					fromNode:   fromNode,
					toNode:     toNode,
				}
				t.Fatalf("Missing wait key of form: %+v", key)
			}
			break
		}
	}
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

func getSortWaitKeys[V any](inputMap map[simulateActionKey]V) []simulateActionKey {
	keys := make([]simulateActionKey, 0, len(inputMap))
	for k := range inputMap {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, compareActionKey)
	return keys
}

func compareActionKey(a, b simulateActionKey) int {
	if a.actionType != b.actionType {
		return cmp.Compare(a.actionType, b.actionType)
	}

	if a.phase != b.phase {
		return cmp.Compare(a.phase, b.phase)
	}

	if a.fromNode != b.fromNode {
		return slices.Compare(a.fromNode[:], b.fromNode[:])
	}

	return slices.Compare(a.toNode[:], b.toNode[:])
}

func TestPaxos__Single_Node(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1},
			[]NodeID{nodeID1},
			defaultSimulationConfig(),
		)

		s.runFullPhases(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionStartElection, nodeID1, nodeID1)

		s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionStartElection, nodeID1, nodeID1)

		s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID1)

		assert.Equal(t, StateLeader, s.nodeMap[nodeID1].core.GetState())
		assert.Equal(t, TermNum{Num: 21, NodeID: nodeID1}, s.nodeMap[nodeID1].persistent.GetLastTerm())

		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)

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

		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)

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

func TestPaxos__Normal_Three_Nodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1, nodeID2, nodeID3},
			[]NodeID{nodeID1, nodeID2, nodeID3},
			defaultSimulationConfig(),
		)

		s.runFullPhases(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionFetchFollower, nodeID1, nodeID2)
		s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID2)
		s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID3)

		s.runFullPhases(t, simulateActionStartElection, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionStartElection, nodeID1, nodeID1)

		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID2)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID3)

		s.runShutdown(t, simulateActionFetchFollower, nodeID2, nodeID1)
		s.runShutdown(t, simulateActionFetchFollower, nodeID2, nodeID2)
		s.runShutdown(t, simulateActionFetchFollower, nodeID2, nodeID3)

		s.runShutdown(t, simulateActionFetchFollower, nodeID3, nodeID1)
		s.runShutdown(t, simulateActionFetchFollower, nodeID3, nodeID2)
		s.runShutdown(t, simulateActionFetchFollower, nodeID3, nodeID3)

		// vote requests
		s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID2)

		// shutdown voters
		s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID2)
		s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID3)

		// rerun state machine
		s.runShutdown(t, simulateActionStateMachine, nodeID1, nodeID1)

		// check logs
		members := []MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		}

		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
		), s.nodeMap[nodeID1].stateMachineLog)

		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
		), s.nodeMap[nodeID3].stateMachineLog)

		// insert commands
		s.insertNewCommand(t,
			nodeID1,
			"cmd test 02",
			"cmd test 03",
		)
		assert.Equal(t, LogPos(1), s.nodeMap[nodeID1].core.GetLastCommitted())

		// send accept to majority
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID2)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1) // because last committed is increased
		assert.Equal(t, LogPos(3), s.nodeMap[nodeID1].core.GetLastCommitted())

		// check logs of leader
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
			s.newInfLogEntry("cmd test 02"),
			s.newInfLogEntry("cmd test 03"),
		), s.nodeMap[nodeID1].stateMachineLog)

		// check logs of node 2
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
		), s.nodeMap[nodeID2].stateMachineLog)
		assert.Equal(t, LogPos(3), s.nodeMap[nodeID2].log.GetFullyReplicated())

		s.runShutdown(t, simulateActionStateMachine, nodeID2, nodeID2)

		// check logs after fully replicated to node 2
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
			s.newInfLogEntry("cmd test 02"),
			s.newInfLogEntry("cmd test 03"),
		), s.nodeMap[nodeID2].stateMachineLog)

		// restart state machine of node 3
		s.runShutdown(t, simulateActionStateMachine, nodeID3, nodeID3)

		// clear existing conn state
		s.closeConn(t, simulateActionAcceptRequest, nodeID1, nodeID3)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID3)

		// check logs BEFORE fully replicated to node 3
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
		), s.nodeMap[nodeID3].stateMachineLog)
		assert.Equal(t, LogPos(1), s.nodeMap[nodeID3].log.GetFullyReplicated())
		assert.Equal(t, LogPos(3), s.nodeMap[nodeID3].acceptor.GetLastCommitted())

		// fully replicate
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID3)
		s.runFullPhases(t, simulateActionReplicateAcceptRequest, nodeID1, nodeID3)

		// check logs AFTER fully replicated to node 3
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members),
			s.newInfLogEntry("cmd test 02"),
			s.newInfLogEntry("cmd test 03"),
		), s.nodeMap[nodeID3].stateMachineLog)
		assert.Equal(t, LogPos(3), s.nodeMap[nodeID3].log.GetFullyReplicated())
		assert.Equal(t, LogPos(3), s.nodeMap[nodeID3].acceptor.GetLastCommitted())

		s.runAction(t, simulateActionFullyReplicate, phaseHandleResponse, nodeID1, nodeID3)

		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID2)

		s.printAllWaiting()
	})
}

func TestPaxos__Single_Node__Change_To_3_Nodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1, nodeID2, nodeID3},
			[]NodeID{nodeID1},
			defaultSimulationConfig(),
		)

		s.runFullPhases(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionStartElection, nodeID1, nodeID1)

		s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionStartElection, nodeID1, nodeID1)

		s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID1)
		s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID1)

		s.runShutdown(t, simulateActionStateMachine, nodeID1, nodeID1)

		// change membership
		leader := s.nodeMap[nodeID1]
		err := leader.core.ChangeMembership(
			context.Background(),
			leader.persistent.GetLastTerm(),
			[]NodeID{nodeID1, nodeID2, nodeID3},
		)
		assert.Equal(t, nil, err)
		synctest.Wait()

		// accept membership entry
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)
		assert.Equal(t, LogPos(1), leader.core.GetLastCommitted())

		// accept membership entry
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID2)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)
		assert.Equal(t, LogPos(2), leader.core.GetLastCommitted())
		assert.Equal(t, LogPos(2), s.nodeMap[nodeID1].log.GetFullyReplicated())

		assert.Equal(t, LogPos(0), s.nodeMap[nodeID2].log.GetFullyReplicated())
		assert.Equal(t, LogPos(2), s.nodeMap[nodeID2].acceptor.GetLastCommitted())

		s.runShutdown(t, simulateActionStateMachine, nodeID2, nodeID2)

		// check logs of node 1
		members1 := []MemberInfo{
			{Nodes: []NodeID{nodeID1}, CreatedAt: 1},
		}
		members2 := []MemberInfo{
			{Nodes: []NodeID{nodeID1}, CreatedAt: 1},
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 2},
		}
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members1),
			NewMembershipLogEntry(InfiniteTerm{}, members2),
		), s.nodeMap[nodeID1].stateMachineLog)

		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID2)
		s.runFullPhases(t, simulateActionReplicateAcceptRequest, nodeID1, nodeID2)
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID2)

		// replicate finish membership change
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID2)
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID1)
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID2)

		// check logs of node 1
		members3 := []MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		}
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members1),
			NewMembershipLogEntry(InfiniteTerm{}, members2),
			NewMembershipLogEntry(InfiniteTerm{}, members3),
		), s.nodeMap[nodeID1].stateMachineLog)

		// check logs of node 2
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members1),
			NewMembershipLogEntry(InfiniteTerm{}, members2),
			NewMembershipLogEntry(InfiniteTerm{}, members3),
		), s.nodeMap[nodeID2].stateMachineLog)

		// check logs of node 3
		assert.Equal(t, s.newPosLogEntries(1), s.nodeMap[nodeID3].stateMachineLog)

		// replicate all to node 3
		s.runFullPhases(t, simulateActionAcceptRequest, nodeID1, nodeID3)
		s.runShutdown(t, simulateActionStateMachine, nodeID3, nodeID3)
		s.runFullPhases(t, simulateActionFullyReplicate, nodeID1, nodeID3)
		s.runFullPhases(t, simulateActionReplicateAcceptRequest, nodeID1, nodeID3)
		s.runAction(t, simulateActionFullyReplicate, phaseHandleResponse, nodeID1, nodeID3)

		// check logs of node 3 again
		assert.Equal(t, s.newPosLogEntries(1,
			NewMembershipLogEntry(InfiniteTerm{}, members1),
			NewMembershipLogEntry(InfiniteTerm{}, members2),
			NewMembershipLogEntry(InfiniteTerm{}, members3),
		), s.nodeMap[nodeID3].stateMachineLog)

		s.printAllWaiting()
	})
}

func (s *simulationTestCase) setupLeaderForThreeNodes(t *testing.T) {
	s.runFullPhases(t, simulateActionFetchFollower, nodeID1, nodeID1)
	s.runFullPhases(t, simulateActionFetchFollower, nodeID1, nodeID2)
	s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID1)
	s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID2)
	s.runShutdown(t, simulateActionFetchFollower, nodeID1, nodeID3)

	s.runFullPhases(t, simulateActionStartElection, nodeID1, nodeID1)
	s.runShutdown(t, simulateActionStartElection, nodeID1, nodeID1)

	s.runAction(t, simulateActionVoteRequest, phaseBeforeRequest, nodeID1, nodeID1)
	s.runAction(t, simulateActionVoteRequest, phaseBeforeRequest, nodeID1, nodeID2)
	s.runAction(t, simulateActionVoteRequest, phaseBeforeRequest, nodeID1, nodeID3)

	s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID1)
	s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID2)
	s.runFullPhases(t, simulateActionVoteRequest, nodeID1, nodeID3)

	s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID1)
	s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID2)
	s.runShutdown(t, simulateActionVoteRequest, nodeID1, nodeID3)

	s.runShutdown(t, simulateActionFetchFollower, nodeID2, nodeID1)
	s.runShutdown(t, simulateActionFetchFollower, nodeID2, nodeID2)
	s.runShutdown(t, simulateActionFetchFollower, nodeID2, nodeID3)

	s.runShutdown(t, simulateActionFetchFollower, nodeID3, nodeID1)
	s.runShutdown(t, simulateActionFetchFollower, nodeID3, nodeID2)
	s.runShutdown(t, simulateActionFetchFollower, nodeID3, nodeID3)

	s.runShutdown(t, simulateActionStateMachine, nodeID1, nodeID1)
	s.runShutdown(t, simulateActionStateMachine, nodeID2, nodeID2)
	s.runShutdown(t, simulateActionStateMachine, nodeID3, nodeID3)
}

func TestPaxos__Normal_Three_Nodes__Insert_Many_Commands(t *testing.T) {
	executeRandomAction := func(s *simulationTestCase, randObj *rand.Rand, nextCmd *int) {
		s.mut.Lock()

		execAction := func() {
			key, ok := getRandomActionKey(randObj, s.waitMap)
			if ok {
				waitCh := s.waitMap[key]
				delete(s.waitMap, key)
				close(waitCh)
			}
		}

		cmdWeight := 1
		if *nextCmd >= 20 {
			cmdWeight = 0
		}

		runRandomAction(
			randObj,
			randomActionWeight(len(s.waitMap), execAction),
			randomActionWeight(
				cmdWeight,
				func() {
					*nextCmd++
					s.nodeMap[nodeID1].cmdChan <- fmt.Sprintf("new command: %d", nextCmd)
				},
			),
		)

		s.mut.Unlock()

		synctest.Wait()
	}

	synctest.Test(t, func(t *testing.T) {
		s := newSimulationTestCase(
			t,
			[]NodeID{nodeID1, nodeID2, nodeID3},
			[]NodeID{nodeID1, nodeID2, nodeID3},
			defaultSimulationConfig(),
		)

		s.setupLeaderForThreeNodes(t)

		randObj := newRandomObject()
		nextCmd := 0

		for range 1000 {
			executeRandomAction(s, randObj, &nextCmd)
		}

		assert.Equal(t, LogPos(21), s.nodeMap[nodeID1].log.GetCommittedInfo().FullyReplicated)
		assert.Equal(t, LogPos(21), s.nodeMap[nodeID2].log.GetCommittedInfo().FullyReplicated)
		assert.Equal(t, LogPos(21), s.nodeMap[nodeID3].log.GetCommittedInfo().FullyReplicated)
		assert.Equal(t, 20, nextCmd)

		s.printAllWaiting()
	})
}

func ref(any) {}
