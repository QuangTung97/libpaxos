package paxos

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
)

type CoreLogic interface {
	StateMachineLogGetter

	StartElection(maxTermValue TermValue) error

	GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, error)
	HandleVoteResponse(ctx context.Context, fromNode NodeID, output RequestVoteOutput) error

	GetAcceptEntriesRequest(
		ctx context.Context, term TermNum, toNode NodeID,
		fromPos LogPos, lastCommittedSent LogPos,
	) (AcceptEntriesInput, error)

	FollowerReceiveTermNum(term TermNum) bool

	HandleAcceptEntriesResponse(fromNode NodeID, output AcceptEntriesOutput) error

	InsertCommand(ctx context.Context, term TermNum, cmdDataList ...[]byte) error

	CheckTimeout()

	ChangeMembership(ctx context.Context, term TermNum, newNodes []NodeID) error

	GetNeedReplicatedLogEntries(input NeedReplicatedInput) (AcceptEntriesInput, error)

	GetChoosingLeaderInfo() ChooseLeaderInfo

	HandleChoosingLeaderInfo(fromNode NodeID, term TermNum, info ChooseLeaderInfo) error

	// -------------------------------------------------------
	// Testing Utility Functions
	// -------------------------------------------------------

	GetState() State
	GetLastCommitted() LogPos
	GetReplicatedPosTest(id NodeID) LogPos
	GetMinBufferLogPos() LogPos
	GetMaxLogPos() LogPos
	GetFollowerWakeUpAt() TimestampMilli

	// CheckInvariant for testing only
	CheckInvariant()

	DisableAlwaysCheckInv()
}

func NewCoreLogic(
	persistent PersistentState,
	log LeaderLogGetter,
	runner NodeRunner,
	nowFunc func() TimestampMilli,
	maxBufferLen LogPos,
	withCheckInv bool,
	timeoutTickDuration TimestampMilli,
	tickRandomJitter TimestampMilli,
) CoreLogic {
	c := &coreLogicImpl{
		nowFunc:      nowFunc,
		maxBufferLen: maxBufferLen,

		state: StateFollower,

		alwaysCheckInv: withCheckInv,

		persistent: persistent,
		log:        log,
		runner:     runner,

		timeoutTickDuration: timeoutTickDuration,
		tickRandomJitter:    tickRandomJitter,
	}

	c.updateFollowerCheckOtherStatus(false, false)
	c.updateAllRunners()

	return c
}

type coreLogicImpl struct {
	nowFunc      func() TimestampMilli
	maxBufferLen LogPos

	mut   sync.Mutex
	state State

	alwaysCheckInv bool

	follower  *followerStateInfo
	candidate *candidateStateInfo
	leader    *leaderStateInfo

	followerRetryCount int

	persistent PersistentState
	log        LeaderLogGetter
	runner     NodeRunner

	timeoutTickDuration TimestampMilli
	tickRandomJitter    TimestampMilli
}

type followerStateInfo struct {
	wakeUpAt         TimestampMilli
	checkStatus      followerCheckOtherStatus
	fastSwitchLeader bool

	members     []MemberInfo
	lastTermVal TermValue
	lastMaxPos  LogPos

	lastNodePos       map[NodeID]LogPos
	noActiveLeaderSet map[NodeID]struct{}
}

type followerCheckOtherStatus int

const (
	followerCheckOtherStatusRunning followerCheckOtherStatus = iota
	followerCheckOtherStatusLeaderIsActive
	followerCheckOtherStatusStartingNewElection
)

type candidateStateInfo struct {
	remainPosMap map[NodeID]InfiniteLogPos
	acceptPos    LogPos
}

type leaderStateInfo struct {
	members          []MemberInfo
	leaderStepDownAt NullLogPos

	lastCommitted LogPos

	memLog *MemLog

	acceptorWakeUpAt map[NodeID]TimestampMilli
	sendAcceptCond   *NodeCond

	acceptorFullyReplicated map[NodeID]LogPos

	logBuffer     *LogBuffer
	bufferMaxCond *NodeCond
}

func (c *coreLogicImpl) generateNextProposeTerm(maxTermValue TermValue) {
	lastTerm := c.persistent.GetLastTerm()
	newTerm := TermNum{
		Num:    max(maxTermValue, lastTerm.Num) + 1,
		NodeID: c.persistent.GetNodeID(),
	}
	c.persistent.UpdateLastTerm(newTerm)
}

func (c *coreLogicImpl) updateLeaderMembers(newMembers []MemberInfo, pos LogPos) error {
	c.leader.members = newMembers
	c.updateVoteRunners()
	c.updateAcceptRunners()

	if err := c.increaseLastCommitted(); err != nil {
		return err
	}

	if c.isInMemberList(c.persistent.GetNodeID()) {
		return nil
	}

	c.leader.leaderStepDownAt = NullLogPos{
		Valid: true,
		Pos:   pos,
	}

	return c.stepDownWhenNotInMemberList()
}

func (c *coreLogicImpl) checkInvariantIfEnabled() {
	if c.alwaysCheckInv {
		c.internalCheckInvariant()
	}
}

func (c *coreLogicImpl) StartElection(maxTermValue TermValue) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateFollower {
		return fmt.Errorf("expected state '%s', got: '%s'", StateFollower.String(), c.state.String())
	}

	commitInfo := c.log.GetCommittedInfo()
	if !IsNodeInMembers(commitInfo.Members, c.persistent.GetNodeID()) {
		return fmt.Errorf("current node is not in its membership config")
	}

	c.state = StateCandidate
	c.generateNextProposeTerm(maxTermValue)

	// init leader state
	c.leader = &leaderStateInfo{
		lastCommitted: commitInfo.FullyReplicated,

		acceptorWakeUpAt: map[NodeID]TimestampMilli{
			c.persistent.GetNodeID(): math.MaxInt64, // current node never wake up
		},
		sendAcceptCond: NewNodeCond(&c.mut),

		acceptorFullyReplicated: map[NodeID]LogPos{},
	}

	c.leader.memLog = NewMemLog(&c.leader.lastCommitted, 10)
	c.leader.logBuffer = NewLogBuffer(&c.leader.lastCommitted, 10)
	c.leader.bufferMaxCond = NewNodeCond(&c.mut)

	// init candidate state
	c.candidate = &candidateStateInfo{
		remainPosMap: map[NodeID]InfiniteLogPos{},
		acceptPos:    commitInfo.FullyReplicated,
	}

	// clear follower
	c.follower = nil

	newMembers := slices.Clone(commitInfo.Members)
	if err := c.updateLeaderMembers(newMembers, commitInfo.FullyReplicated); err != nil {
		return err
	}

	c.updateAllRunners()
	c.checkInvariantIfEnabled()
	return nil
}

func (c *coreLogicImpl) updateVoteRunners() bool {
	if c.state == StateFollower || c.state == StateLeader {
		return c.runner.StartVoteRequestRunners(c.getCurrentTerm(), nil)
	}

	allMembers := GetAllMembers(c.leader.members)
	for nodeID := range allMembers {
		_, ok := c.candidate.remainPosMap[nodeID]
		if ok {
			continue
		}
		c.candidate.remainPosMap[nodeID] = InfiniteLogPos{
			IsFinite: true,
			Pos:      c.candidate.acceptPos + 1,
		}
	}

	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			// if +infinity => remove from runnable voters
			delete(allMembers, nodeID)
		}
	}

	return c.runner.StartVoteRequestRunners(c.getCurrentTerm(), allMembers)
}

func (c *coreLogicImpl) updateAcceptRunners() bool {
	if c.state == StateFollower {
		return c.runner.StartAcceptRequestRunners(c.getCurrentTerm(), nil)
	}

	allMembers := GetAllMembers(c.leader.members)
	return c.runner.StartAcceptRequestRunners(c.getCurrentTerm(), allMembers)
}

func (c *coreLogicImpl) getMaxValidLogPos() LogPos {
	if c.state == StateCandidate {
		return c.candidate.acceptPos
	}
	return c.leader.memLog.MaxLogPos()
}

func (c *coreLogicImpl) validateInMemberList(nodeID NodeID) error {
	if c.isInMemberList(nodeID) {
		return nil
	}
	return fmt.Errorf("node id '%s' is not in current member list", nodeID.String())
}

func (c *coreLogicImpl) GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.checkStateEqual(term, StateCandidate); err != nil {
		return RequestVoteInput{}, err
	}

	if err := c.validateInMemberList(toNode); err != nil {
		return RequestVoteInput{}, err
	}

	remainPos := c.candidate.remainPosMap[toNode]

	if !remainPos.IsFinite {
		err := fmt.Errorf("remain pos of node id '%s' is infinite", toNode.String())
		return RequestVoteInput{}, err
	}

	c.checkInvariantIfEnabled()
	return RequestVoteInput{
		ToNode:  toNode,
		Term:    c.getCurrentTerm(),
		FromPos: remainPos.Pos,
	}, nil
}

func (c *coreLogicImpl) HandleVoteResponse(
	ctx context.Context, id NodeID, output RequestVoteOutput,
) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		c.stepDownWhenEncounterHigherTerm(output.Term)
		c.checkInvariantIfEnabled()
		return nil
	}

StartFunction:
	if err := c.checkStateEqual(output.Term, StateCandidate); err != nil {
		return err
	}

	if err := c.validateInMemberList(id); err != nil {
		return err
	}

	for len(output.Entries) > 0 {
		entry := output.Entries[0]

		status, err := c.handleVoteResponseEntry(ctx, id, entry)
		if err != nil {
			return err
		}
		if status == handleStatusNeedReCheck {
			goto StartFunction
		}

		output.Entries = output.Entries[1:]
	}

	if err := c.increaseAcceptPos(); err != nil {
		return err
	}

	err := c.switchFromCandidateToLeader()
	c.checkInvariantIfEnabled()
	return err
}

func (c *coreLogicImpl) stepDownWhenEncounterHigherTerm(inputTerm TermNum) {
	c.followDoCheckAcceptEntriesRequest(inputTerm)
}

type handleStatus int

const (
	handleStatusSuccess handleStatus = iota + 1
	handleStatusFailed
	handleStatusNeedReCheck
)

func (c *coreLogicImpl) handleVoteResponseEntry(
	ctx context.Context, id NodeID, entry VoteLogEntry,
) (handleStatus, error) {
	AssertTrue(entry.Entry.Pos > 0) // TODO remove

	remainPos := c.candidate.remainPosMap[id]
	if !remainPos.IsFinite {
		// is infinite => do nothing
		return handleStatusFailed, nil
	}

	pos := entry.Entry.Pos

	if entry.IsFinal {
		if pos > remainPos.Pos {
			return handleStatusFailed, nil
		}

		c.candidate.remainPosMap[id] = InfiniteLogPos{}
		c.updateVoteRunners()
		return handleStatusSuccess, nil
	}

	if remainPos.Pos != pos {
		return handleStatusFailed, nil
	}
	if pos <= c.candidate.acceptPos {
		return handleStatusFailed, nil
	}

	return c.waitForFreeSpace(ctx, id, pos, func() {
		c.candidatePutVoteEntry(id, entry)
	})
}

func (c *coreLogicImpl) waitForFreeSpace(
	ctx context.Context, id NodeID, pos LogPos,
	callback func(),
) (handleStatus, error) {
	frontPos := c.leader.logBuffer.GetFrontPos()
	maxBufferPos := frontPos + c.maxBufferLen - 1

	if pos > maxBufferPos {
		if err := c.leader.bufferMaxCond.Wait(ctx, id); err != nil {
			return handleStatusFailed, err
		}
		return handleStatusNeedReCheck, nil
	}

	callback()
	return handleStatusSuccess, nil
}

func (c *coreLogicImpl) candidatePutVoteEntry(id NodeID, entry VoteLogEntry) {
	pos := entry.Entry.Pos

	putEntry := entry.Entry
	if !putEntry.Term.IsFinite {
		// +infinity => update to current term
		putEntry.Term = c.getCurrentTerm().ToInf()
	}

	if putEntry.IsNull() {
		putEntry = NewNoOpLogEntry(pos)
	}

	oldEntry := c.leader.memLog.Get(pos)
	if oldEntry.IsNull() {
		c.leader.memLog.Put(putEntry)
	} else {
		if CompareInfiniteTerm(oldEntry.Term, putEntry.Term) < 0 {
			c.leader.memLog.Put(putEntry)
		}
	}

	c.candidate.remainPosMap[id] = InfiniteLogPos{
		IsFinite: true,
		Pos:      pos + 1,
	}
}

func (c *coreLogicImpl) increaseAcceptPos() error {
	for {
		ok, err := c.tryIncreaseAcceptPosAt(c.candidate.acceptPos + 1)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	return nil
}

func (c *coreLogicImpl) tryIncreaseAcceptPosAt(pos LogPos) (bool, error) {
	if pos > c.leader.memLog.MaxLogPos() {
		return false, nil
	}

	remainOkSet := map[NodeID]struct{}{}
	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			remainOkSet[nodeID] = struct{}{}
			continue
		}

		if remainPos.Pos > pos {
			remainOkSet[nodeID] = struct{}{}
			continue
		}
	}

	if !IsQuorum(c.leader.members, remainOkSet) {
		return false, nil
	}

	c.candidate.acceptPos = pos
	logEntry := c.leader.memLog.Get(pos)
	logEntry.Term = c.getCurrentTerm().ToInf()
	c.leader.memLog.Put(logEntry)
	c.broadcastAllAcceptors()

	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			continue
		}
		// update remain pos of all nodes
		if remainPos.Pos < pos {
			remainPos.Pos = pos + 1
			c.candidate.remainPosMap[nodeID] = remainPos
		}
	}

	if logEntry.Type == LogTypeMembership {
		if err := c.updateLeaderMembers(logEntry.Members, pos); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (c *coreLogicImpl) stepDownWhenNotInMemberList() error {
	stepDownAt := c.leader.leaderStepDownAt
	if !stepDownAt.Valid {
		return nil
	}

	if stepDownAt.Pos > c.leader.lastCommitted {
		return nil
	}

	fullyReplicatedSet := map[NodeID]struct{}{}
	for nodeID, replicatedPos := range c.leader.acceptorFullyReplicated {
		if stepDownAt.Pos <= replicatedPos {
			fullyReplicatedSet[nodeID] = struct{}{}
		}
	}

	if !IsQuorum(c.leader.members, fullyReplicatedSet) {
		return nil
	}

	c.stepDownToFollower(false, true)
	return fmt.Errorf("current leader has just stepped down")
}

func (c *coreLogicImpl) isInMemberList(nodeID NodeID) bool {
	return IsNodeInMembers(c.leader.members, nodeID)
}

func (c *coreLogicImpl) switchFromCandidateToLeader() error {
	infiniteSet := map[NodeID]struct{}{}
	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			infiniteSet[nodeID] = struct{}{}
		}
	}

	if !IsQuorum(c.leader.members, infiniteSet) {
		return nil
	}

	c.state = StateLeader
	c.candidate = nil
	c.updateVoteRunners()
	c.runner.StartStateMachine(c.getCurrentTerm(), StateMachineRunnerInfo{
		Running:       true,
		IsLeader:      true,
		AcceptCommand: true,
	})

	return c.finishMembershipChange()
}

func (c *coreLogicImpl) GetAcceptEntriesRequest(
	ctx context.Context, term TermNum, toNode NodeID,
	fromPos LogPos, lastCommittedSent LogPos,
) (AcceptEntriesInput, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

StartFunction:
	if err := c.isCandidateOrLeader(term); err != nil {
		return AcceptEntriesInput{}, err
	}

	if err := c.validateInMemberList(toNode); err != nil {
		return AcceptEntriesInput{}, err
	}

	maxLogPos := c.getMaxValidLogPos()

	afterCommit := c.leader.lastCommitted + 1
	if fromPos < afterCommit {
		fromPos = afterCommit
	}

	waitCond := func() bool {
		if fromPos <= maxLogPos {
			return false
		}
		if lastCommittedSent < c.leader.lastCommitted {
			return false
		}
		if c.isExpired(c.leader.acceptorWakeUpAt[toNode]) {
			return false
		}
		return true
	}

	if waitCond() {
		if err := c.leader.sendAcceptCond.Wait(ctx, toNode); err != nil {
			return AcceptEntriesInput{}, err
		}
		goto StartFunction
	}

	var acceptEntries []PosLogEntry
	for pos := fromPos; pos <= maxLogPos; pos++ {
		acceptEntries = append(acceptEntries, PosLogEntry{
			Pos:   pos,
			Entry: c.leader.memLog.Get(pos),
		})
	}

	c.leader.acceptorWakeUpAt[toNode] = c.computeNextWakeUp(1)

	c.checkInvariantIfEnabled()
	return AcceptEntriesInput{
		ToNode:    toNode,
		Term:      c.getCurrentTerm(),
		Entries:   acceptEntries,
		NextPos:   maxLogPos + 1,
		Committed: c.leader.lastCommitted,
	}, nil
}

func (c *coreLogicImpl) isCandidateOrLeader(term TermNum) error {
	if !c.doCheckStateIsCandidateOrLeader() {
		return fmt.Errorf("expected state is 'Candidate' or 'Leader', got: '%s'", c.state.String())
	}
	return c.doCheckValidTerm(term)
}

func (c *coreLogicImpl) doCheckStateIsCandidateOrLeader() bool {
	if c.state == StateCandidate {
		return true
	}
	if c.state == StateLeader {
		return true
	}
	return false
}

func (c *coreLogicImpl) FollowerReceiveTermNum(term TermNum) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	ok := c.followDoCheckAcceptEntriesRequest(term)
	c.checkInvariantIfEnabled()
	return ok
}

func (c *coreLogicImpl) followDoCheckAcceptEntriesRequest(term TermNum) bool {
	if CompareTermNum(c.getCurrentTerm(), term) >= 0 {
		// current term >= term => do nothing
		return false
	}

	c.persistent.UpdateLastTerm(term)

	if c.state == StateFollower {
		c.updateFollowerCheckOtherStatus(true, false)
		c.updateStateMachineRunner()
		return true
	}

	// when state = candidate / leader
	c.stepDownToFollower(true, false)
	return true
}

func (c *coreLogicImpl) updateFollowerCheckOtherStatus(
	causedByAnotherLeader bool,
	fastSwitchLeader bool,
) {
	c.follower = &followerStateInfo{
		wakeUpAt: c.computeNextWakeUp(2),
	}

	if causedByAnotherLeader {
		c.follower.checkStatus = followerCheckOtherStatusLeaderIsActive
		c.updateFetchingFollowerInfoRunners()
		return
	}

	c.follower.checkStatus = followerCheckOtherStatusRunning
	c.follower.fastSwitchLeader = fastSwitchLeader
	c.followerRetryCount++

	commitInfo := c.log.GetCommittedInfo()
	c.follower.members = commitInfo.Members
	c.follower.lastNodePos = map[NodeID]LogPos{}
	c.follower.noActiveLeaderSet = map[NodeID]struct{}{}

	c.updateFetchingFollowerInfoRunners()
}

func (c *coreLogicImpl) stepDownToFollower(causedByAnotherLeader bool, fastSwitchLeader bool) {
	c.state = StateFollower
	c.candidate = nil

	c.broadcastAllAcceptors()
	c.leader.bufferMaxCond.Broadcast()
	c.leader = nil

	c.updateFollowerCheckOtherStatus(causedByAnotherLeader, fastSwitchLeader)
	c.updateAllRunners()
}

func (c *coreLogicImpl) updateAllRunners() (bool, string) {
	updated := false
	inputLabel := ""
	setUpdated := func(result bool, label string) {
		if result {
			updated = true
			inputLabel = label
		}
	}

	setUpdated(c.updateVoteRunners(), "vote")
	setUpdated(c.updateAcceptRunners(), "accept")
	setUpdated(c.updateFetchingFollowerInfoRunners(), "fetch")
	setUpdated(c.updateStateMachineRunner(), "state")
	return updated, inputLabel
}

func (c *coreLogicImpl) updateStateMachineRunner() bool {
	term := c.getCurrentTerm()
	if c.state == StateLeader {
		return c.runner.StartStateMachine(term, StateMachineRunnerInfo{
			Running:       true,
			IsLeader:      true,
			AcceptCommand: true,
		})
	}

	if c.state == StateCandidate {
		return c.runner.StartStateMachine(term, StateMachineRunnerInfo{
			Running:  true,
			IsLeader: true,
		})
	}

	return c.runner.StartStateMachine(term, StateMachineRunnerInfo{
		Running: true,
	})
}

func (c *coreLogicImpl) updateFetchingFollowerInfoRunners() bool {
	term := c.getCurrentTerm()

	if c.state != StateFollower {
		updated := false
		if c.runner.StartFetchingFollowerInfoRunners(term, nil, 0) {
			updated = true
		}
		if c.runner.StartElectionRunner(0, false, NodeID{}, 0) {
			updated = true
		}
		return updated
	}

	if c.follower.checkStatus == followerCheckOtherStatusRunning {
		allMembers := GetAllMembers(c.follower.members)
		for id := range allMembers {
			_, ok := c.follower.lastNodePos[id]
			if ok {
				delete(allMembers, id)
			}
		}
		c.runner.StartFetchingFollowerInfoRunners(term, allMembers, c.followerRetryCount)
	} else {
		c.runner.StartFetchingFollowerInfoRunners(term, nil, 0)
	}

	if c.follower.checkStatus == followerCheckOtherStatusStartingNewElection {
		allCheckedNodes := slices.Collect(maps.Keys(c.follower.lastNodePos))
		slices.SortFunc(allCheckedNodes, func(a, b NodeID) int {
			return slices.Compare(a[:], b[:])
		})

		allMembers := GetAllMembers(c.follower.members)

		var lastPos LogPos
		var lastNode NodeID
		for _, id := range allCheckedNodes {
			_, ok := allMembers[id]
			if !ok {
				continue
			}

			replicatedPos := c.follower.lastNodePos[id]
			if replicatedPos > lastPos {
				lastPos = replicatedPos
				lastNode = id
			}
		}

		return c.runner.StartElectionRunner(
			c.follower.lastTermVal, true, lastNode, c.followerRetryCount,
		)
	} else {
		return c.runner.StartElectionRunner(0, false, NodeID{}, 0)
	}
}

func (c *coreLogicImpl) HandleAcceptEntriesResponse(
	fromNode NodeID, output AcceptEntriesOutput,
) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		c.stepDownWhenEncounterHigherTerm(output.Term)
		return nil
	}

	if err := c.isCandidateOrLeader(output.Term); err != nil {
		return err
	}

	for _, pos := range output.PosList {
		c.handleAcceptResponseForPos(fromNode, pos)
	}

	if err := c.increaseLastCommitted(); err != nil {
		return err
	}
	c.checkInvariantIfEnabled()
	return nil
}

func (c *coreLogicImpl) handleAcceptResponseForPos(id NodeID, pos LogPos) bool {
	if pos <= c.leader.lastCommitted {
		return false
	}

	memLog := c.leader.memLog

	maxPos := memLog.MaxLogPos()
	if pos > maxPos {
		return false
	}

	voted := memLog.GetVoted(pos)
	_, existed := voted[id]
	if existed {
		return false
	}

	voted[id] = struct{}{}
	return true
}

func (c *coreLogicImpl) increaseLastCommitted() error {
	memLog := c.leader.memLog

	needCheck := false
	for memLog.GetQueueSize() > 0 {
		votedSet := memLog.GetFrontVoted()
		if !IsQuorum(c.leader.members, votedSet) {
			break
		}

		// when voted set is a quorum
		needCheck = true

		popEntry := memLog.PopFront()
		popEntry.Term = InfiniteTerm{}
		c.leader.logBuffer.Insert(popEntry)
	}

	if needCheck {
		if err := c.finishMembershipChange(); err != nil {
			return err
		}
		c.broadcastAllAcceptors()

		if err := c.stepDownWhenNotInMemberList(); err != nil {
			return err
		}
	}

	return nil
}

func (c *coreLogicImpl) isValidLeader(term TermNum) error {
	if err := c.checkStateEqual(term, StateLeader); err != nil {
		return err
	}

	if !c.isInMemberList(c.persistent.GetNodeID()) {
		return fmt.Errorf("current leader is stopping")
	}

	return nil
}

func (c *coreLogicImpl) InsertCommand(
	ctx context.Context, term TermNum, cmdList ...[]byte,
) error {
	c.mut.Lock()
	defer c.mut.Unlock()

StartFunction:
	if err := c.isValidLeader(term); err != nil {
		return err
	}

	for len(cmdList) > 0 {
		cmd := cmdList[0]

		status, err := c.handleInsertSingleCmd(ctx, cmd)
		if err != nil {
			return err
		}
		if status == handleStatusNeedReCheck {
			goto StartFunction
		}

		cmdList = cmdList[1:]
	}

	c.checkInvariantIfEnabled()
	return nil
}

func (c *coreLogicImpl) handleInsertSingleCmd(
	ctx context.Context, cmd []byte,
) (handleStatus, error) {
	maxPos := c.leader.memLog.MaxLogPos()
	pos := maxPos + 1

	return c.waitForFreeSpace(ctx, c.persistent.GetNodeID(), pos, func() {
		entry := NewCmdLogEntry(
			pos,
			c.getCurrentTerm().ToInf(),
			cmd,
		)
		c.appendNewEntry(entry)
	})
}

func (c *coreLogicImpl) appendNewEntry(entry LogEntry) {
	c.leader.memLog.Put(entry)
	c.broadcastAllAcceptors()
}

func (c *coreLogicImpl) CheckTimeout() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state == StateFollower {
		if c.isExpired(c.follower.wakeUpAt) {
			c.updateFollowerCheckOtherStatus(false, false)
		}
		c.checkInvariantIfEnabled()
		return
	}

	// for candidate & leader
	for nodeID, weakUpAt := range c.leader.acceptorWakeUpAt {
		if c.isExpired(weakUpAt) {
			delete(c.leader.acceptorWakeUpAt, nodeID)
			c.leader.sendAcceptCond.Signal(nodeID)
		}
	}

	c.checkInvariantIfEnabled()
}

func ErrMismatchTerm(inputTerm TermNum, actual TermNum) error {
	return fmt.Errorf(
		"mismatch term number, input: %s, actual: %s",
		inputTerm,
		actual,
	)
}

func (c *coreLogicImpl) doCheckValidTerm(term TermNum) error {
	if c.getCurrentTerm() == term {
		return nil
	}
	return ErrMismatchTerm(term, c.getCurrentTerm())
}

func (c *coreLogicImpl) checkStateEqual(term TermNum, expectedState State) error {
	if c.state != expectedState {
		return fmt.Errorf("expected state '%s', got: '%s'", expectedState.String(), c.state.String())
	}
	return c.doCheckValidTerm(term)
}

func (c *coreLogicImpl) broadcastAllAcceptors() {
	c.leader.sendAcceptCond.Broadcast()
}

func (c *coreLogicImpl) ChangeMembership(ctx context.Context, term TermNum, newNodes []NodeID) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if len(newNodes) <= 0 {
		return fmt.Errorf("can not change membership to empty")
	}

	inputSet := map[NodeID]struct{}{}
	for _, id := range newNodes {
		_, existed := inputSet[id]
		if existed {
			return fmt.Errorf("duplicated node id: %s", id.String())
		}
		inputSet[id] = struct{}{}
	}

StartFunction:
	if err := c.isValidLeader(term); err != nil {
		return err
	}

	pos := c.leader.memLog.MaxLogPos() + 1
	newMembers := append(c.leader.members, MemberInfo{
		Nodes:     newNodes,
		CreatedAt: pos,
	})

	status, err := c.waitForFreeSpace(ctx, c.persistent.GetNodeID(), pos, func() {
		entry := NewMembershipLogEntry(
			pos,
			c.getCurrentTerm().ToInf(),
			newMembers,
		)
		c.appendNewEntry(entry)
	})
	if err != nil {
		return err
	}
	if status == handleStatusNeedReCheck {
		goto StartFunction
	}

	err = c.updateLeaderMembers(newMembers, pos)
	c.checkInvariantIfEnabled()
	return err
}

func (c *coreLogicImpl) doUpdateAcceptorFullyReplicated(nodeID NodeID, pos LogPos) error {
	oldPos := c.leader.acceptorFullyReplicated[nodeID]
	if pos <= oldPos {
		return nil
	}

	c.leader.acceptorFullyReplicated[nodeID] = pos
	c.removeFromLogBuffer(nodeID, pos)

	if err := c.finishMembershipChange(); err != nil {
		return err
	}
	return c.stepDownWhenNotInMemberList()
}

func (c *coreLogicImpl) removeFromLogBuffer(id NodeID, pos LogPos) {
	if id != c.persistent.GetNodeID() {
		return
	}

	for c.leader.logBuffer.Size() > 0 {
		frontPos := c.leader.logBuffer.GetFrontPos()
		if frontPos > pos {
			return
		}
		c.leader.logBuffer.PopFront()
		c.leader.bufferMaxCond.Broadcast()
	}
}

func (c *coreLogicImpl) finishMembershipChange() error {
	if c.state != StateLeader {
		return nil
	}

	if len(c.leader.members) <= 1 {
		return nil
	}

	newConf := c.leader.members[1]
	if newConf.CreatedAt > c.leader.lastCommitted {
		return nil
	}

	validSet := map[NodeID]struct{}{}
	for nodeID, pos := range c.leader.acceptorFullyReplicated {
		if pos < newConf.CreatedAt {
			continue
		}
		validSet[nodeID] = struct{}{}
	}

	if !IsQuorum(c.leader.members, validSet) {
		return nil
	}

	pos := c.leader.memLog.MaxLogPos() + 1
	newMembers := slices.Clone(c.leader.members[1:])
	newMembers[0].CreatedAt = 1

	entry := NewMembershipLogEntry(
		pos,
		c.getCurrentTerm().ToInf(),
		newMembers,
	)
	c.appendNewEntry(entry)

	return c.updateLeaderMembers(newMembers, pos)
}

func (c *coreLogicImpl) GetNeedReplicatedLogEntries(
	input NeedReplicatedInput,
) (AcceptEntriesInput, error) {
	acceptInput, diskPosList, err := c.getNeedReplicatedFromMem(input)
	if err != nil {
		return AcceptEntriesInput{}, err
	}

	if len(diskPosList) > 0 {
		diskEntries := c.log.GetEntriesWithPos(diskPosList...)
		acceptInput.Entries = append(diskEntries, acceptInput.Entries...)
	}

	return acceptInput, nil
}

func (c *coreLogicImpl) getNeedReplicatedFromMem(
	input NeedReplicatedInput,
) (AcceptEntriesInput, []LogPos, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.isCandidateOrLeader(input.Term); err != nil {
		return AcceptEntriesInput{}, nil, err
	}

	if err := c.validateInMemberList(input.FromNode); err != nil {
		return AcceptEntriesInput{}, nil, err
	}

	if err := c.doUpdateAcceptorFullyReplicated(input.FromNode, input.FullyReplicated); err != nil {
		return AcceptEntriesInput{}, nil, err
	}

	if len(input.PosList) == 0 {
		c.checkInvariantIfEnabled()
		return AcceptEntriesInput{
			ToNode: input.FromNode,
			Term:   c.getCurrentTerm(),
		}, nil, nil
	}

	minPos := c.leader.logBuffer.GetFrontPos()
	var memPosList []LogPos
	var diskPosList []LogPos
	for _, pos := range input.PosList {
		if pos >= minPos {
			memPosList = append(memPosList, pos)
		} else {
			diskPosList = append(diskPosList, pos)
		}
	}

	c.checkInvariantIfEnabled()
	return AcceptEntriesInput{
		ToNode:  input.FromNode,
		Term:    c.getCurrentTerm(),
		Entries: NewPosLogEntryList(c.leader.logBuffer.GetEntries(memPosList...)),
	}, diskPosList, nil
}

func (c *coreLogicImpl) GetChoosingLeaderInfo() ChooseLeaderInfo {
	c.mut.Lock()
	defer c.mut.Unlock()

	commitInfo := c.log.GetCommittedInfo()
	output := ChooseLeaderInfo{
		Members:         commitInfo.Members,
		FullyReplicated: commitInfo.FullyReplicated,
		LastTermVal:     c.persistent.GetLastTerm().Num,
	}

	if c.state != StateFollower {
		return output
	}

	if c.follower.checkStatus == followerCheckOtherStatusLeaderIsActive {
		return output
	}

	output.NoActiveLeader = true
	return output
}

func (c *coreLogicImpl) HandleChoosingLeaderInfo(
	fromNode NodeID, term TermNum, info ChooseLeaderInfo,
) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.checkStateEqual(term, StateFollower); err != nil {
		return err
	}

	if c.follower.checkStatus != followerCheckOtherStatusRunning {
		return fmt.Errorf("check status is not running, got: %d", c.follower.checkStatus)
	}

	c.follower.lastTermVal = max(c.follower.lastTermVal, info.LastTermVal)

	if c.follower.lastMaxPos < info.FullyReplicated {
		c.follower.members = info.Members
		c.follower.lastMaxPos = info.FullyReplicated
	}

	c.follower.lastNodePos[fromNode] = info.FullyReplicated

	if info.NoActiveLeader || c.follower.fastSwitchLeader {
		c.follower.noActiveLeaderSet[fromNode] = struct{}{}
	}

	if IsQuorum(c.follower.members, c.follower.noActiveLeaderSet) {
		c.follower.checkStatus = followerCheckOtherStatusStartingNewElection
		c.follower.wakeUpAt = c.computeNextWakeUp(2)
	}

	c.updateFetchingFollowerInfoRunners()
	c.checkInvariantIfEnabled()
	return nil
}

func (c *coreLogicImpl) GetCommittedEntriesWithWait(
	ctx context.Context, term TermNum,
	fromPos LogPos, limit int,
) (GetCommittedEntriesOutput, error) {
	var extra getCommittedEntriesExtra

	output, err := c.getCommittedEntriesWithWaitFromMem(ctx, term, fromPos, limit, &extra)
	if err != nil {
		return GetCommittedEntriesOutput{}, err
	}

	if extra.diskMinPos <= extra.diskMaxPos {
		diskLimit := extra.diskMaxPos - extra.diskMinPos + 1
		diskEntries := c.log.GetEntries(extra.diskMinPos, int(diskLimit))
		output.Entries = append(diskEntries, output.Entries...)
	}

	return output, nil
}

type getCommittedEntriesExtra struct {
	diskMaxPos LogPos
	diskMinPos LogPos
}

func (c *coreLogicImpl) getCommittedEntriesWithWaitFromMem(
	ctx context.Context, term TermNum,
	fromPos LogPos, limit int,
	extra *getCommittedEntriesExtra,
) (GetCommittedEntriesOutput, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

StartFunction:
	if err := c.isCandidateOrLeader(term); err != nil {
		return GetCommittedEntriesOutput{}, err
	}

	if c.leader.lastCommitted < fromPos {
		if err := c.leader.sendAcceptCond.Wait(ctx, c.persistent.GetNodeID()); err != nil {
			return GetCommittedEntriesOutput{}, err
		}
		goto StartFunction
	}

	maxPos := fromPos + LogPos(limit-1)
	if maxPos > c.leader.lastCommitted {
		maxPos = c.leader.lastCommitted
	}

	memMinPos := fromPos
	if memMinPos < c.leader.logBuffer.GetFrontPos() {
		memMinPos = c.leader.logBuffer.GetFrontPos()
	}

	posList := make([]LogPos, 0, maxPos-memMinPos+1)
	for pos := memMinPos; pos <= maxPos; pos++ {
		posList = append(posList, pos)
	}

	memEntries := c.leader.logBuffer.GetEntries(posList...)

	extra.diskMinPos = fromPos
	extra.diskMaxPos = memMinPos - 1

	return GetCommittedEntriesOutput{
		Entries: NewPosLogEntryList(memEntries),
		NextPos: maxPos + 1,
	}, nil
}

func (c *coreLogicImpl) computeNextWakeUp(numTicks int) TimestampMilli {
	return c.nowFunc() + (c.timeoutTickDuration+c.getRandomJitter())*TimestampMilli(numTicks)
}

func (c *coreLogicImpl) getRandomJitter() TimestampMilli {
	if c.tickRandomJitter <= 0 {
		return 0
	}
	randVal := rand.Intn(int(c.tickRandomJitter))
	return TimestampMilli(randVal)
}

func (c *coreLogicImpl) getCurrentTerm() TermNum {
	return c.persistent.GetLastTerm()
}

func (c *coreLogicImpl) isExpired(ts TimestampMilli) bool {
	return ts <= c.nowFunc()
}

// ---------------------------------------------------------------------------
// Utility Functions for testing
// ---------------------------------------------------------------------------

func (c *coreLogicImpl) GetState() State {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.state
}

func (c *coreLogicImpl) GetLastCommitted() LogPos {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.leader.lastCommitted
}

func (c *coreLogicImpl) GetReplicatedPosTest(id NodeID) LogPos {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.leader.acceptorFullyReplicated[id]
}

func (c *coreLogicImpl) GetMinBufferLogPos() LogPos {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.leader.logBuffer.GetFrontPos()
}

func (c *coreLogicImpl) GetMaxLogPos() LogPos {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.leader.memLog.MaxLogPos()
}

func (c *coreLogicImpl) GetFollowerWakeUpAt() TimestampMilli {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.follower.wakeUpAt
}

func (c *coreLogicImpl) CheckInvariant() {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.internalCheckInvariant()
}

func (c *coreLogicImpl) DisableAlwaysCheckInv() {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.alwaysCheckInv = false
}

func (c *coreLogicImpl) internalCheckInvariant() {
	if c.state != StateFollower {
		// check mem log is not null & term is finite
		memLog := c.leader.memLog
		for pos := c.leader.lastCommitted + 1; pos <= memLog.MaxLogPos(); pos++ {
			entry := memLog.Get(pos)
			AssertTrue(entry.Term.IsFinite)
			AssertTrue(!entry.IsNull())
			AssertTrue(entry.Pos == pos)
		}

		// check fully replicated always greater than or equal min buffer pos
		AssertTrue(c.log.GetCommittedInfo().FullyReplicated+1 >= c.leader.logBuffer.GetFrontPos())

		// check log buffer fully replicated
		for pos := c.leader.logBuffer.GetFrontPos(); pos <= c.leader.lastCommitted; pos++ {
			entries := c.leader.logBuffer.GetEntries(pos)
			entry := entries[0]
			AssertTrue(pos == entry.Pos)
			AssertTrue(!entry.Term.IsFinite)
			AssertTrue(!entry.IsNull())
		}
	}

	// check disk log is not null & term is infinite before fully-replicated pos
	fullyReplicated := c.log.GetCommittedInfo().FullyReplicated
	for pos := LogPos(1); pos <= fullyReplicated; pos++ {
		entry := c.log.GetEntriesWithPos(pos)[0]
		AssertTrue(pos == entry.Pos)
		AssertTrue(!entry.Entry.Term.IsFinite)
		AssertTrue(!entry.Entry.IsNull())
	}

	switch c.state {
	case StateLeader:
		AssertTrue(c.follower == nil)
		AssertTrue(c.candidate == nil)
		AssertTrue(c.leader != nil)

	case StateCandidate:
		AssertTrue(c.follower == nil)
		AssertTrue(c.candidate != nil)
		AssertTrue(c.leader != nil)
		AssertTrue(c.candidate.acceptPos <= c.leader.memLog.MaxLogPos())

	default:
		AssertTrue(c.follower != nil)
		AssertTrue(c.candidate == nil)
		AssertTrue(c.leader == nil)
		AssertTrue(c.state == StateFollower)
	}

	if updated, label := c.updateAllRunners(); updated {
		panic("Invariant failed on label: " + label)
	}
}

func AssertTrue(b bool) {
	if !b {
		panic("Should be true here")
	}
}
