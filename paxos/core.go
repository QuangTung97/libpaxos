package paxos

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

type CoreLogic interface {
	StartElection(maxTermValue TermValue) error

	GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, error)
	HandleVoteResponse(ctx context.Context, fromNode NodeID, output RequestVoteOutput) error

	GetAcceptEntriesRequest(
		ctx context.Context, term TermNum, toNode NodeID,
		fromPos LogPos, lastCommittedSent LogPos,
	) (AcceptEntriesInput, error)

	FollowerReceiveAcceptEntriesRequest(term TermNum) bool

	HandleAcceptEntriesResponse(fromNode NodeID, output AcceptEntriesOutput) error

	InsertCommand(ctx context.Context, term TermNum, cmdDataList ...[]byte) error

	CheckTimeout()

	ChangeMembership(term TermNum, newNodes []NodeID) error

	GetNeedReplicatedLogEntries(input NeedReplicatedInput) (AcceptEntriesInput, error)

	GetChoosingLeaderInfo() ChooseLeaderInfo

	HandleChoosingLeaderInfo(fromNode NodeID, term TermNum, info ChooseLeaderInfo) error

	// -------------------------------------------------------
	// Testing Utility Functions
	// -------------------------------------------------------

	GetState() State
	GetLastCommitted() LogPos
	GetMinBufferLogPos() LogPos
	GetFollowerWakeUpAt() TimestampMilli

	// CheckInvariant for testing only
	CheckInvariant()
}

func NewCoreLogic(
	persistent PersistentState,
	log LeaderLogGetter,
	runner NodeRunner,
	nowFunc func() TimestampMilli,
	maxBufferLen LogPos,
) CoreLogic {
	c := &coreLogicImpl{
		nowFunc:      nowFunc,
		maxBufferLen: maxBufferLen,

		state: StateFollower,

		persistent: persistent,
		log:        log,
		runner:     runner,
	}

	c.updateFollowerCheckOtherStatus(false)
	c.updateAllRunners()

	return c
}

type coreLogicImpl struct {
	nowFunc      func() TimestampMilli
	maxBufferLen LogPos

	mut   sync.Mutex
	state State

	follower  *followerStateInfo
	candidate *candidateStateInfo
	leader    *leaderStateInfo

	followerRetryCount int

	persistent PersistentState
	log        LeaderLogGetter
	runner     NodeRunner
}

type followerStateInfo struct {
	wakeUpAt    TimestampMilli
	checkStatus followerCheckOtherStatus

	members     []MemberInfo
	lastPos     LogPos
	lastNodeID  NodeID
	lastTermVal TermValue

	checkedSet        map[NodeID]struct{}
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
	nodeCondVar      *NodeCond

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

	if c.isInMemberList(c.persistent.GetNodeID()) {
		return nil
	}

	c.leader.leaderStepDownAt = NullLogPos{
		Valid: true,
		Pos:   pos,
	}

	return c.stepDownWhenNotInMemberList()
}

func (c *coreLogicImpl) StartElection(maxTermValue TermValue) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateFollower {
		return fmt.Errorf("expected state '%s', got: '%s'", StateFollower.String(), c.state.String())
	}

	c.state = StateCandidate
	commitInfo := c.log.GetCommittedInfo()

	if !IsNodeInMembers(commitInfo.Members, c.persistent.GetNodeID()) {
		// TODO testing
		return fmt.Errorf("current node is not in its membership config")
	}

	c.generateNextProposeTerm(maxTermValue)

	// init leader state
	c.leader = &leaderStateInfo{
		lastCommitted: commitInfo.FullyReplicated,

		acceptorWakeUpAt: map[NodeID]TimestampMilli{},
		nodeCondVar:      NewNodeCond(&c.mut),

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
	return nil
}

func (c *coreLogicImpl) updateVoteRunners() {
	if c.state == StateFollower || c.state == StateLeader {
		c.runner.StartVoteRequestRunners(c.getCurrentTerm(), nil)
		return
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
	c.runner.StartVoteRequestRunners(c.getCurrentTerm(), allMembers)
}

func (c *coreLogicImpl) updateAcceptRunners() {
	if c.state == StateFollower {
		c.runner.StartAcceptRequestRunners(c.getCurrentTerm(), nil)
		return
	}

	allMembers := GetAllMembers(c.leader.members)
	c.runner.StartAcceptRequestRunners(c.getCurrentTerm(), allMembers)
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
		return nil
	}

StartFunction:
	if err := c.checkStateEqual(output.Term, StateCandidate); err != nil {
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

	return c.switchFromCandidateToLeader()
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
	remainPos := c.candidate.remainPosMap[id]
	if !remainPos.IsFinite {
		// is infinite => do nothing
		return handleStatusFailed, nil
	}

	pos := entry.Pos

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

	return c.waitForFreeSpace(ctx, id, entry.Pos, func() {
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
	pos := entry.Pos

	putEntry := entry.Entry
	if !putEntry.Term.IsFinite {
		// +infinity => update to current term
		putEntry.Term = c.getCurrentTerm().ToInf()
	}

	if putEntry.IsNull() {
		putEntry = NewNoOpLogEntry()
	}

	oldEntry := c.leader.memLog.Get(pos)
	if oldEntry.IsNull() {
		c.leader.memLog.Put(pos, putEntry)
	} else {
		if CompareInfiniteTerm(oldEntry.Term, putEntry.Term) < 0 {
			c.leader.memLog.Put(pos, putEntry)
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
	c.leader.memLog.Put(pos, logEntry)
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

	c.stepDownToFollower(false)
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
	c.runner.SetLeader(c.getCurrentTerm(), true)

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
		if err := c.leader.nodeCondVar.Wait(ctx, toNode); err != nil {
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

	c.leader.acceptorWakeUpAt[toNode] = c.computeNextWakeUp()

	return AcceptEntriesInput{
		ToNode:    toNode,
		Term:      c.getCurrentTerm(),
		Entries:   acceptEntries,
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

func (c *coreLogicImpl) FollowerReceiveAcceptEntriesRequest(term TermNum) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	return c.followDoCheckAcceptEntriesRequest(term)
}

func (c *coreLogicImpl) followDoCheckAcceptEntriesRequest(term TermNum) bool {
	if CompareTermNum(c.getCurrentTerm(), term) >= 0 {
		// current term >= term => do nothing
		return false
	}

	c.persistent.UpdateLastTerm(term)

	if c.state == StateFollower {
		c.updateFollowerCheckOtherStatus(true)
		return true
	}

	// when state = candidate / leader
	c.stepDownToFollower(true)
	return true
}

func (c *coreLogicImpl) updateFollowerCheckOtherStatus(causedByAnotherLeader bool) {
	c.follower = &followerStateInfo{
		wakeUpAt: c.computeNextWakeUp(),
	}

	if causedByAnotherLeader {
		c.follower.checkStatus = followerCheckOtherStatusLeaderIsActive
		c.updateFetchingFollowerInfoRunners()
		return
	}

	c.follower.checkStatus = followerCheckOtherStatusRunning
	c.followerRetryCount++

	commitInfo := c.log.GetCommittedInfo()
	c.follower.members = commitInfo.Members
	c.follower.lastPos = commitInfo.FullyReplicated
	c.follower.lastNodeID = c.persistent.GetNodeID()

	c.follower.checkedSet = map[NodeID]struct{}{}
	c.follower.noActiveLeaderSet = map[NodeID]struct{}{}

	c.updateFetchingFollowerInfoRunners()
}

func (c *coreLogicImpl) stepDownToFollower(causedByAnotherLeader bool) {
	c.state = StateFollower
	c.candidate = nil

	c.broadcastAllAcceptors()
	c.leader.bufferMaxCond.Broadcast()
	c.leader = nil

	c.updateFollowerCheckOtherStatus(causedByAnotherLeader)
	c.updateAllRunners()
}

func (c *coreLogicImpl) updateAllRunners() {
	c.updateVoteRunners()
	c.updateAcceptRunners()
	c.updateFetchingFollowerInfoRunners()

	term := c.getCurrentTerm()
	if c.state == StateLeader {
		c.runner.SetLeader(term, true)
	} else {
		c.runner.SetLeader(term, false)
	}
}

func (c *coreLogicImpl) updateFetchingFollowerInfoRunners() {
	term := c.getCurrentTerm()

	if c.state != StateFollower {
		c.runner.StartFetchingFollowerInfoRunners(term, nil, 0)
		c.runner.StartElectionRunner(0, false, NodeID{}, 0)
		return
	}

	if c.follower.checkStatus == followerCheckOtherStatusRunning {
		allMembers := GetAllMembers(c.follower.members)
		for id := range allMembers {
			_, ok := c.follower.checkedSet[id]
			if ok {
				delete(allMembers, id)
			}
		}
		c.runner.StartFetchingFollowerInfoRunners(term, allMembers, c.followerRetryCount)
	} else {
		c.runner.StartFetchingFollowerInfoRunners(term, nil, 0)
	}

	if c.follower.checkStatus == followerCheckOtherStatusStartingNewElection {
		c.runner.StartElectionRunner(
			c.follower.lastTermVal, true, c.follower.lastNodeID, c.followerRetryCount,
		)
	} else {
		c.runner.StartElectionRunner(0, false, NodeID{}, 0)
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

	_ = c.increaseLastCommitted()
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

	if IsQuorum(c.leader.members, voted) {
		// set log entry term as +infinity
		entry := memLog.Get(pos)
		entry.Term = InfiniteTerm{}
		memLog.Put(pos, entry)
	}

	return true
}

func (c *coreLogicImpl) increaseLastCommitted() error {
	memLog := c.leader.memLog

	for memLog.GetQueueSize() > 0 {
		term := memLog.GetFrontTerm()
		if term.IsFinite {
			break
		}

		// when term = +infinity
		popEntry := memLog.PopFront()
		c.leader.logBuffer.Insert(popEntry)

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

	return nil
}

func (c *coreLogicImpl) handleInsertSingleCmd(
	ctx context.Context, cmd []byte,
) (handleStatus, error) {
	maxPos := c.leader.memLog.MaxLogPos()
	pos := maxPos + 1

	return c.waitForFreeSpace(ctx, c.persistent.GetNodeID(), pos, func() {
		entry := LogEntry{
			Type:    LogTypeCmd,
			Term:    c.getCurrentTerm().ToInf(),
			CmdData: cmd,
		}
		c.appendNewEntry(pos, entry)
	})
}

func (c *coreLogicImpl) appendNewEntry(pos LogPos, entry LogEntry) {
	c.leader.memLog.Put(pos, entry)
	c.broadcastAllAcceptors()
}

func (c *coreLogicImpl) CheckTimeout() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state == StateFollower {
		if c.isExpired(c.follower.wakeUpAt) {
			c.updateFollowerCheckOtherStatus(false)
		}
		return
	}

	// for candidate & leader
	for nodeID, weakUpAt := range c.leader.acceptorWakeUpAt {
		if c.isExpired(weakUpAt) {
			delete(c.leader.acceptorWakeUpAt, nodeID)
			c.leader.nodeCondVar.Signal(nodeID)
		}
	}
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
	c.leader.nodeCondVar.Broadcast()
}

func (c *coreLogicImpl) ChangeMembership(term TermNum, newNodes []NodeID) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.isValidLeader(term); err != nil {
		return err
	}

	pos := c.leader.memLog.MaxLogPos() + 1
	newMembers := append(c.leader.members, MemberInfo{
		Nodes:     newNodes,
		CreatedAt: pos,
	})

	entry := NewMembershipLogEntry(
		c.getCurrentTerm().ToInf(),
		newMembers,
	)
	c.appendNewEntry(pos, entry)

	return c.updateLeaderMembers(newMembers, pos)
}

func (c *coreLogicImpl) doUpdateAcceptorFullyReplicated(nodeID NodeID, pos LogPos) error {
	c.leader.acceptorFullyReplicated[nodeID] = pos
	c.removeFromLogBuffer(nodeID, pos)
	return c.finishMembershipChange()
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
		c.getCurrentTerm().ToInf(),
		newMembers,
	)
	c.appendNewEntry(pos, entry)

	return c.updateLeaderMembers(newMembers, pos)
}

func (c *coreLogicImpl) GetNeedReplicatedLogEntries(
	input NeedReplicatedInput,
) (AcceptEntriesInput, error) {
	acceptInput, diskPosList, err := c.getNeedReplicatedFromMem(input)
	if err != nil {
		return AcceptEntriesInput{}, err
	}

	diskEntries := c.log.GetEntriesWithPos(diskPosList...)
	acceptInput.Entries = append(diskEntries, acceptInput.Entries...)

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

	if err := c.doUpdateAcceptorFullyReplicated(input.FromNode, input.FullyReplicated); err != nil {
		return AcceptEntriesInput{}, nil, err
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

	return AcceptEntriesInput{
		ToNode:  input.FromNode,
		Term:    c.getCurrentTerm(),
		Entries: c.leader.logBuffer.GetEntries(memPosList...),
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

	if c.follower.lastPos < info.FullyReplicated {
		c.follower.lastNodeID = fromNode
		c.follower.lastPos = info.FullyReplicated
		c.follower.members = info.Members
	}

	c.follower.checkedSet[fromNode] = struct{}{}
	if info.NoActiveLeader {
		c.follower.noActiveLeaderSet[fromNode] = struct{}{}
	}

	if IsQuorum(c.follower.members, c.follower.noActiveLeaderSet) {
		c.follower.checkStatus = followerCheckOtherStatusStartingNewElection
		c.follower.wakeUpAt = c.computeNextWakeUp()
	}

	c.updateFetchingFollowerInfoRunners()
	return nil
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

func (c *coreLogicImpl) GetMinBufferLogPos() LogPos {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.leader.logBuffer.GetFrontPos()
}

func (c *coreLogicImpl) GetFollowerWakeUpAt() TimestampMilli {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.follower.wakeUpAt
}

func (c *coreLogicImpl) computeNextWakeUp() TimestampMilli {
	// TODO add jitter
	return c.nowFunc() + 5_000 // TODO configurable
}

func (c *coreLogicImpl) getCurrentTerm() TermNum {
	return c.persistent.GetLastTerm()
}

func (c *coreLogicImpl) isExpired(ts TimestampMilli) bool {
	return ts <= c.nowFunc()
}

func (c *coreLogicImpl) CheckInvariant() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateFollower {
		memLog := c.leader.memLog
		for pos := c.leader.lastCommitted + 1; pos <= memLog.MaxLogPos(); pos++ {
			entry := memLog.Get(pos)
			AssertTrue(entry.Term.IsFinite)
			AssertTrue(entry.Type != LogTypeNull)
		}
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
}

func AssertTrue(b bool) {
	if !b {
		panic("Should be true here")
	}
}
