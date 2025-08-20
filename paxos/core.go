package paxos

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

type CoreLogic interface {
	StartElection(term TermNum, maxTermValue TermValue) error

	GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, error)
	HandleVoteResponse(fromNode NodeID, output RequestVoteOutput) error

	GetAcceptEntriesRequest(
		ctx context.Context, term TermNum, toNode NodeID,
		fromPos LogPos, lastCommittedSent LogPos,
	) (AcceptEntriesInput, error)

	FollowerReceiveAcceptEntriesRequest(term TermNum, pos LogPos) bool

	HandleAcceptEntriesResponse(fromNode NodeID, output AcceptEntriesOutput) error

	InsertCommand(term TermNum, cmdDataList ...[]byte) error

	CheckTimeout()

	ChangeMembership(term TermNum, newNodes []NodeID) error

	UpdateAcceptorFullyReplicated(term TermNum, nodeID NodeID, pos LogPos) error

	GetReadyToStartElection(ctx context.Context, term TermNum) error

	// -------------------------------------------------------
	// Testing Utility Functions
	// -------------------------------------------------------

	GetState() State
	GetLastCommitted() LogPos

	// CheckInvariant for testing only
	CheckInvariant()
}

func NewCoreLogic(
	persistent PersistentState,
	log LogStorage,
	runner NodeRunner,
	nowFunc func() TimestampMilli,
) CoreLogic {
	c := &coreLogicImpl{
		state:   StateFollower,
		nowFunc: nowFunc,

		persistent: persistent,
		log:        log,
		runner:     runner,
	}

	c.follower = &followerStateInfo{
		wakeUpAt: c.nowFunc(),
		waitCond: NewNodeCond(&c.mut),
	}

	c.resetRunnersForFollower()

	return c
}

type coreLogicImpl struct {
	nowFunc func() TimestampMilli

	mut   sync.Mutex
	state State

	follower  *followerStateInfo
	candidate *candidateStateInfo
	leader    *leaderStateInfo

	persistent PersistentState
	log        LogStorage
	runner     NodeRunner
}

type followerStateInfo struct {
	wakeUpAt TimestampMilli
	waitCond *NodeCond
}

type candidateStateInfo struct {
	remainPosMap map[NodeID]InfiniteLogPos
	acceptPos    LogPos
}

type leaderStateInfo struct {
	members       []MemberInfo
	lastCommitted LogPos

	memLog *MemLog

	acceptorWakeUpAt map[NodeID]TimestampMilli
	nodeCondVar      *NodeCond

	acceptorFullyReplicated map[NodeID]LogPos
}

func (c *coreLogicImpl) generateNextProposeTerm(maxTermValue TermValue) {
	lastTerm := c.persistent.GetLastTerm()
	newTerm := TermNum{
		Num:    max(maxTermValue, lastTerm.Num) + 1,
		NodeID: c.persistent.GetNodeID(),
	}
	c.persistent.UpdateLastTerm(newTerm)
}

func (c *coreLogicImpl) StartElection(inputTerm TermNum, maxTermValue TermValue) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.checkStateEqual(inputTerm, StateFollower); err != nil {
		return err
	}

	c.state = StateCandidate
	commitInfo := c.log.GetCommittedInfo()

	c.generateNextProposeTerm(maxTermValue)

	// init leader state
	c.leader = &leaderStateInfo{
		members:       commitInfo.Members,
		lastCommitted: commitInfo.Pos,

		acceptorWakeUpAt: map[NodeID]TimestampMilli{},
		nodeCondVar:      NewNodeCond(&c.mut),

		acceptorFullyReplicated: map[NodeID]LogPos{},
	}
	c.leader.memLog = NewMemLog(&c.leader.lastCommitted, 10)

	// init candidate state
	c.candidate = &candidateStateInfo{
		remainPosMap: map[NodeID]InfiniteLogPos{},
		acceptPos:    commitInfo.Pos,
	}

	// clear follower
	c.follower.waitCond.Broadcast()
	c.follower = nil

	c.updateVoteRunners()
	c.updateAcceptRunners()

	term := c.getCurrentTerm()
	c.runner.SetLeader(term, false)
	c.runner.StartFollowerRunner(term, false)

	return nil
}

func (c *coreLogicImpl) updateVoteRunners() {
	if c.state == StateLeader {
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

	remainPos, ok := c.candidate.remainPosMap[toNode]
	if !ok {
		// This should never occur
		err := fmt.Errorf("missing remain pos for node id '%s'", toNode.String())
		return RequestVoteInput{}, err
	}

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

func (c *coreLogicImpl) HandleVoteResponse(id NodeID, output RequestVoteOutput) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		// TODO handle
		return nil
	}

	if err := c.checkStateEqual(output.Term, StateCandidate); err != nil {
		return err
	}

	for _, entry := range output.Entries {
		c.handleVoteResponseEntry(id, entry)
	}

	c.increaseAcceptPos()
	c.switchFromCandidateToLeader()

	return nil
}

func (c *coreLogicImpl) handleVoteResponseEntry(
	id NodeID, entry VoteLogEntry,
) {
	remainPos := c.candidate.remainPosMap[id]
	if !remainPos.IsFinite {
		// is infinite => do nothing
		return
	}

	pos := entry.Pos
	if entry.More {
		if remainPos.Pos != pos {
			return
		}
		if pos <= c.candidate.acceptPos {
			return
		}
	} else {
		if pos > remainPos.Pos {
			return
		}
	}

	c.candidatePutVoteEntry(id, entry)
}

func (c *coreLogicImpl) candidatePutVoteEntry(id NodeID, entry VoteLogEntry) {
	pos := entry.Pos

	if !entry.More {
		c.candidate.remainPosMap[id] = InfiniteLogPos{}
		c.updateVoteRunners()
		return
	}

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

func (c *coreLogicImpl) increaseAcceptPos() {
	for {
		ok := c.tryIncreaseAcceptPosAt(c.candidate.acceptPos + 1)
		if !ok {
			break
		}
	}
}

func (c *coreLogicImpl) tryIncreaseAcceptPosAt(pos LogPos) bool {
	if pos > c.leader.memLog.MaxLogPos() {
		return false
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
		return false
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
		c.leader.members = logEntry.Members
		c.updateVoteRunners()
		c.updateAcceptRunners()
	}

	return true
}

func (c *coreLogicImpl) stepDownWhenNotInMemberList() {
	if c.leader.memLog.GetQueueSize() > 0 {
		return
	}
	if c.isInMemberList(c.persistent.GetNodeID()) {
		return
	}
	c.stepDownToFollower()
}

func (c *coreLogicImpl) isInMemberList(nodeID NodeID) bool {
	for _, conf := range c.leader.members {
		for _, id := range conf.Nodes {
			if id == nodeID {
				return true
			}
		}
	}
	return false
}

func (c *coreLogicImpl) switchFromCandidateToLeader() {
	infiniteSet := map[NodeID]struct{}{}
	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			infiniteSet[nodeID] = struct{}{}
		}
	}

	if !IsQuorum(c.leader.members, infiniteSet) {
		return
	}

	c.state = StateLeader
	c.candidate = nil
	c.updateVoteRunners()
	c.runner.SetLeader(c.getCurrentTerm(), true)

	c.finishMembershipChange()
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

	var acceptEntries []AcceptLogEntry
	for pos := fromPos; pos <= maxLogPos; pos++ {
		acceptEntries = append(acceptEntries, AcceptLogEntry{
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

func (c *coreLogicImpl) FollowerReceiveAcceptEntriesRequest(
	term TermNum, pog LogPos,
) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if CompareTermNum(c.getCurrentTerm(), term) >= 0 {
		// current term >= term => do nothing
		return false
	}

	c.persistent.UpdateLastTerm(term)

	if c.state == StateFollower {
		c.follower.wakeUpAt = c.computeNextWakeUp()
		c.resetRunnersForFollower()
		return true
	}

	// when state = candidate / leader
	c.stepDownToFollower()
	return true
}

func (c *coreLogicImpl) stepDownToFollower() {
	c.state = StateFollower
	c.candidate = nil

	c.broadcastAllAcceptors()
	c.leader = nil

	c.follower = &followerStateInfo{
		wakeUpAt: c.computeNextWakeUp(),
		waitCond: NewNodeCond(&c.mut),
	}

	c.resetRunnersForFollower()
}

func (c *coreLogicImpl) resetRunnersForFollower() {
	term := c.getCurrentTerm()

	c.runner.StartVoteRequestRunners(term, nil)
	c.runner.StartAcceptRequestRunners(term, nil)
	c.runner.StartFollowerRunner(term, true)
	c.runner.SetLeader(term, false)
}

func (c *coreLogicImpl) HandleAcceptEntriesResponse(
	fromNode NodeID, output AcceptEntriesOutput,
) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		// TODO handle
		return nil
	}

	if err := c.isCandidateOrLeader(output.Term); err != nil {
		return err
	}

	for _, pos := range output.PosList {
		c.handleAcceptResponseForPos(fromNode, pos)
	}

	c.increaseLastCommitted()
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

func (c *coreLogicImpl) increaseLastCommitted() {
	memLog := c.leader.memLog

	for memLog.GetQueueSize() > 0 {
		term := memLog.GetFrontTerm()
		if term.IsFinite {
			break
		}

		// when term = +infinity
		memLog.PopFront()

		c.finishMembershipChange()
		c.broadcastAllAcceptors()
		c.stepDownWhenNotInMemberList()
	}
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

func (c *coreLogicImpl) InsertCommand(term TermNum, cmdList ...[]byte) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.isValidLeader(term); err != nil {
		return err
	}

	for _, cmd := range cmdList {
		maxPos := c.leader.memLog.MaxLogPos()
		pos := maxPos + 1
		entry := LogEntry{
			Type:    LogTypeCmd,
			Term:    c.getCurrentTerm().ToInf(),
			CmdData: cmd,
		}
		c.appendNewEntry(pos, entry)
	}

	return nil
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
			c.follower.waitCond.Broadcast()
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
	c.leader.members = append(c.leader.members, MemberInfo{
		Nodes:     newNodes,
		CreatedAt: pos,
	})

	entry := LogEntry{
		Type:    LogTypeMembership,
		Term:    c.getCurrentTerm().ToInf(),
		Members: c.leader.members,
	}
	c.appendNewEntry(pos, entry)
	c.updateAcceptRunners()

	return nil
}

func (c *coreLogicImpl) UpdateAcceptorFullyReplicated(
	term TermNum, nodeID NodeID, pos LogPos,
) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err := c.isCandidateOrLeader(term); err != nil {
		return err
	}

	c.leader.acceptorFullyReplicated[nodeID] = pos
	c.finishMembershipChange()

	return nil
}

func (c *coreLogicImpl) finishMembershipChange() {
	if c.state != StateLeader {
		return
	}

	if len(c.leader.members) <= 1 {
		return
	}

	newConf := c.leader.members[1]
	if newConf.CreatedAt > c.leader.lastCommitted {
		return
	}

	validSet := map[NodeID]struct{}{}
	for nodeID, pos := range c.leader.acceptorFullyReplicated {
		if pos < newConf.CreatedAt {
			continue
		}
		validSet[nodeID] = struct{}{}
	}

	if !IsQuorum([]MemberInfo{newConf}, validSet) {
		return
	}

	pos := c.leader.memLog.MaxLogPos() + 1
	c.leader.members = slices.Clone(c.leader.members[1:])
	c.leader.members[0].CreatedAt = 1

	entry := LogEntry{
		Type:    LogTypeMembership,
		Term:    c.getCurrentTerm().ToInf(),
		Members: c.leader.members,
	}
	c.appendNewEntry(pos, entry)
	c.updateAcceptRunners()
}

func (c *coreLogicImpl) GetReadyToStartElection(ctx context.Context, term TermNum) error {
	c.mut.Lock()
	defer c.mut.Unlock()

StartLoop:
	if err := c.checkStateEqual(term, StateFollower); err != nil {
		return err
	}

	if !c.isExpired(c.follower.wakeUpAt) {
		if err := c.follower.waitCond.Wait(ctx, c.persistent.GetNodeID()); err != nil {
			return err
		}
		goto StartLoop
	}

	c.follower.wakeUpAt = c.computeNextWakeUp()
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
	return c.leader.lastCommitted
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
			assertTrue(entry.Term.IsFinite)
			assertTrue(entry.Type != LogTypeNull)
		}
	}

	switch c.state {
	case StateLeader:
		assertTrue(c.follower == nil)
		assertTrue(c.candidate == nil)
		assertTrue(c.leader != nil)

	case StateCandidate:
		assertTrue(c.follower == nil)
		assertTrue(c.candidate != nil)
		assertTrue(c.leader != nil)

	default:
		assertTrue(c.follower != nil)
		assertTrue(c.candidate == nil)
		assertTrue(c.leader == nil)
		assertTrue(c.state == StateFollower)
	}
}

func assertTrue(b bool) {
	if !b {
		panic("Should be true here")
	}
}
