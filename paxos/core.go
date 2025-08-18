package paxos

import (
	"context"
	"sync"
)

type CoreLogic interface {
	StartElection(term TermNum) bool

	GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, bool)
	HandleVoteResponse(fromNode NodeID, output RequestVoteOutput) bool

	GetAcceptEntriesRequest(
		ctx context.Context, term TermNum, toNode NodeID,
		fromPos LogPos, lastCommittedSent LogPos,
	) (AcceptEntriesInput, bool)

	FollowerReceiveAcceptEntriesRequest(fromNode NodeID, term TermNum) bool

	HandleAcceptEntriesResponse(fromNode NodeID, output AcceptEntriesOutput) bool

	InsertCommand(term TermNum, cmdDataList ...[]byte) bool

	CheckTimeout()

	ChangeMembership(term TermNum, newNodes []NodeID) bool

	UpdateAcceptorFullyReplicated(term TermNum, nodeID NodeID, pos LogPos) bool

	GetReadyToStartElection(ctx context.Context, term TermNum) bool

	GetState() State
	GetLastCommitted() LogPos
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

	lastTerm := c.persistent.GetLastTerm()

	c.follower = &followerStateInfo{
		wakeUpAt: c.nowFunc(),
		waitCond: NewNodeCond(&c.mut),
	}

	c.runner.SetLeader(lastTerm, false)
	c.runner.StartFollowerRunner(true, lastTerm)

	return c
}

type coreLogicImpl struct {
	nowFunc func() TimestampMilli

	mut    sync.Mutex
	state  State
	nodeID NodeID

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

func (c *coreLogicImpl) StartElection(inputTerm TermNum) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateFollower {
		// TODO testing
		return false
	}

	if c.persistent.GetLastTerm() != inputTerm {
		// TODO testing
		return false
	}

	c.state = StateCandidate

	commitInfo := c.log.GetCommittedInfo()

	c.persistent.NextProposeTerm()

	c.leader = &leaderStateInfo{
		members:       commitInfo.Members,
		lastCommitted: commitInfo.Pos,

		acceptorWakeUpAt: map[NodeID]TimestampMilli{},
		nodeCondVar:      NewNodeCond(&c.mut),

		acceptorFullyReplicated: map[NodeID]LogPos{},
	}

	c.leader.memLog = NewMemLog(&c.leader.lastCommitted, 10)

	c.candidate = &candidateStateInfo{
		remainPosMap: map[NodeID]InfiniteLogPos{},
		acceptPos:    commitInfo.Pos,
	}
	c.follower = nil

	c.updateVoteRunners()
	c.updateAcceptRunners()

	term := c.getCurrentTerm()
	c.runner.SetLeader(term, false)
	c.runner.StartFollowerRunner(false, term)

	return true
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

func (c *coreLogicImpl) GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateCandidate {
		// TODO testing
		return RequestVoteInput{}, false
	}
	if !c.isValidTerm(term) {
		// TODO testing
		return RequestVoteInput{}, false
	}

	remainPos, ok := c.candidate.remainPosMap[toNode]
	if !ok {
		// TODO testing
		return RequestVoteInput{}, false
	}

	if !remainPos.IsFinite {
		return RequestVoteInput{}, false
	}

	return RequestVoteInput{
		ToNode:  toNode,
		Term:    c.getCurrentTerm(),
		FromPos: remainPos.Pos,
	}, true
}

func (c *coreLogicImpl) HandleVoteResponse(id NodeID, output RequestVoteOutput) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		// TODO handle
		return true
	}

	if c.state != StateCandidate {
		return false
	}

	if !c.isValidTerm(output.Term) {
		// TODO testing
		return false
	}

	for _, entry := range output.Entries {
		c.handleVoteResponseEntry(id, entry)
	}

	c.increaseAcceptPos()
	c.switchFromCandidateToLeader()

	return true
}

func (c *coreLogicImpl) handleVoteResponseEntry(
	id NodeID, entry VoteLogEntry,
) {
	remainPos := c.candidate.remainPosMap[id]
	if !remainPos.IsFinite {
		// is infinite => do nothing
		// TODO testing
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

	term := c.getCurrentTerm()

	putEntry := entry.Entry
	if putEntry.IsNull() {
		putEntry = NewNoOpLogEntry(term)
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

	// TODO step down to Follower when current candidate is no longer in membership list

	return true
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
}

func (c *coreLogicImpl) GetAcceptEntriesRequest(
	ctx context.Context, term TermNum, toNode NodeID,
	fromPos LogPos, lastCommittedSent LogPos,
) (AcceptEntriesInput, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

StartFunction:
	if !c.isCandidateOrLeader() {
		// TODO testing
		return AcceptEntriesInput{}, false
	}

	if !c.isValidTerm(term) {
		// TODO testing
		return AcceptEntriesInput{}, false
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
		if c.leader.acceptorWakeUpAt[toNode] <= c.nowFunc() {
			return false
		}
		return true
	}

	if waitCond() {
		if err := c.leader.nodeCondVar.Wait(ctx, toNode); err != nil {
			return AcceptEntriesInput{}, false
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
	}, true
}

func (c *coreLogicImpl) isCandidateOrLeader() bool {
	if c.state == StateCandidate {
		return true
	}
	if c.state == StateLeader {
		return true
	}
	return false
}

func (c *coreLogicImpl) FollowerReceiveAcceptEntriesRequest(
	fromNode NodeID, term TermNum,
) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !c.persistent.RecordLastTerm(term) {
		// no update => do nothing
		return false
	}

	if c.state == StateFollower {
		c.follower.wakeUpAt = c.computeNextWakeUp()
		return true
	}

	// when state = candidate / leader
	c.state = StateFollower
	c.candidate = nil

	c.broadcastAllAcceptors()
	c.leader = nil

	c.follower = &followerStateInfo{
		wakeUpAt: c.computeNextWakeUp(),
		waitCond: NewNodeCond(&c.mut),
	}

	c.runner.StartVoteRequestRunners(c.getCurrentTerm(), nil)
	c.runner.StartAcceptRequestRunners(c.getCurrentTerm(), nil)
	c.runner.StartFollowerRunner(true, c.getCurrentTerm())
	c.runner.SetLeader(c.getCurrentTerm(), false)

	return true
}

func (c *coreLogicImpl) HandleAcceptEntriesResponse(
	fromNode NodeID, output AcceptEntriesOutput,
) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		// TODO handle
		return false
	}

	if !c.isValidTerm(output.Term) {
		// TODO testing
		return false
	}

	for _, pos := range output.PosList {
		c.handleAcceptResponseForPos(fromNode, pos)
	}

	return true
}

func (c *coreLogicImpl) handleAcceptResponseForPos(id NodeID, pos LogPos) bool {
	if pos <= c.leader.lastCommitted {
		return false
	}

	memLog := c.leader.memLog

	maxPos := memLog.MaxLogPos()
	if pos > maxPos {
		// TODO testing
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

	c.increaseLastCommitted()

	return true
}

func (c *coreLogicImpl) increaseLastCommitted() {
	memLog := c.leader.memLog

	for memLog.GetQueueSize() > 0 {
		voted := memLog.GetFrontVoted()
		if !IsQuorum(c.leader.members, voted) {
			break
		}
		memLog.PopFront()
		c.broadcastAllAcceptors()
	}
}

func (c *coreLogicImpl) InsertCommand(term TermNum, cmdList ...[]byte) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateLeader {
		return false
	}

	if !c.isValidTerm(term) {
		return false
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

	return true
}

func (c *coreLogicImpl) appendNewEntry(pos LogPos, entry LogEntry) {
	c.leader.memLog.Put(pos, entry)
	c.broadcastAllAcceptors()
}

func (c *coreLogicImpl) CheckTimeout() {
	c.mut.Lock()
	defer c.mut.Unlock()

	now := c.nowFunc()

	if c.state == StateFollower {
		// TODO testing
		if c.follower.wakeUpAt <= now {
			c.follower.waitCond.Broadcast()
		}
		return
	}

	// for candidate & leader
	for nodeID, weakUpAt := range c.leader.acceptorWakeUpAt {
		if weakUpAt <= now {
			delete(c.leader.acceptorWakeUpAt, nodeID)
			c.leader.nodeCondVar.Signal(nodeID)
		}
	}
}

func (c *coreLogicImpl) isValidTerm(term TermNum) bool {
	return c.getCurrentTerm() == term
}

func (c *coreLogicImpl) broadcastAllAcceptors() {
	c.leader.nodeCondVar.Broadcast()
}

func (c *coreLogicImpl) ChangeMembership(term TermNum, newNodes []NodeID) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateLeader {
		// TODO testing
		return false
	}

	if !c.isValidTerm(term) {
		// TODO testing
		return false
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

	return true
}

func (c *coreLogicImpl) UpdateAcceptorFullyReplicated(
	term TermNum, nodeID NodeID, pos LogPos,
) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	return true
}

func (c *coreLogicImpl) GetReadyToStartElection(ctx context.Context, term TermNum) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

StartLoop:
	if c.state != StateFollower {
		return false
	}

	if c.follower.wakeUpAt > c.nowFunc() {
		if err := c.follower.waitCond.Wait(ctx, c.nodeID); err != nil {
			return false
		}
		goto StartLoop
	}

	return true
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
	return c.nowFunc() + 5_000 // TODO configurable
}

func (c *coreLogicImpl) getCurrentTerm() TermNum {
	return c.persistent.GetLastTerm()
}
