package paxos

import (
	"context"
	"sync"
)

type CoreLogic interface {
	StartElection()

	GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, bool)
	HandleVoteResponse(fromNode NodeID, output RequestVoteOutput) bool

	GetAcceptEntriesRequest(
		ctx context.Context, term TermNum, toNode NodeID,
		fromPos LogPos, lastCommittedSent LogPos,
	) (AcceptEntriesInput, bool)

	HandleAcceptEntriesResponse(fromNode NodeID, output AcceptEntriesOutput) bool

	InsertCommand(term TermNum, cmdDataList ...[]byte) bool

	CheckTimeout()

	ChangeMembership(term TermNum, newNodes []NodeID) bool

	UpdateAcceptorFullyReplicated(term TermNum, nodeID NodeID, pos LogPos) bool

	GetState() State
	GetLastCommitted() LogPos
}

func NewCoreLogic(
	persistent PersistentState,
	log LogStorage,
	runner NodeRunner,
	nowFunc func() TimestampMilli,
) CoreLogic {
	return &coreLogicImpl{
		state:   StateFollower,
		nowFunc: nowFunc,

		persistent: persistent,
		log:        log,
		runner:     runner,
	}
}

type coreLogicImpl struct {
	nowFunc func() TimestampMilli

	mut   sync.Mutex
	state State

	candidate *candidateStateInfo
	leader    *leaderStateInfo

	persistent PersistentState
	log        LogStorage
	runner     NodeRunner
}

type candidateStateInfo struct {
	remainPosMap map[NodeID]InfiniteLogPos
	acceptPos    LogPos
}

type leaderStateInfo struct {
	members       []MemberInfo
	lastCommitted LogPos
	proposeTerm   TermNum

	memLog *MemLog

	acceptorWakeUpAt map[NodeID]TimestampMilli
	nodeCondVar      *NodeCond

	acceptorFullyReplicated map[NodeID]LogPos
}

func (c *coreLogicImpl) StartElection() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateFollower {
		return
	}

	c.state = StateCandidate

	commitInfo := c.log.GetCommittedInfo()

	c.leader = &leaderStateInfo{
		members:       commitInfo.Members,
		lastCommitted: commitInfo.Pos,
		proposeTerm:   c.persistent.NextProposeTerm(),

		acceptorWakeUpAt: map[NodeID]TimestampMilli{},
		nodeCondVar:      NewNodeCond(&c.mut),

		acceptorFullyReplicated: map[NodeID]LogPos{},
	}

	c.leader.memLog = NewMemLog(&c.leader.lastCommitted, 10)

	allMembers := GetAllMembers(commitInfo.Members)

	remainPosMap := map[NodeID]InfiniteLogPos{}
	nextPos := commitInfo.Pos + 1
	for member := range allMembers {
		remainPosMap[member] = InfiniteLogPos{
			IsFinite: true,
			Pos:      nextPos,
		}
	}

	c.candidate = &candidateStateInfo{
		remainPosMap: remainPosMap,
		acceptPos:    commitInfo.Pos,
	}

	c.updateVoteRunners()
	c.updateAcceptRunners()
}

func (c *coreLogicImpl) updateVoteRunners() {
	if c.state == StateLeader {
		c.runner.StartVoteRequestRunners(c.leader.proposeTerm, nil)
		return
	}

	allMembers := GetAllMembers(c.leader.members)
	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			// if +infinity => remove from runnable voters
			delete(allMembers, nodeID)
		}
	}
	c.runner.StartVoteRequestRunners(c.leader.proposeTerm, allMembers)
}

func (c *coreLogicImpl) updateAcceptRunners() {
	allMembers := GetAllMembers(c.leader.members)
	c.runner.StartAcceptRequestRunners(c.leader.proposeTerm, allMembers)
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
		Term:    c.getLeaderTerm(),
		FromPos: remainPos.Pos,
	}, true
}

func (c *coreLogicImpl) HandleVoteResponse(id NodeID, output RequestVoteOutput) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		// TODO
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

	c.switchFromCandidateToLeader()

	return true
}

func (c *coreLogicImpl) handleVoteResponseEntry(
	id NodeID, entry VoteLogEntry,
) {
	remainPos := c.candidate.remainPosMap[id]
	if !remainPos.IsFinite {
		// TODO testing
		return
	}

	pos := entry.Pos

	if remainPos.Pos != pos {
		return
	}

	if pos <= c.candidate.acceptPos {
		return
	}

	c.candidatePutVoteEntry(id, entry)
	c.increaseAcceptPos()
}

func (c *coreLogicImpl) candidatePutVoteEntry(id NodeID, entry VoteLogEntry) {
	pos := entry.Pos

	if !entry.More {
		c.candidate.remainPosMap[id] = InfiniteLogPos{}
		c.updateVoteRunners()
		return
	}

	term := c.getLeaderTerm()

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
	needBroadcast := false
	for {
		ok := c.tryIncreaseAcceptPosAt(c.candidate.acceptPos + 1)
		if !ok {
			break
		}
		needBroadcast = true
	}

	if needBroadcast {
		c.broadcastAllAcceptors()
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
	logEntry.Term = c.getLeaderTerm().ToInf()
	c.leader.memLog.Put(pos, logEntry)

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

	c.leader.acceptorWakeUpAt[toNode] = c.nowFunc() + 5_000 // TODO configurable

	return AcceptEntriesInput{
		ToNode:    toNode,
		Term:      c.getLeaderTerm(),
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

func (c *coreLogicImpl) HandleAcceptEntriesResponse(
	fromNode NodeID, output AcceptEntriesOutput,
) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !output.Success {
		// TODO
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
		// TODO broadcast
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
			Term:    c.getLeaderTerm().ToInf(),
			CmdData: cmd,
		}
		c.appendNewEntry(pos, entry)
	}

	return true
}

func (c *coreLogicImpl) appendNewEntry(pos LogPos, entry LogEntry) {
	c.leader.memLog.Put(pos, entry)
	// TODO broadcast acceptors
}

func (c *coreLogicImpl) CheckTimeout() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if !c.isCandidateOrLeader() {
		// TODO testing
		return
	}

	now := c.nowFunc()
	for nodeID, weakUpAt := range c.leader.acceptorWakeUpAt {
		if weakUpAt <= now {
			delete(c.leader.acceptorWakeUpAt, nodeID)
			c.leader.nodeCondVar.Signal(nodeID)
		}
	}
}

func (c *coreLogicImpl) getLeaderTerm() TermNum {
	return c.leader.proposeTerm
}

func (c *coreLogicImpl) isValidTerm(term TermNum) bool {
	return c.leader.proposeTerm == term
}

func (c *coreLogicImpl) broadcastAllAcceptors() {
	c.leader.nodeCondVar.Broadcast()
}

func (c *coreLogicImpl) ChangeMembership(term TermNum, newNodes []NodeID) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateLeader {
		return false
	}

	if !c.isValidTerm(term) {
		return false
	}

	pos := c.leader.memLog.MaxLogPos() + 1
	c.leader.members = append(c.leader.members, MemberInfo{
		Nodes:     newNodes,
		CreatedAt: pos,
	})

	entry := LogEntry{
		Type:    LogTypeMembership,
		Term:    c.leader.proposeTerm.ToInf(),
		Members: c.leader.members,
	}
	c.appendNewEntry(pos, entry)
	c.updateAcceptRunners()

	return true
}

func (c *coreLogicImpl) UpdateAcceptorFullyReplicated(
	term TermNum, nodeID NodeID, pos LogPos,
) bool {
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
