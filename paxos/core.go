package paxos

import (
	"context"
	"sync"

	"github.com/QuangTung97/libpaxos/paxos/cond"
)

type CoreLogic interface {
	StartElection()

	GetVoteRequest(term TermNum, toNode NodeID) (RequestVoteInput, bool)
	HandleVoteResponse(fromNode NodeID, output RequestVoteOutput) bool

	GetAcceptEntriesRequest(
		ctx context.Context, toNode NodeID,
		fromPos LogPos, lastCommittedSent LogPos,
	) (AcceptEntriesInput, bool)

	HandleAcceptEntriesResponse(fromNode NodeID, output AcceptEntriesOutput) bool

	InsertCommand(term TermNum, cmdDataList ...[]byte) bool

	CheckTimeout()

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

	c.getAcceptCond = cond.NewCond(&c.mut)

	return c
}

type coreLogicImpl struct {
	nowFunc func() TimestampMilli

	mut   sync.Mutex
	state State

	getAcceptCond *cond.Cond

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

	acceptorCommitted map[NodeID]LogPos
	acceptorWakeUpAt  map[NodeID]TimestampMilli
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

		// TODO acceptorCommitted
	}

	c.leader.memLog = NewMemLog(&c.leader.lastCommitted, 10)

	nextPos := commitInfo.Pos + 1
	allMembers := GetAllMembers(commitInfo.Members, nextPos)

	remainPosMap := map[NodeID]InfiniteLogPos{}
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

	c.runner.StartVoteRequestRunners(allMembers)
	c.runner.StartAcceptRequestRunners(allMembers)
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
	c.increaseAcceptPos(pos)
}

func (c *coreLogicImpl) candidatePutVoteEntry(id NodeID, entry VoteLogEntry) {
	pos := entry.Pos

	if !entry.More {
		c.candidate.remainPosMap[id] = InfiniteLogPos{}
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

func (c *coreLogicImpl) increaseAcceptPos(pos LogPos) bool {
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

	if !IsQuorum(c.leader.members, remainOkSet, pos) {
		return false
	}

	c.candidate.acceptPos = pos
	logEntry := c.leader.memLog.Get(pos)
	logEntry.Term = c.getLeaderTerm().ToInf()
	c.leader.memLog.Put(pos, logEntry)

	return true
}

func (c *coreLogicImpl) switchFromCandidateToLeader() {
	infiniteSet := map[NodeID]struct{}{}
	for nodeID, remainPos := range c.candidate.remainPosMap {
		if !remainPos.IsFinite {
			infiniteSet[nodeID] = struct{}{}
		}
	}

	if !IsQuorum(c.leader.members, infiniteSet, c.candidate.acceptPos+1) {
		return
	}

	c.state = StateLeader
	c.candidate = nil
	c.runner.StartVoteRequestRunners(nil)
}

func (c *coreLogicImpl) GetAcceptEntriesRequest(
	ctx context.Context, toNode NodeID,
	fromPos LogPos, lastCommittedSent LogPos,
) (AcceptEntriesInput, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

StartFunction:
	if !c.isCandidateOrLeader() {
		// TODO testing
		return AcceptEntriesInput{}, false
	}

	maxLogPos := c.leader.memLog.MaxLogPos()
	if c.state == StateCandidate {
		maxLogPos = c.candidate.acceptPos
	}

	afterCommit := c.leader.lastCommitted + 1
	if fromPos < afterCommit {
		fromPos = afterCommit
	}

	if fromPos > maxLogPos && c.leader.lastCommitted <= lastCommittedSent {
		if err := c.getAcceptCond.Wait(ctx); err != nil {
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

	if IsQuorum(c.leader.members, voted, pos) {
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
		pos, voted := memLog.GetFrontVoted()
		if !IsQuorum(c.leader.members, voted, pos) {
			break
		}
		memLog.PopFront()
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
		c.leader.memLog.Put(pos, LogEntry{
			Type:    LogTypeCmd,
			Term:    c.getLeaderTerm().ToInf(),
			CmdData: cmd,
		})
	}

	return true
}

func (c *coreLogicImpl) CheckTimeout() {
}

func (c *coreLogicImpl) GetState() State {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.state
}

func (c *coreLogicImpl) getLeaderTerm() TermNum {
	return c.leader.proposeTerm
}

func (c *coreLogicImpl) isValidTerm(term TermNum) bool {
	return c.leader.proposeTerm == term
}

func (c *coreLogicImpl) GetLastCommitted() LogPos {
	return c.leader.lastCommitted
}
