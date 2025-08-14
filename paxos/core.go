package paxos

import (
	"context"
	"sync"
)

type CoreLogic interface {
	StartElection()

	GetVoteRequest(toNode NodeID) (RequestVoteInput, bool)
	HandleVoteResponse(fromNode NodeID, output RequestVoteOutput) bool

	GetAcceptEntriesRequest(ctx context.Context, toNode NodeID) (AcceptEntriesInput, bool)

	InsertCommand(cmdDataList ...[]byte) bool

	GetState() State
}

func NewCoreLogic(
	persistent PersistentState,
	log LogStorage,
	runner NodeRunner,
) CoreLogic {
	return &coreLogicImpl{
		state: StateFollower,

		persistent: persistent,
		log:        log,
		runner:     runner,
	}
}

type coreLogicImpl struct {
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

	acceptorCommitted map[NodeID]LogPos
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

func (c *coreLogicImpl) GetVoteRequest(toNode NodeID) (RequestVoteInput, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateCandidate {
		return RequestVoteInput{}, false
	}

	term := c.leader.proposeTerm
	remainPos, ok := c.candidate.remainPosMap[toNode]
	if !ok {
		return RequestVoteInput{}, false
	}

	if !remainPos.IsFinite {
		return RequestVoteInput{}, false
	}

	return RequestVoteInput{
		ToNode:  toNode,
		Term:    term,
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

	if c.leader.proposeTerm != output.Term {
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

	term := c.leader.proposeTerm

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
	logEntry.Term = c.leader.proposeTerm.ToInf()
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

	if IsQuorum(c.leader.members, infiniteSet, c.candidate.acceptPos+1) {
		c.state = StateLeader
		c.candidate = nil
	}
}

func (c *coreLogicImpl) GetAcceptEntriesRequest(
	_ context.Context, toNode NodeID,
) (AcceptEntriesInput, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	checkStateOK := func() bool {
		if c.state == StateCandidate {
			return true
		}
		if c.state == StateLeader {
			return true
		}
		return false
	}

	if !checkStateOK() {
		return AcceptEntriesInput{}, false
	}

	maxLogPos := c.leader.memLog.MaxLogPos()
	if c.state == StateCandidate {
		maxLogPos = c.candidate.acceptPos
	}

	// TODO wait on condition: maxLogPos > lastCommitted

	var acceptEntries []AcceptLogEntry
	for pos := c.leader.lastCommitted + 1; pos <= maxLogPos; pos++ {
		acceptEntries = append(acceptEntries, AcceptLogEntry{
			Pos:   pos,
			Entry: c.leader.memLog.Get(pos),
		})
	}

	return AcceptEntriesInput{
		ToNode:  toNode,
		Term:    c.leader.proposeTerm,
		Entries: acceptEntries,
	}, true
}

func (c *coreLogicImpl) InsertCommand(cmdList ...[]byte) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.state != StateLeader {
		return false
	}

	for _, cmd := range cmdList {
		maxPos := c.leader.memLog.MaxLogPos()
		pos := maxPos + 1
		c.leader.memLog.Put(pos, LogEntry{
			Type:    LogTypeCmd,
			Term:    c.leader.proposeTerm.ToInf(),
			CmdData: cmd,
		})
	}

	return true
}

func (c *coreLogicImpl) GetState() State {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.state
}
