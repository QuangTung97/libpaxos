package paxos

import "sync"

type CoreLogic interface {
	StartElection()

	GetVoteRequest(toNode NodeID) (RequestVoteInput, bool)
	HandleVoteResponse(fromNode NodeID, output RequestVoteOutput) bool
}

func NewCoreLogic() CoreLogic {
	return &coreLogicImpl{
		state: StateFollower,
	}
}

type coreLogicImpl struct {
	mut   sync.Mutex
	state State

	candidate *candidateStateInfo
	leader    *leaderStateInfo

	persistent PersistentState
	log        ReplicatedLog
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

	memLog   []LogEntry
	logVoted [][]NodeID

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

	if entry.More {
		c.candidate.remainPosMap[id] = InfiniteLogPos{
			IsFinite: true,
			Pos:      pos + 1,
		}
	} else {
		c.candidate.remainPosMap[id] = InfiniteLogPos{}
	}

	c.increaseAcceptPos(pos)
}

func (c *coreLogicImpl) increaseAcceptPos(pos LogPos) bool {
	memPos := c.computeMemPos(pos)
	if memPos > c.getMemLogLen() {
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
		c.candidate.acceptPos = pos
		logEntry := c.getMemLogEntry(memPos)
		logEntry.Term = InfiniteTerm{
			IsFinite: true,
			Term:     c.leader.proposeTerm,
		}
		return false
	}

	return true
}

func (c *coreLogicImpl) getMemLogLen() MemLogPos {
	return MemLogPos(len(c.leader.memLog))
}

func (c *coreLogicImpl) getMemLogEntry(memPos MemLogPos) *LogEntry {
	return &c.leader.memLog[memPos-1]
}

func (c *coreLogicImpl) computeMemPos(pos LogPos) MemLogPos {
	return MemLogPos(pos - c.leader.lastCommitted)
}
