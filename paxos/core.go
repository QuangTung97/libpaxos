package paxos

import "sync"

type CoreLogic interface {
	StartElection()

	GetVoteRequests(toNode NodeID, limit int) RequestVoteInput
}

func NewCoreLogic() CoreLogic {
	return &coreLogicImpl{
		state: StateFollower,
	}
}

type coreLogicImpl struct {
	mut   sync.Mutex
	state State

	candidate *candidateState
	leader    *leaderState

	persistent PersistentState
	log        ReplicatedLog
}

type candidateState struct {
	remainPosMap map[NodeID]InfiniteLogPos
	acceptPos    LogPos
}

type leaderState struct {
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

	c.leader = &leaderState{
		members:       commitInfo.Members,
		lastCommitted: commitInfo.Pos,
		proposeTerm:   c.persistent.NextProposeTerm(),

		// TODO acceptorCommitted
	}

	nextPos := commitInfo.Pos + 1
	allMembers := GetAllMembers(commitInfo.Members, nextPos)

	remainPosMap := map[NodeID]InfiniteLogPos{}
	for _, member := range allMembers {
		remainPosMap[member] = InfiniteLogPos{
			IsFinite: true,
			Pos:      nextPos,
		}
	}

	c.candidate = &candidateState{
		remainPosMap: remainPosMap,
		acceptPos:    commitInfo.Pos,
	}
}

func (c *coreLogicImpl) GetVoteRequests(toNode NodeID, limit int) RequestVoteInput {
	return RequestVoteInput{}
}
