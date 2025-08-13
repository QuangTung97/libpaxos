package paxos

import "sync"

type CoreLogic interface {
}

func NewCoreLogic() CoreLogic {
	return &coreLogicImpl{
		state: StateFollower,
	}
}

type coreLogicImpl struct {
	mut       sync.Mutex
	state     State
	candidate *candidateState
	leader    *leaderState
}

type candidateState struct {
	remainPosMap map[NodeID]InfiniteLogPos
	acceptPosMap map[NodeID]LogPos
}

type leaderState struct {
}
