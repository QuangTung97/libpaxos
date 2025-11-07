package fake

import (
	"slices"

	"github.com/QuangTung97/libpaxos/paxos"
)

type NodeRunnerFake struct {
	VoteTerm    paxos.TermNum
	VoteRunners []paxos.NodeID

	AcceptTerm    paxos.TermNum
	AcceptRunners []paxos.NodeID

	StateMachineTerm paxos.TermNum
	StateMachineInfo paxos.StateMachineRunnerInfo

	FetchFollowerTerm paxos.TermNum
	FetchFollowers    []paxos.NodeID
	FetchFollowerGen  paxos.FollowerGeneration

	ElectionInfo paxos.ElectionRunnerInfo
}

var _ paxos.NodeRunner = &NodeRunnerFake{}

func (r *NodeRunnerFake) StartVoteRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var updated bool

	newNodes := nodeSetToSlice(nodes)
	if !slices.Equal(r.VoteRunners, newNodes) {
		updated = true
	}

	if len(newNodes) > 0 && r.VoteTerm != term {
		updated = true
	}

	r.VoteTerm = term
	r.VoteRunners = newNodes

	return updated
}

func (r *NodeRunnerFake) StartAcceptRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var updated bool

	newNodes := nodeSetToSlice(nodes)
	if !slices.Equal(r.AcceptRunners, newNodes) {
		updated = true
	}

	if len(newNodes) > 0 && r.AcceptTerm != term {
		updated = true
	}

	r.AcceptTerm = term
	r.AcceptRunners = newNodes

	return updated
}

func (r *NodeRunnerFake) StartStateMachine(term paxos.TermNum, info paxos.StateMachineRunnerInfo) bool {
	var updated bool
	if r.StateMachineInfo != info {
		updated = true
	}
	if info.Running && r.StateMachineTerm != term {
		updated = true
	}

	r.StateMachineTerm = term
	r.StateMachineInfo = info

	return updated
}

func (r *NodeRunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, generation paxos.FollowerGeneration, nodes map[paxos.NodeID]struct{},
) bool {
	var updated bool

	newNodes := nodeSetToSlice(nodes)
	if !slices.Equal(r.FetchFollowers, newNodes) {
		updated = true
	}

	if len(newNodes) > 0 {
		if r.FetchFollowerTerm != term {
			updated = true
		}
		if r.FetchFollowerGen != generation {
			updated = true
		}
	}

	r.FetchFollowerTerm = term
	r.FetchFollowers = newNodes
	r.FetchFollowerGen = generation

	return updated
}

func (r *NodeRunnerFake) StartElectionRunner(newInfo paxos.ElectionRunnerInfo) bool {
	if r.ElectionInfo == newInfo {
		return false
	}

	r.ElectionInfo = newInfo
	return true
}

func nodeSetToSlice(nodes map[paxos.NodeID]struct{}) []paxos.NodeID {
	result := make([]paxos.NodeID, 0, len(nodes))
	for id := range nodes {
		result = append(result, id)
	}
	slices.SortFunc(result, func(a, b paxos.NodeID) int {
		return slices.Compare(a[:], b[:])
	})
	return result
}
