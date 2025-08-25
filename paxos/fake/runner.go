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
	FetchRetryCount   int

	ElectionTerm       paxos.TermValue
	ElectionStarted    bool
	ElectionChosen     paxos.NodeID
	ElectionRetryCount int
}

var _ paxos.NodeRunner = &NodeRunnerFake{}

func (r *NodeRunnerFake) StartVoteRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var updated bool

	if r.VoteTerm != term {
		updated = true
		r.VoteTerm = term
	}

	newNodes := nodeSetToSlice(nodes)
	if !slices.Equal(r.VoteRunners, newNodes) {
		updated = true
		r.VoteRunners = newNodes
	}

	return updated
}

func (r *NodeRunnerFake) StartAcceptRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var updated bool

	if r.AcceptTerm != term {
		updated = true
		r.AcceptTerm = term
	}

	newNodes := nodeSetToSlice(nodes)
	if !slices.Equal(r.AcceptRunners, newNodes) {
		updated = true
		r.AcceptRunners = newNodes
	}

	return updated
}

func (r *NodeRunnerFake) StartStateMachine(term paxos.TermNum, info paxos.StateMachineRunnerInfo) bool {
	var updated bool

	if r.StateMachineTerm != term {
		updated = true
		r.StateMachineTerm = term
	}

	if r.StateMachineInfo != info {
		updated = true
		r.StateMachineInfo = info
	}

	return updated
}

func (r *NodeRunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{}, retryCount int,
) bool {
	var updated bool

	if r.FetchFollowerTerm != term {
		updated = true
		r.FetchFollowerTerm = term
	}

	newNodes := nodeSetToSlice(nodes)
	if !slices.Equal(r.FetchFollowers, newNodes) {
		updated = true
		r.FetchFollowers = newNodes
	}

	if r.FetchRetryCount != retryCount {
		updated = true
		r.FetchRetryCount = retryCount
	}

	return updated
}

func (r *NodeRunnerFake) StartElectionRunner(
	termValue paxos.TermValue, started bool, chosen paxos.NodeID, retryCount int,
) bool {
	var updated bool

	if r.ElectionTerm != termValue {
		updated = true
		r.ElectionTerm = termValue
	}

	if r.ElectionStarted != started {
		updated = true
		r.ElectionStarted = started
	}

	if r.ElectionChosen != chosen {
		updated = true
		r.ElectionChosen = chosen
	}

	if r.ElectionRetryCount != retryCount {
		updated = true
		r.ElectionRetryCount = retryCount
	}

	return updated
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
