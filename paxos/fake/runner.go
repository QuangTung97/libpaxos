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

	LeaderTerm paxos.TermNum
	IsLeader   bool

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
) {
	r.VoteTerm = term
	r.VoteRunners = nodeSetToSlice(nodes)
}

func (r *NodeRunnerFake) StartAcceptRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) {
	r.AcceptTerm = term
	r.AcceptRunners = nodeSetToSlice(nodes)
}

func (r *NodeRunnerFake) SetLeader(term paxos.TermNum, isLeader bool) {
	r.LeaderTerm = term
	r.IsLeader = isLeader
}

func (r *NodeRunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{}, retryCount int,
) {
	r.FetchFollowerTerm = term
	r.FetchFollowers = nodeSetToSlice(nodes)
	r.FetchRetryCount = retryCount
}

func (r *NodeRunnerFake) StartElectionRunner(
	termValue paxos.TermValue, started bool, chosen paxos.NodeID, retryCount int,
) {
	r.ElectionTerm = termValue
	r.ElectionStarted = started
	r.ElectionChosen = chosen
	r.ElectionRetryCount = retryCount
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
