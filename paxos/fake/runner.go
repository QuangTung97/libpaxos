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

	FollowerTerm    paxos.TermNum
	FollowerRunning bool
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

func (r *NodeRunnerFake) StartFollowerRunner(term paxos.TermNum, running bool) {
	r.FollowerRunning = running
	r.FollowerTerm = term
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
