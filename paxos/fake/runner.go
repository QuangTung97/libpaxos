package fake

import (
	"slices"

	"github.com/QuangTung97/libpaxos/paxos"
)

type NodeRunnerFake struct {
	VoteRunners   []paxos.NodeID
	AcceptRunners []paxos.NodeID
}

var _ paxos.NodeRunner = &NodeRunnerFake{}

func (r *NodeRunnerFake) StartVoteRequestRunners(nodes map[paxos.NodeID]struct{}) {
	r.VoteRunners = nodeSetToSlice(nodes)
}

func (r *NodeRunnerFake) StartAcceptRequestRunners(nodes map[paxos.NodeID]struct{}) {
	r.AcceptRunners = nodeSetToSlice(nodes)
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
