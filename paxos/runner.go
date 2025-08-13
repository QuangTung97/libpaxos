package paxos

type NodeRunner interface {
	StartVoteRequestRunners(nodes map[NodeID]struct{})
}
