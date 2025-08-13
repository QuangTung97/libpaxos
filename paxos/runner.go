package paxos

type NodeRunner interface {
	// StartVoteRequestRunners for nodes after leader.acceptPos
	StartVoteRequestRunners(nodes map[NodeID]struct{})

	// StartAcceptRequestRunners for nodes after leader.lastCommitted
	StartAcceptRequestRunners(nodes map[NodeID]struct{})
}
