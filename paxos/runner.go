package paxos

type NodeRunner interface {
	// StartVoteRequestRunners for nodes after leader.acceptPos
	StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{})

	// StartAcceptRequestRunners for nodes after leader.lastCommitted
	StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{})

	SetLeader(term TermNum, isLeader bool)
}
