package paxos

type NodeRunner interface {
	StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{})

	StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{})

	SetLeader(term TermNum, isLeader bool)

	StartFollowerRunner(term TermNum, isRunning bool)
}
