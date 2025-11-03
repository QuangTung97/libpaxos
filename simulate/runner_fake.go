package simulate

import "github.com/QuangTung97/libpaxos/paxos"

type RunnerFake struct {
}

var _ paxos.NodeRunner = &RunnerFake{}

func NewRunnerFake() *RunnerFake {
	return &RunnerFake{}
}

func (r *RunnerFake) StartVoteRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	return false
}

func (r *RunnerFake) StartAcceptRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	return false
}

func (r *RunnerFake) StartStateMachine(
	term paxos.TermNum, info paxos.StateMachineRunnerInfo,
) bool {
	return false
}

func (r *RunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{}, retryCount int,
) bool {
	return false
}

func (r *RunnerFake) StartElectionRunner(
	termValue paxos.TermValue, started bool, chosen paxos.NodeID, retryCount int,
) bool {
	return false
}
