package fake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
)

var (
	nodeID1 = NewNodeID(1)
	nodeID2 = NewNodeID(2)
	nodeID3 = NewNodeID(3)
)

func TestNodeRunnerFake_StartVoteRequestRunners(t *testing.T) {
	runner := &NodeRunnerFake{}

	term := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	nodes := map[NodeID]struct{}{
		nodeID1: {},
		nodeID2: {},
		nodeID3: {},
	}

	assert.Equal(t, true, runner.StartVoteRequestRunners(term, nodes))
	assert.Equal(t, false, runner.StartVoteRequestRunners(term, nodes))
	assert.Equal(t, term, runner.VoteTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.VoteRunners)

	// stop
	assert.Equal(t, true, runner.StartVoteRequestRunners(term, nil))
	assert.Equal(t, term, runner.VoteTerm)
	assert.Equal(t, []NodeID{}, runner.VoteRunners)

	assert.Equal(t, false, runner.StartVoteRequestRunners(TermNum{}, nil))

	// start again
	assert.Equal(t, true, runner.StartVoteRequestRunners(term, nodes))
}

func TestNodeRunnerFake_StartAcceptRequestRunners(t *testing.T) {
	runner := &NodeRunnerFake{}

	term := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	nodes := map[NodeID]struct{}{
		nodeID1: {},
		nodeID2: {},
		nodeID3: {},
	}

	assert.Equal(t, true, runner.StartAcceptRequestRunners(term, nodes))
	assert.Equal(t, term, runner.AcceptTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.AcceptRunners)

	assert.Equal(t, false, runner.StartAcceptRequestRunners(term, nodes))

	// stop
	assert.Equal(t, true, runner.StartAcceptRequestRunners(term, nil))
	assert.Equal(t, term, runner.AcceptTerm)
	assert.Equal(t, []NodeID{}, runner.AcceptRunners)

	// stop again
	assert.Equal(t, false, runner.StartAcceptRequestRunners(TermNum{}, nil))
}

func TestNodeRunnerFake_StartStateMachine(t *testing.T) {
	runner := &NodeRunnerFake{}

	term := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	info := StateMachineRunnerInfo{
		Running: true,
	}
	assert.Equal(t, true, runner.StartStateMachine(term, info))
	assert.Equal(t, term, runner.StateMachineTerm)
	assert.Equal(t, info, runner.StateMachineInfo)

	assert.Equal(t, false, runner.StartStateMachine(term, info))

	// update
	info.IsLeader = true
	info.AcceptCommand = true
	assert.Equal(t, true, runner.StartStateMachine(term, info))
	assert.Equal(t, term, runner.StateMachineTerm)
	assert.Equal(t, info, runner.StateMachineInfo)

	// stop
	info.Running = false
	assert.Equal(t, true, runner.StartStateMachine(term, info))
	assert.Equal(t, false, runner.StartStateMachine(TermNum{}, info))
}

func TestNodeRunnerFake_StartFetchingFollowerInfoRunners(t *testing.T) {
	runner := &NodeRunnerFake{}

	term := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	nodes := map[NodeID]struct{}{
		nodeID1: {},
		nodeID2: {},
		nodeID3: {},
	}

	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(term, nodes, 1))
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.FetchFollowers)
	assert.Equal(t, term, runner.FetchFollowerTerm)
	assert.Equal(t, 1, runner.FetchRetryCount)

	// increase retry
	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(term, nodes, 2))
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.FetchFollowers)
	assert.Equal(t, term, runner.FetchFollowerTerm)
	assert.Equal(t, 2, runner.FetchRetryCount)

	assert.Equal(t, false, runner.StartFetchingFollowerInfoRunners(term, nodes, 2))

	// stop
	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(term, nil, 3))
	assert.Equal(t, []NodeID{}, runner.FetchFollowers)
	assert.Equal(t, term, runner.FetchFollowerTerm)
	assert.Equal(t, 3, runner.FetchRetryCount)

	assert.Equal(t, false, runner.StartFetchingFollowerInfoRunners(TermNum{}, nil, 3))
}

func TestNodeRunnerFake_StartElectionRunner(t *testing.T) {
	runner := &NodeRunnerFake{}

	assert.Equal(t, true, runner.StartElectionRunner(21, true, nodeID1, 3))
	assert.Equal(t, true, runner.ElectionStarted)
	assert.Equal(t, TermValue(21), runner.ElectionTerm)
	assert.Equal(t, nodeID1, runner.ElectionChosen)
	assert.Equal(t, 3, runner.ElectionRetryCount)

	assert.Equal(t, false, runner.StartElectionRunner(21, true, nodeID1, 3))

	// stop
	assert.Equal(t, true, runner.StartElectionRunner(21, false, nodeID1, 3))
	assert.Equal(t, false, runner.ElectionStarted)
	assert.Equal(t, TermValue(21), runner.ElectionTerm)
	assert.Equal(t, nodeID1, runner.ElectionChosen)
	assert.Equal(t, 3, runner.ElectionRetryCount)

	assert.Equal(t, false, runner.StartElectionRunner(0, false, NodeID{}, 0))
}
