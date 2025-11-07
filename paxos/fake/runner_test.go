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

	// start
	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(term, 2, nodes))
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.FetchFollowers)
	assert.Equal(t, FollowerGeneration(2), runner.FetchFollowerGen)
	assert.Equal(t, term, runner.FetchFollowerTerm)

	// new term
	newTerm := term
	newTerm.Num++
	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(newTerm, 2, nodes))
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.FetchFollowers)
	assert.Equal(t, newTerm, runner.FetchFollowerTerm)

	assert.Equal(t, false, runner.StartFetchingFollowerInfoRunners(newTerm, 2, nodes))

	// start with new generation
	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(term, 3, nodes))
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, runner.FetchFollowers)
	assert.Equal(t, FollowerGeneration(3), runner.FetchFollowerGen)
	assert.Equal(t, term, runner.FetchFollowerTerm)

	// stop
	assert.Equal(t, true, runner.StartFetchingFollowerInfoRunners(newTerm, 0, nil))
	assert.Equal(t, []NodeID{}, runner.FetchFollowers)
	assert.Equal(t, newTerm, runner.FetchFollowerTerm)
	assert.Equal(t, false, runner.StartFetchingFollowerInfoRunners(newTerm, 0, nil))
}

func TestNodeRunnerFake_StartElectionRunner(t *testing.T) {
	runner := &NodeRunnerFake{}

	term := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	info := ElectionRunnerInfo{
		Term:         term,
		Started:      true,
		MaxTermValue: 22,
		Chosen:       nodeID1,
	}
	assert.Equal(t, true, runner.StartElectionRunner(info))
	assert.Equal(t, info, runner.ElectionInfo)

	// start again
	assert.Equal(t, false, runner.StartElectionRunner(info))

	// stop
	assert.Equal(t, true, runner.StartElectionRunner(ElectionRunnerInfo{Term: term}))
	assert.Equal(t, ElectionRunnerInfo{
		Term: term,
	}, runner.ElectionInfo)

	// stop again
	assert.Equal(t, false, runner.StartElectionRunner(ElectionRunnerInfo{Term: term}))
}
