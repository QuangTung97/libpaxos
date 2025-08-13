package paxos_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

var nodeID1 = fake.NewNodeID(1)
var nodeID2 = fake.NewNodeID(2)
var nodeID3 = fake.NewNodeID(3)

type coreLogicTest struct {
	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *fake.NodeRunnerFake

	core CoreLogic
}

func newCoreLogicTest() *coreLogicTest {
	c := &coreLogicTest{}

	c.persistent = &fake.PersistentStateFake{
		NodeID:    nodeID1,
		LastValue: 20,
	}
	c.log = &fake.LogStorageFake{}
	c.runner = &fake.NodeRunnerFake{}

	// setup init members
	initEntry := LogEntry{
		Type: LogTypeMembership,
		Term: InfiniteTerm{},
		Members: []MemberInfo{
			{
				Nodes:      []NodeID{nodeID1, nodeID2, nodeID3},
				ActiveFrom: 2,
			},
		},
	}
	c.log.UpsertEntries([]PosLogEntry{
		{
			Pos:   1,
			Entry: initEntry,
		},
	})

	c.core = NewCoreLogic(
		c.persistent,
		c.log,
		c.runner,
	)
	return c
}

func TestCoreLogic_StartElection__Then_GetRequestVote(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	// check runners
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)

	// get vote request
	voteReq, ok := c.core.GetVoteRequest(nodeID2)
	assert.Equal(t, true, ok)
	assert.Equal(t, RequestVoteInput{
		Term: TermNum{
			Num:    21,
			NodeID: nodeID1,
		},
		ToNode:  nodeID2,
		FromPos: 2,
	}, voteReq)

	// get vote request for leader node
	voteReq, ok = c.core.GetVoteRequest(nodeID1)
	assert.Equal(t, true, ok)
	assert.Equal(t, RequestVoteInput{
		Term: TermNum{
			Num:    21,
			NodeID: nodeID1,
		},
		ToNode:  nodeID1,
		FromPos: 2,
	}, voteReq)
}
