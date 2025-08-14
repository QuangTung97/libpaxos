package paxos_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

var nodeID1 = fake.NewNodeID(1)
var nodeID2 = fake.NewNodeID(2)
var nodeID3 = fake.NewNodeID(3)

type coreLogicTest struct {
	ctx context.Context

	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *fake.NodeRunnerFake

	core CoreLogic

	currentTerm TermNum
}

func newCoreLogicTest() *coreLogicTest {
	c := &coreLogicTest{}
	c.ctx = context.Background()

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

func TestCoreLogic_StartElection__Then_HandleVoteResponse(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	voteOutput := RequestVoteOutput{
		Success: true,
		Term: TermNum{
			Num:    21,
			NodeID: nodeID1,
		},
		Entries: []VoteLogEntry{
			{
				Pos:  2,
				More: false,
			},
		},
	}

	// handle vote response
	c.core.HandleVoteResponse(nodeID1, voteOutput)

	assert.Equal(t, false, c.core.InsertCommand([]byte("cmd 01")))

	// switch to leader state
	c.core.HandleVoteResponse(nodeID2, voteOutput)

	assert.Equal(t, true, c.core.InsertCommand([]byte("cmd 01")))

	// do nothing
	c.core.HandleVoteResponse(nodeID3, voteOutput)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Entries__To_Leader(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	currentTerm := TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	entry1 := LogEntry{
		Type: LogTypeCmd,
		Term: TermNum{
			Num:    19,
			NodeID: nodeID3,
		}.ToInf(),
		CmdData: []byte("Cmd Data 01"),
	}

	voteOutput1 := RequestVoteOutput{
		Success: true,
		Term:    currentTerm,
		Entries: []VoteLogEntry{
			{
				Pos:   2,
				More:  true,
				Entry: entry1,
			},
			{
				Pos:  3,
				More: false,
			},
		},
	}
	voteOutput2 := RequestVoteOutput{
		Success: true,
		Term:    currentTerm,
		Entries: []VoteLogEntry{
			{
				Pos:  2,
				More: false,
			},
		},
	}

	// handle vote 1
	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// handle vote 2
	c.core.HandleVoteResponse(nodeID2, voteOutput2)
	assert.Equal(t, StateLeader, c.core.GetState())

	// get accept req
	entry1.Term.Term = currentTerm

	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID1)
	assert.Equal(t, true, ok)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos:   2,
				Entry: entry1,
			},
		},
	}, acceptReq)
}

func (c *coreLogicTest) startAsLeader() {
	c.core.StartElection()

	c.currentTerm = TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	voteOutput := RequestVoteOutput{
		Success: true,
		Term:    c.currentTerm,
		Entries: []VoteLogEntry{
			{
				Pos:  2,
				More: false,
			},
		},
	}
	c.core.HandleVoteResponse(nodeID1, voteOutput)
	c.core.HandleVoteResponse(nodeID2, voteOutput)
}

func TestCoreLogic__Insert_Cmd__Then_Get_Accept_Request(t *testing.T) {
	c := newCoreLogicTest()

	c.startAsLeader()

	// insert 2 commands
	assert.Equal(t, true, c.core.InsertCommand([]byte("cmd 01")))
	assert.Equal(t, true, c.core.InsertCommand([]byte("cmd 02")))

	req, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID2)
	assert.Equal(t, true, ok)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID2,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos: 2,
				Entry: LogEntry{
					Type:    LogTypeCmd,
					Term:    c.currentTerm.ToInf(),
					CmdData: []byte("cmd 01"),
				},
			},
			{
				Pos: 3,
				Entry: LogEntry{
					Type:    LogTypeCmd,
					Term:    c.currentTerm.ToInf(),
					CmdData: []byte("cmd 02"),
				},
			},
		},
	}, req)
}
