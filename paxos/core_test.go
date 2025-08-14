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
	ctx       context.Context
	cancelCtx context.Context

	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *fake.NodeRunnerFake

	core CoreLogic

	currentTerm TermNum
}

func newCoreLogicTest() *coreLogicTest {
	c := &coreLogicTest{}
	c.ctx = context.Background()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	c.cancelCtx = cancelCtx

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

	// setup current term
	c.currentTerm = TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	c.core = NewCoreLogic(
		c.persistent,
		c.log,
		c.runner,
	)
	return c
}

func (c *coreLogicTest) newLogEntry(cmdStr string, termNum TermValue) LogEntry {
	return LogEntry{
		Type: LogTypeCmd,
		Term: TermNum{
			Num:    termNum,
			NodeID: nodeID3,
		}.ToInf(),
		CmdData: []byte(cmdStr),
	}
}

func (c *coreLogicTest) newVoteOutput(
	fromPos LogPos, withFinal bool, entries ...LogEntry,
) RequestVoteOutput {
	voteEntries := make([]VoteLogEntry, 0, len(entries)+1)
	for index, e := range entries {
		voteEntries = append(voteEntries, VoteLogEntry{
			Pos:   fromPos + LogPos(index),
			More:  true,
			Entry: e,
		})
	}

	if withFinal {
		voteEntries = append(voteEntries, VoteLogEntry{
			Pos:  fromPos + LogPos(len(entries)),
			More: false,
		})
	}

	return RequestVoteOutput{
		Success: true,
		Term:    c.currentTerm,
		Entries: voteEntries,
	}
}

func (c *coreLogicTest) startAsLeader() {
	c.core.StartElection()

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

func (c *coreLogicTest) doInsertCmd(cmdList ...string) {
	cmdListBytes := make([][]byte, 0, len(cmdList))
	for _, cmd := range cmdList {
		cmdListBytes = append(cmdListBytes, []byte(cmd))
	}
	if ok := c.core.InsertCommand(c.currentTerm, cmdListBytes...); !ok {
		panic("can not insert")
	}
}

func TestCoreLogic_StartElection__Then_GetRequestVote(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	// check runners
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)

	// get vote request
	voteReq, ok := c.core.GetVoteRequest(c.currentTerm, nodeID2)
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
	voteReq, ok = c.core.GetVoteRequest(c.currentTerm, nodeID1)
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

	assert.Equal(t, false, c.core.InsertCommand(c.currentTerm, []byte("cmd 01")))

	// switch to leader state
	c.core.HandleVoteResponse(nodeID2, voteOutput)

	assert.Equal(t, true, c.core.InsertCommand(c.currentTerm, []byte("cmd 01")))

	// do nothing
	c.core.HandleVoteResponse(nodeID3, voteOutput)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Entries__To_Leader(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	entry1 := c.newLogEntry("cmd test 01", 19)
	voteOutput1 := c.newVoteOutput(2, true, entry1)
	voteOutput2 := c.newVoteOutput(2, true)

	// handle vote 1
	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// handle vote 2
	c.core.HandleVoteResponse(nodeID2, voteOutput2)
	assert.Equal(t, StateLeader, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID1, 1)
	assert.Equal(t, true, ok)

	entry1.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos:   2,
				Entry: entry1,
			},
		},
	}, acceptReq)

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_2_Entries__Stay_At_Candidate(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	entry1 := c.newLogEntry("cmd data 01", 19)
	entry2 := c.newLogEntry("cmd data 02", 19)
	entry3 := c.newLogEntry("cmd data 03", 18)

	voteOutput1 := c.newVoteOutput(2, false, entry1, entry2)
	voteOutput2 := c.newVoteOutput(2, true, LogEntry{}, entry3)

	// handle vote 1
	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// handle vote 2
	c.core.HandleVoteResponse(nodeID2, voteOutput2)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID1, 1)
	assert.Equal(t, true, ok)

	entry1.Term = c.currentTerm.ToInf()
	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos:   2,
				Entry: entry1,
			},
			{
				Pos:   3,
				Entry: entry2,
			},
		},
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Null_Entry(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	voteOutput1 := c.newVoteOutput(2, false, LogEntry{}, entry1)
	voteOutput2 := c.newVoteOutput(2, false, LogEntry{}, entry2)

	// handle vote 1
	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// handle vote 2
	c.core.HandleVoteResponse(nodeID2, voteOutput2)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID1, 1)
	assert.Equal(t, true, ok)

	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos: 2,
				Entry: LogEntry{
					Type: LogTypeNoOp,
					Term: c.currentTerm.ToInf(),
				},
			},
			{
				Pos:   3,
				Entry: entry2,
			},
		},
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Accept_Pos_Inc_By_One_Only(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	voteOutput1 := c.newVoteOutput(2, false, LogEntry{}, entry1)
	voteOutput2 := c.newVoteOutput(2, false, entry2)

	// handle vote 1
	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// handle vote 2
	c.core.HandleVoteResponse(nodeID2, voteOutput2)
	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID1, 1)
	assert.Equal(t, true, ok)

	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos:   2,
				Entry: entry2,
			},
		},
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Vote_Entry_Wrong_Start_Pos(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)

	voteOutput1 := c.newVoteOutput(2, true)
	voteOutput2 := c.newVoteOutput(3, false, entry1)

	// handle vote 1
	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	c.core.HandleVoteResponse(nodeID2, voteOutput2)

	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.cancelCtx, nodeID1, 1)
	assert.Equal(t, false, ok)
	assert.Equal(t, AcceptEntriesInput{}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Do_Not_Handle_Third_Vote_Response(t *testing.T) {
	c := newCoreLogicTest()

	// start election
	c.core.StartElection()

	entry1 := c.newLogEntry("cmd data 01", 17)
	entry2 := c.newLogEntry("cmd data 02", 18)
	entry3 := c.newLogEntry("cmd data 03", 22)

	voteOutput1 := c.newVoteOutput(2, false, entry1)
	voteOutput2 := c.newVoteOutput(2, false, entry2)
	voteOutput3 := c.newVoteOutput(2, false, entry3)

	c.core.HandleVoteResponse(nodeID1, voteOutput1)
	c.core.HandleVoteResponse(nodeID2, voteOutput2)
	c.core.HandleVoteResponse(nodeID3, voteOutput3)

	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID1, 1)
	assert.Equal(t, true, ok)

	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos:   2,
				Entry: entry2,
			},
		},
	}, acceptReq)
}

func TestCoreLogic__Insert_Cmd__Then_Get_Accept_Request(t *testing.T) {
	c := newCoreLogicTest()

	c.startAsLeader()

	// insert 2 commands
	c.doInsertCmd(
		"cmd data 01",
		"cmd data 02",
	)

	req, ok := c.core.GetAcceptEntriesRequest(c.ctx, nodeID2, 1)
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
					CmdData: []byte("cmd data 01"),
				},
			},
			{
				Pos: 3,
				Entry: LogEntry{
					Type:    LogTypeCmd,
					Term:    c.currentTerm.ToInf(),
					CmdData: []byte("cmd data 02"),
				},
			},
		},
	}, req)
}

func (c *coreLogicTest) newAcceptOutput(posList ...LogPos) AcceptEntriesOutput {
	return AcceptEntriesOutput{
		Success: true,
		Term:    c.currentTerm,
		PosList: posList,
	}
}

func TestCoreLogic__Insert_Cmd__Then_Handle_Accept_Response(t *testing.T) {
	c := newCoreLogicTest()

	c.startAsLeader()

	// insert 2 commands
	c.doInsertCmd(
		"cmd data 01",
		"cmd data 02",
	)

	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept
	ok := c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept node2
	ok = c.core.HandleAcceptEntriesResponse(nodeID2, c.newAcceptOutput(2))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(2), c.core.GetLastCommitted())

	// do accept node3
	ok = c.core.HandleAcceptEntriesResponse(nodeID3, c.newAcceptOutput(2))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(2), c.core.GetLastCommitted())
}

func TestCoreLogic__Insert_Cmd__Accept_Response_2_Entries(t *testing.T) {
	c := newCoreLogicTest()

	c.startAsLeader()

	// insert 2 commands
	c.doInsertCmd(
		"cmd data 01",
		"cmd data 02",
		"cmd data 03",
	)

	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept
	ok := c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2, 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept node2
	ok = c.core.HandleAcceptEntriesResponse(nodeID2, c.newAcceptOutput(2, 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(3), c.core.GetLastCommitted())
}

func TestCoreLogic__Insert_Cmd__Then_Accept_Response_Same_Node_Multi_Times(t *testing.T) {
	c := newCoreLogicTest()

	c.startAsLeader()

	// insert 2 commands
	c.doInsertCmd(
		"cmd data 01",
		"cmd data 02",
		"cmd data 03",
	)

	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept
	ok := c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2, 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept again
	ok = c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2, 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())
}
