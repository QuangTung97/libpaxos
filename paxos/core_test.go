package paxos_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
	"github.com/QuangTung97/libpaxos/paxos/testutil"
)

var (
	nodeID1 = fake.NewNodeID(1)
	nodeID2 = fake.NewNodeID(2)
	nodeID3 = fake.NewNodeID(3)

	nodeID4 = fake.NewNodeID(4)
	nodeID5 = fake.NewNodeID(5)
	nodeID6 = fake.NewNodeID(6)
)

type coreLogicTest struct {
	ctx       context.Context
	cancelCtx context.Context

	now atomic.Int64

	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *fake.NodeRunnerFake

	core CoreLogic

	currentTerm TermNum
}

func newCoreLogicTest(t *testing.T) *coreLogicTest {
	c := &coreLogicTest{}

	c.ctx = context.Background()
	c.now.Store(10_000)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	c.cancelCtx = cancelCtx

	c.persistent = &fake.PersistentStateFake{
		NodeID: nodeID1,
		LastTerm: TermNum{
			Num:    20,
			NodeID: nodeID5,
		},
	}
	c.log = &fake.LogStorageFake{}
	c.runner = &fake.NodeRunnerFake{}

	// setup init members
	initEntry := LogEntry{
		Type: LogTypeMembership,
		Term: InfiniteTerm{},
		Members: []MemberInfo{
			{
				Nodes:     []NodeID{nodeID1, nodeID2, nodeID3},
				CreatedAt: 1,
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
		func() TimestampMilli {
			return TimestampMilli(c.now.Load())
		},
	)

	t.Cleanup(c.core.CheckInvariant)

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

func (c *coreLogicTest) doHandleVoteResp(
	nodeID NodeID, fromPos LogPos, withFinal bool, entries ...LogEntry,
) {
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

	voteOutput := RequestVoteOutput{
		Success: true,
		Term:    c.currentTerm,
		Entries: voteEntries,
	}

	ok := c.core.HandleVoteResponse(nodeID, voteOutput)
	if !ok {
		panic("Should handle vote response ok")
	}
}

func (c *coreLogicTest) startAsLeader() {
	if !c.core.StartElection(c.persistent.GetLastTerm()) {
		panic("Should be able to start election")
	}

	c.doHandleVoteResp(nodeID1, 2, true)
	c.doHandleVoteResp(nodeID2, 2, true)
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
	c := newCoreLogicTest(t)

	// check leader & follower runners before election
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.LeaderTerm)
	assert.Equal(t, false, c.runner.IsLeader)
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.FollowerTerm)
	assert.Equal(t, true, c.runner.FollowerRunning)

	// start election
	ok := c.core.StartElection(c.persistent.GetLastTerm())
	assert.Equal(t, true, ok)

	// check runners
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)
	assert.Equal(t, c.currentTerm, c.runner.VoteTerm)
	assert.Equal(t, c.currentTerm, c.runner.AcceptTerm)

	// check leader & follower runners
	assert.Equal(t, c.currentTerm, c.runner.LeaderTerm)
	assert.Equal(t, false, c.runner.IsLeader)
	assert.Equal(t, c.currentTerm, c.runner.FollowerTerm)
	assert.Equal(t, false, c.runner.FollowerRunning)

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
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

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

	// handle vote response
	c.core.HandleVoteResponse(nodeID1, voteOutput)
	assert.Equal(t, StateCandidate, c.core.GetState())

	assert.Equal(t, []NodeID{nodeID2, nodeID3}, c.runner.VoteRunners)

	// switch to leader state
	c.core.HandleVoteResponse(nodeID2, voteOutput)
	assert.Equal(t, StateLeader, c.core.GetState())

	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, c.currentTerm, c.runner.LeaderTerm)
	assert.Equal(t, true, c.runner.IsLeader)

	// do nothing
	c.core.HandleVoteResponse(nodeID3, voteOutput)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Entries__To_Leader(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd test 01", 19)

	c.doHandleVoteResp(nodeID1, 2, true, entry1)
	c.doHandleVoteResp(nodeID2, 2, true)

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
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
		Committed: 1,
	}, acceptReq)

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_2_Entries__Stay_At_Candidate(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 19)
	entry2 := c.newLogEntry("cmd data 02", 19)
	entry3 := c.newLogEntry("cmd data 03", 18)

	c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2)
	c.doHandleVoteResp(nodeID2, 2, true, LogEntry{}, entry3)

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
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
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Both_Null_Entries(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	c.doHandleVoteResp(nodeID1, 2, false, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, LogEntry{}, entry2)

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
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
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Null_Entry__Replaced_By_Other_Entry(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 15)
	entry3 := c.newLogEntry("cmd data 03", 19)

	c.doHandleVoteResp(nodeID1, 2, false, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry2, entry3)

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
	assert.Equal(t, true, ok)

	entry2.Term = c.currentTerm.ToInf()
	entry3.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos:   2,
				Entry: entry2,
			},
			{
				Pos:   3,
				Entry: entry3,
			},
		},
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Accept_Pos_Inc_By_One_Only(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	c.doHandleVoteResp(nodeID1, 2, false, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry2)

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
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
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Vote_Entry_Wrong_Start_Pos(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 18)

	c.doHandleVoteResp(nodeID1, 2, true)
	c.doHandleVoteResp(nodeID2, 3, false, entry1)

	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req, first time not wait
	acceptReq, ok := c.core.GetAcceptEntriesRequest(
		c.cancelCtx, c.currentTerm, nodeID1, 2, 1,
	)
	assert.Equal(t, true, ok)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID1,
		Term:      c.currentTerm,
		Committed: 1,
	}, acceptReq)

	// check get accept req, waiting
	acceptReq, ok = c.core.GetAcceptEntriesRequest(
		c.cancelCtx, c.currentTerm, nodeID1, 2, 1,
	)
	assert.Equal(t, false, ok)
	assert.Equal(t, AcceptEntriesInput{}, acceptReq)
}

func (c *coreLogicTest) firstGetAcceptToSetTimeout() {
	// check get accept req, first time not wait
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 2, 1)
	if !ok {
		panic("Should be ok here")
	}
	if len(acceptReq.Entries) > 0 {
		panic("Should be empty here")
	}
}

func TestCoreLogic_GetAcceptEntries__Waiting__Then_Recv_2_Vote_Outputs(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 18)

	c.firstGetAcceptToSetTimeout()

	testutil.RunInBackground(t, func() {
		// check get accept req, waiting
		acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 2, 1)
		assert.Equal(t, true, ok)

		acceptEntry1 := entry1
		acceptEntry1.Term = c.currentTerm.ToInf()

		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID1,
			Term:   c.currentTerm,
			Entries: []AcceptLogEntry{
				{Pos: 2, Entry: acceptEntry1},
			},
			Committed: 1,
		}, acceptReq)
	})

	time.Sleep(10 * time.Millisecond)

	// vote response full for entry1
	c.doHandleVoteResp(nodeID1, 2, false, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry1)
}

func TestCoreLogic_GetAcceptEntries__Waiting__Then_Recv_2_Vote_Outputs__One_Is_Inf(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	c.firstGetAcceptToSetTimeout()

	testutil.RunInBackground(t, func() {
		// check get accept req, waiting
		acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 2, 1)
		assert.Equal(t, true, ok)

		acceptEntry1 := entry1
		acceptEntry1.Term = c.currentTerm.ToInf()

		acceptEntry2 := entry2
		acceptEntry2.Term = c.currentTerm.ToInf()

		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID1,
			Term:   c.currentTerm,
			Entries: []AcceptLogEntry{
				{Pos: 2, Entry: acceptEntry1},
				{Pos: 3, Entry: acceptEntry2},
			},
			Committed: 1,
		}, acceptReq)
	})

	time.Sleep(10 * time.Millisecond)

	// vote response full for entry1
	c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2)
	c.doHandleVoteResp(nodeID2, 2, true)
}

func TestCoreLogic_GetAcceptEntries__Waiting__Then_5_Sec_Timeout(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	c.firstGetAcceptToSetTimeout()

	testutil.RunInBackground(t, func() {
		// check get accept req, waiting
		acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 2, 1)
		assert.Equal(t, true, ok)

		assert.Equal(t, AcceptEntriesInput{
			ToNode:    nodeID1,
			Term:      c.currentTerm,
			Entries:   nil,
			Committed: 1,
		}, acceptReq)
	})

	time.Sleep(10 * time.Millisecond)

	c.now.Add(6_000)

	c.core.CheckTimeout()
}

func TestCoreLogic_HandleVoteResponse__Do_Not_Handle_Third_Vote_Response(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.core.StartElection(c.persistent.GetLastTerm())

	entry1 := c.newLogEntry("cmd data 01", 17)
	entry2 := c.newLogEntry("cmd data 02", 18)
	entry3 := c.newLogEntry("cmd data 03", 22)

	c.doHandleVoteResp(nodeID1, 2, false, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry2)
	c.doHandleVoteResp(nodeID3, 2, false, entry3)

	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 1, 0)
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
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic__Insert_Cmd__Then_Get_Accept_Request(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()

	// insert 2 commands
	c.doInsertCmd(
		"cmd data 01",
		"cmd data 02",
	)

	req, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID2, 1, 0)
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
		Committed: 1,
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
	c := newCoreLogicTest(t)

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
	c := newCoreLogicTest(t)

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
	c := newCoreLogicTest(t)

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

func (c *coreLogicTest) doHandleAccept(nodeID NodeID, posList ...LogPos) {
	ok := c.core.HandleAcceptEntriesResponse(nodeID, AcceptEntriesOutput{
		Success: true,
		Term:    c.currentTerm,
		PosList: posList,
	})
	if !ok {
		panic("Do handle accept should be ok")
	}
}

func TestCoreLogic__Handle_Vote_Resp__Without_More_After_Accept_Pos_Went_Up(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c := newCoreLogicTest(t)

		c.core.StartElection(c.persistent.GetLastTerm())

		entry1 := c.newLogEntry("cmd data 01", 18)
		entry2 := c.newLogEntry("cmd data 02", 18)

		c.doHandleVoteResp(nodeID1, 2, true, entry1, entry2)
		c.doHandleVoteResp(nodeID2, 2, false, entry1, entry2)
		assert.Equal(t, StateCandidate, c.core.GetState())

		c.doHandleVoteResp(nodeID3, 2, true)
		assert.Equal(t, StateLeader, c.core.GetState())
	})

	t.Run("pos max", func(t *testing.T) {
		c := newCoreLogicTest(t)

		c.core.StartElection(c.persistent.GetLastTerm())

		entry1 := c.newLogEntry("cmd data 01", 18)
		entry2 := c.newLogEntry("cmd data 02", 18)

		c.doHandleVoteResp(nodeID1, 2, true, entry1, entry2)
		c.doHandleVoteResp(nodeID2, 2, false, entry1, entry2)
		assert.Equal(t, StateCandidate, c.core.GetState())

		c.doHandleVoteResp(nodeID3, 4, true) // max possible
		assert.Equal(t, StateLeader, c.core.GetState())
	})

	t.Run("greater than pos max", func(t *testing.T) {
		c := newCoreLogicTest(t)

		c.core.StartElection(c.persistent.GetLastTerm())

		entry1 := c.newLogEntry("cmd data 01", 18)
		entry2 := c.newLogEntry("cmd data 02", 18)

		c.doHandleVoteResp(nodeID1, 2, true, entry1, entry2)
		c.doHandleVoteResp(nodeID2, 2, false, entry1, entry2)
		assert.Equal(t, StateCandidate, c.core.GetState())

		c.doHandleVoteResp(nodeID3, 5, true)
		assert.Equal(t, StateCandidate, c.core.GetState())
	})
}

func TestCoreLogic__Leader__Insert_Cmd__Then_Change_Membership(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()

	c.doInsertCmd(
		"cmd data 01",
		"cmd data 02",
	)

	// do change
	ok := c.core.ChangeMembership(c.currentTerm, []NodeID{nodeID4, nodeID5, nodeID6})
	assert.Equal(t, true, ok)

	// check active runners
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	c.doHandleAccept(nodeID1, 2, 3)
	c.doHandleAccept(nodeID2, 2, 3)
	// still not move up
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	c.doHandleAccept(nodeID4, 2, 3)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	c.doHandleAccept(nodeID5, 2, 3)
	// finally move up
	assert.Equal(t, LogPos(3), c.core.GetLastCommitted())

	acceptReq, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID6, 0, 0)
	assert.Equal(t, true, ok)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID6,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{
				Pos: 4,
				Entry: LogEntry{
					Type: LogTypeMembership,
					Term: c.currentTerm.ToInf(),
					Members: []MemberInfo{
						{
							Nodes:     []NodeID{nodeID1, nodeID2, nodeID3},
							CreatedAt: 1,
						},
						{
							Nodes:     []NodeID{nodeID4, nodeID5, nodeID6},
							CreatedAt: 4,
						},
					},
				},
			},
		},
		Committed: 3,
	}, acceptReq)
}

func TestCoreLogic__Candidate__Handle_Vote_Resp_With_Membership_Change(t *testing.T) {
	c := newCoreLogicTest(t)

	c.core.StartElection(c.persistent.GetLastTerm())

	newMembers := []MemberInfo{
		{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 4},
	}

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 18)
	entry3 := LogEntry{
		Type: LogTypeMembership,
		Term: TermNum{
			Num:    19,
			NodeID: nodeID3,
		}.ToInf(),
		Members: newMembers,
	}

	c.doHandleVoteResp(nodeID1, 2, true, entry1, entry2, entry3)

	assert.Equal(t, []NodeID{nodeID2, nodeID3}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)

	c.doHandleVoteResp(nodeID2, 2, true)

	assert.Equal(t, []NodeID{
		nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.VoteRunners)

	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	// state is still candidate
	assert.Equal(t, StateCandidate, c.core.GetState())

	c.doHandleVoteResp(nodeID4, 5, true)
	assert.Equal(t, StateCandidate, c.core.GetState())

	c.doHandleVoteResp(nodeID5, 5, true)
	assert.Equal(t, StateLeader, c.core.GetState())
}

func (c *coreLogicTest) doGetAcceptReq(
	nodeID NodeID, fromPos LogPos, lastCommitted LogPos,
) AcceptEntriesInput {
	req, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID, fromPos, lastCommitted)
	if !ok {
		panic("Get accept entries req should return ok")
	}
	return req
}

func (c *coreLogicTest) doGetAcceptReqAsync(
	t *testing.T, nodeID NodeID, fromPos LogPos, lastCommitted LogPos,
) func() AcceptEntriesInput {
	t.Helper()
	fn, _ := testutil.RunAsync[AcceptEntriesInput](t, func() AcceptEntriesInput {
		return c.doGetAcceptReq(nodeID, fromPos, lastCommitted)
	})
	return fn
}

func TestCoreLogic__Candidate__Change_Membership(t *testing.T) {
	t.Run("2 consecutive member changes", func(t *testing.T) {
		c := newCoreLogicTest(t)

		c.core.StartElection(c.persistent.GetLastTerm())

		newMembers1 := []MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
			{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 4},
		}
		newMembers2 := []MemberInfo{
			{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 1},
		}

		entry1 := LogEntry{
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    19,
				NodeID: nodeID3,
			}.ToInf(),
			Members: newMembers1,
		}
		entry2 := LogEntry{
			Type: LogTypeMembership,
			Term: TermNum{
				Num:    19,
				NodeID: nodeID3,
			}.ToInf(),
			Members: newMembers2,
		}
		entry3 := c.newLogEntry("cmd data 03", 18)

		c.doHandleVoteResp(nodeID1, 2, true, entry1, entry2)
		c.doHandleVoteResp(nodeID2, 2, true)

		// state is still candidate
		assert.Equal(t, StateCandidate, c.core.GetState())

		assert.Equal(t, []NodeID{nodeID3, nodeID4, nodeID5, nodeID6}, c.runner.VoteRunners)
		assert.Equal(t, []NodeID{
			nodeID1, nodeID2, nodeID3,
			nodeID4, nodeID5, nodeID6,
		}, c.runner.AcceptRunners)

		// handle for node 4
		c.doHandleVoteResp(nodeID4, 3, true)
		assert.Equal(t, StateCandidate, c.core.GetState())

		assert.Equal(t, []NodeID{nodeID3, nodeID5, nodeID6}, c.runner.VoteRunners)
		assert.Equal(t, []NodeID{
			nodeID1, nodeID2, nodeID3,
			nodeID4, nodeID5, nodeID6,
		}, c.runner.AcceptRunners)

		// handle for node 5
		c.doHandleVoteResp(nodeID5, 3, true, entry2, entry3)
		assert.Equal(t, StateLeader, c.core.GetState())

		assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
		assert.Equal(t, []NodeID{
			nodeID4, nodeID5, nodeID6,
		}, c.runner.AcceptRunners)

		// get accept requests
		entry1.Term = c.currentTerm.ToInf()
		entry2.Term = c.currentTerm.ToInf()
		entry3.Term = c.currentTerm.ToInf()

		accReq := c.doGetAcceptReq(nodeID6, 2, 0)
		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID6,
			Term:   c.currentTerm,
			Entries: []AcceptLogEntry{
				{Pos: 2, Entry: entry1},
				{Pos: 3, Entry: entry2},
				{Pos: 4, Entry: entry3},
			},
			Committed: 1,
		}, accReq)

		// put entries
		c.doHandleAccept(nodeID4, 2, 3, 4)
		c.doHandleAccept(nodeID5, 2, 3, 4)

		// get again empty but not wait
		accReq = c.doGetAcceptReq(nodeID6, 5, 1)
		assert.Equal(t, AcceptEntriesInput{
			ToNode:    nodeID6,
			Term:      c.currentTerm,
			Committed: 4,
		}, accReq)
	})
}

func (c *coreLogicTest) doChangeMembers(newNodes []NodeID) {
	if !c.core.ChangeMembership(c.currentTerm, newNodes) {
		panic("Change members should be OK")
	}
}

func TestCoreLogic__Leader__Change_Membership__Then_Wait_New_Accept_Entry(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()
	c.doChangeMembers([]NodeID{
		nodeID4, nodeID5, nodeID6,
	})

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	c.doInsertCmd("cmd data 01")
	c.doInsertCmd("cmd data 02", "cmd data 03")

	// check accept req
	acceptReq := c.doGetAcceptReq(nodeID6, 0, 0)

	newCmdFunc := func(cmdStr string) LogEntry {
		entry := c.newLogEntry(cmdStr, c.currentTerm.Num)
		entry.Term = c.currentTerm.ToInf()
		return entry
	}

	membersEntry := LogEntry{
		Type: LogTypeMembership,
		Term: c.currentTerm.ToInf(),
		Members: []MemberInfo{
			{CreatedAt: 1, Nodes: []NodeID{nodeID1, nodeID2, nodeID3}},
			{CreatedAt: 2, Nodes: []NodeID{nodeID4, nodeID5, nodeID6}},
		},
	}

	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID6,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{Pos: 2, Entry: membersEntry},
			{Pos: 3, Entry: newCmdFunc("cmd data 01")},
			{Pos: 4, Entry: newCmdFunc("cmd data 02")},
			{Pos: 5, Entry: newCmdFunc("cmd data 03")},
		},
		Committed: 1,
	}, acceptReq)

	synctest.Test(t, func(t *testing.T) {
		// check accept req again, waiting
		acceptFn, _ := testutil.RunAsync(t, func() AcceptEntriesInput {
			return c.doGetAcceptReq(nodeID6, 6, 1)
		})

		c.doInsertCmd("cmd data 04")

		acceptReq := acceptFn()
		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID6,
			Term:   c.currentTerm,
			Entries: []AcceptLogEntry{
				{Pos: 6, Entry: newCmdFunc("cmd data 04")},
			},
			Committed: 1,
		}, acceptReq)
	})
}

func TestCoreLogic__Leader__Wait_For_New_Committed_Pos(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()

	c.doInsertCmd("cmd data 01")
	c.doInsertCmd("cmd data 02", "cmd data 03")

	// check accept req
	acceptReq := c.doGetAcceptReq(nodeID3, 0, 0)

	newCmdFunc := func(cmdStr string) LogEntry {
		entry := c.newLogEntry(cmdStr, c.currentTerm.Num)
		entry.Term = c.currentTerm.ToInf()
		return entry
	}

	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID3,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{Pos: 2, Entry: newCmdFunc("cmd data 01")},
			{Pos: 3, Entry: newCmdFunc("cmd data 02")},
			{Pos: 4, Entry: newCmdFunc("cmd data 03")},
		},
		Committed: 1,
	}, acceptReq)

	synctest.Test(t, func(t *testing.T) {
		acceptFn, _ := testutil.RunAsync(t, func() AcceptEntriesInput {
			return c.doGetAcceptReq(nodeID3, 5, 1)
		})

		c.doHandleAccept(nodeID1, 2, 3)
		c.doHandleAccept(nodeID2, 2, 3)

		accReq := acceptFn()
		assert.Equal(t, AcceptEntriesInput{
			ToNode:    nodeID3,
			Term:      c.currentTerm,
			Committed: 3,
		}, accReq)
	})

	// check accept req after committed pos = 3
	acceptReq = c.doGetAcceptReq(nodeID1, 0, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{Pos: 4, Entry: newCmdFunc("cmd data 03")},
		},
		Committed: 3,
	}, acceptReq)
}

func TestCoreLogic__Follower_GetReadyToStartElect(t *testing.T) {
	c := newCoreLogicTest(t)

	ok := c.core.GetReadyToStartElection(c.ctx, c.persistent.GetLastTerm())
	assert.Equal(t, true, ok)

	synctest.Test(t, func(t *testing.T) {
		checkFn, assertNotFinish := testutil.RunAsync(t, func() bool {
			return c.core.GetReadyToStartElection(c.ctx, c.persistent.GetLastTerm())
		})

		c.now.Add(4000)
		c.core.CheckTimeout()
		assertNotFinish()

		c.now.Add(1100)
		c.core.CheckTimeout()

		assert.Equal(t, true, checkFn())
	})
}

func TestCoreLogic__GetReadyToStartElect_Wait__Then_Switch_To_Candidate(t *testing.T) {
	c := newCoreLogicTest(t)

	ok := c.core.GetReadyToStartElection(c.ctx, c.persistent.GetLastTerm())
	assert.Equal(t, true, ok)

	// check with wrong term
	ok = c.core.GetReadyToStartElection(c.ctx, c.currentTerm)
	assert.Equal(t, false, ok)

	synctest.Test(t, func(t *testing.T) {
		checkFn, _ := testutil.RunAsync(t, func() bool {
			return c.core.GetReadyToStartElection(c.ctx, c.persistent.GetLastTerm())
		})

		ok := c.core.StartElection(c.persistent.GetLastTerm())
		assert.Equal(t, true, ok)

		assert.Equal(t, false, checkFn())

		// check again
		ok = c.core.GetReadyToStartElection(c.ctx, c.currentTerm)
		assert.Equal(t, false, ok)
	})
}

func (c *coreLogicTest) doStartElection() {
	if !c.core.StartElection(c.persistent.GetLastTerm()) {
		panic("Should start election OK")
	}
}

func TestCoreLogic__Candidate__Recv_Higher_Accept_Req_Term(t *testing.T) {
	c := newCoreLogicTest(t)
	c.doStartElection()

	accReq := c.doGetAcceptReq(nodeID3, 2, 1)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID3,
		Term:      c.currentTerm,
		Committed: 1,
	}, accReq)

	synctest.Test(t, func(t *testing.T) {
		acceptResult, _ := testutil.RunAsync(t, func() bool {
			_, ok := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID3, 2, 1)
			return ok
		})

		newTerm := TermNum{
			Num:    22,
			NodeID: nodeID2,
		}
		c.core.FollowerReceiveAcceptEntriesRequest(newTerm)

		assert.Equal(t, false, acceptResult())

		// check state
		assert.Equal(t, StateFollower, c.core.GetState())

		// check runners
		assert.Equal(t, newTerm, c.runner.VoteTerm)
		assert.Equal(t, []NodeID{}, c.runner.VoteRunners)

		assert.Equal(t, newTerm, c.runner.AcceptTerm)
		assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)

		assert.Equal(t, newTerm, c.runner.LeaderTerm)
		assert.Equal(t, false, c.runner.IsLeader)

		assert.Equal(t, newTerm, c.runner.FollowerTerm)
		assert.Equal(t, true, c.runner.FollowerRunning)

		// check follower waiting
		c.now.Add(4000)
		checkFn, _ := testutil.RunAsync(t, func() bool {
			return c.core.GetReadyToStartElection(c.ctx, newTerm)
		})

		c.now.Add(1100)
		c.core.CheckTimeout()

		assert.Equal(t, true, checkFn())
	})
}

func TestCoreLogic__Follower__Recv_Higher_Accept_Req_Term(t *testing.T) {
	c := newCoreLogicTest(t)

	newTerm := TermNum{
		Num:    22,
		NodeID: nodeID2,
	}

	synctest.Test(t, func(t *testing.T) {
		c.core.FollowerReceiveAcceptEntriesRequest(newTerm)
		// check follower runner
		assert.Equal(t, true, c.runner.FollowerRunning)
		assert.Equal(t, newTerm, c.runner.FollowerTerm)

		checkFn, _ := testutil.RunAsync(t, func() bool {
			return c.core.GetReadyToStartElection(c.ctx, newTerm)
		})

		c.now.Add(5100)
		c.core.CheckTimeout()

		assert.Equal(t, true, checkFn())
	})
}

func TestCoreLogic__Candidate__Recv_Lower_Term(t *testing.T) {
	c := newCoreLogicTest(t)
	c.doStartElection()

	newTerm := TermNum{
		Num:    17,
		NodeID: nodeID2,
	}
	affected := c.core.FollowerReceiveAcceptEntriesRequest(newTerm)
	assert.Equal(t, false, affected)

	// no change in state
	assert.Equal(t, StateCandidate, c.core.GetState())
}

func (c *coreLogicTest) doUpdateFullyReplicated(nodeID NodeID, pos LogPos) {
	if !c.core.UpdateAcceptorFullyReplicated(c.currentTerm, nodeID, pos) {
		panic("Should update OK")
	}
}

func TestCoreLogic__Leader__Change_Membership__Update_Fully_Replicated__Finish_Membership_Change(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doInsertCmd("cmd 01", "cmd 02")

	ok := c.core.ChangeMembership(c.currentTerm, []NodeID{
		nodeID3, nodeID4, nodeID5,
	})
	assert.Equal(t, true, ok)

	c.doUpdateFullyReplicated(nodeID3, 1)

	c.doHandleAccept(nodeID1, 2, 3, 4)
	c.doHandleAccept(nodeID3, 2, 3, 4)
	c.doHandleAccept(nodeID4, 2, 3, 4)
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())

	c.doUpdateFullyReplicated(nodeID4, 1)
	c.doUpdateFullyReplicated(nodeID3, 4)

	// check accept entries
	accReq := c.doGetAcceptReq(nodeID5, 0, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID5,
		Term:      c.currentTerm,
		Committed: 4,
	}, accReq)

	c.doUpdateFullyReplicated(nodeID4, 4)

	// check accept entries again
	accReq = c.doGetAcceptReq(nodeID5, 0, 0)
	newMembers := LogEntry{
		Type: LogTypeMembership,
		Term: c.currentTerm.ToInf(),
		Members: []MemberInfo{
			{Nodes: []NodeID{nodeID3, nodeID4, nodeID5}, CreatedAt: 1},
		},
	}
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID5,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{Pos: 5, Entry: newMembers},
		},
		Committed: 4,
	}, accReq)
}

func TestCoreLogic__Leader__Fully_Replicated_Faster_Than_Last_Committed(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doInsertCmd("cmd 01", "cmd 02")

	ok := c.core.ChangeMembership(c.currentTerm, []NodeID{
		nodeID3, nodeID4, nodeID5,
	})
	assert.Equal(t, true, ok)

	c.doUpdateFullyReplicated(nodeID3, 4)
	c.doUpdateFullyReplicated(nodeID4, 4)

	// check accept entries
	accReq := c.doGetAcceptReq(nodeID5, 5, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID5,
		Term:      c.currentTerm,
		Committed: 1,
	}, accReq)

	c.doHandleAccept(nodeID1, 2, 3, 4)
	c.doHandleAccept(nodeID3, 2, 3, 4)
	c.doHandleAccept(nodeID4, 2, 3, 4)
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())

	// check accept entries again
	accReq = c.doGetAcceptReq(nodeID5, 5, 0)
	newMembers := LogEntry{
		Type: LogTypeMembership,
		Term: c.currentTerm.ToInf(),
		Members: []MemberInfo{
			{Nodes: []NodeID{nodeID3, nodeID4, nodeID5}, CreatedAt: 1},
		},
	}
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID5,
		Term:   c.currentTerm,
		Entries: []AcceptLogEntry{
			{Pos: 5, Entry: newMembers},
		},
		Committed: 4,
	}, accReq)
}

func TestCoreLogic__Candidate__Handle_Inf_Term_Vote_Response(t *testing.T) {
	c := newCoreLogicTest(t)
	c.doStartElection()

	entry1 := c.newLogEntry("cmd 01", 18)
	entry2 := c.newLogEntry("cmd 02", 18)
	entry2.Term = InfiniteTerm{}

	c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2)
	c.core.CheckInvariant()

	c.doHandleVoteResp(nodeID2, 2, false, entry1)

	c.doHandleAccept(nodeID1, 2)
	c.doHandleAccept(nodeID2, 2)

	// check get accept req
	accReq := c.doGetAcceptReq(nodeID3, 0, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID3,
		Term:      c.currentTerm,
		Committed: 2,
	}, accReq)
}
