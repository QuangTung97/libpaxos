package paxos_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

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

type coreLogicTestConfig struct {
	maxBufferLen LogPos
}

func newCoreTestConfig() coreLogicTestConfig {
	return coreLogicTestConfig{
		maxBufferLen: 1000,
	}
}

func newCoreLogicTest(t *testing.T) *coreLogicTest {
	return newCoreLogicTestWithConfig(t, newCoreTestConfig())
}

func newCoreLogicTestWithConfig(t *testing.T, config coreLogicTestConfig) *coreLogicTest {
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
	}, nil)

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
		config.maxBufferLen,
		true,
		5000, // 5 seconds
		0,    // 0 second
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

func (c *coreLogicTest) newInfLogEntry(cmdStr string) LogEntry {
	return NewCmdLogEntry(InfiniteTerm{}, []byte(cmdStr))
}

func (c *coreLogicTest) newAcceptLogEntry(cmdStr string) LogEntry {
	return NewCmdLogEntry(c.currentTerm.ToInf(), []byte(cmdStr))
}

func (c *coreLogicTest) doHandleVoteResp(
	nodeID NodeID, fromPos LogPos, withFinal bool, entries ...LogEntry,
) {
	voteEntries := make([]VoteLogEntry, 0, len(entries)+1)
	for index, e := range entries {
		voteEntries = append(voteEntries, VoteLogEntry{
			Pos:     fromPos + LogPos(index),
			IsFinal: false,
			Entry:   e,
		})
	}

	if withFinal {
		voteEntries = append(voteEntries, VoteLogEntry{
			Pos:     fromPos + LogPos(len(entries)),
			IsFinal: true,
		})
	}

	voteOutput := RequestVoteOutput{
		Success: true,
		Term:    c.currentTerm,
		Entries: voteEntries,
	}

	if err := c.core.HandleVoteResponse(c.ctx, nodeID, voteOutput); err != nil {
		panic(err.Error())
	}

	c.core.CheckInvariant()
}

func (c *coreLogicTest) startAsLeader() {
	if err := c.core.StartElection(0); err != nil {
		panic("Should be able to start election, but got error: " + err.Error())
	}

	c.doHandleVoteResp(nodeID1, 2, true)
	c.doHandleVoteResp(nodeID2, 2, true)

	c.core.CheckInvariant()
}

func (c *coreLogicTest) doInsertCmd(cmdList ...string) {
	cmdListBytes := make([][]byte, 0, len(cmdList))
	for _, cmd := range cmdList {
		cmdListBytes = append(cmdListBytes, []byte(cmd))
	}
	if err := c.core.InsertCommand(c.ctx, c.currentTerm, cmdListBytes...); err != nil {
		panic("can not insert, error: " + err.Error())
	}

	c.core.CheckInvariant()
}

func (c *coreLogicTest) insertToDiskLog(from LogPos, entries ...LogEntry) {
	var posEntries []PosLogEntry
	for _, entry := range entries {
		entry.Term = InfiniteTerm{}
		posEntries = append(posEntries, PosLogEntry{
			Pos:   from,
			Entry: entry,
		})
		from++
	}
	c.log.UpsertEntries(posEntries, nil)
}

func TestCoreLogic_StartElection__Then_GetRequestVote(t *testing.T) {
	c := newCoreLogicTest(t)

	// check leader & follower runners before election
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.StateMachineTerm)
	assert.Equal(t, StateMachineRunnerInfo{
		Running: true,
	}, c.runner.StateMachineInfo)
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.FetchFollowerTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.FetchFollowers)
	assert.Equal(t, ChooseLeaderInfo{
		NoActiveLeader:  true,
		Members:         c.log.GetCommittedInfo().Members,
		FullyReplicated: c.log.GetFullyReplicated(),
		LastTermVal:     20,
	}, c.core.GetChoosingLeaderInfo())

	// start election
	err := c.core.StartElection(0)
	assert.Equal(t, nil, err)
	assert.Equal(t, false, c.core.GetChoosingLeaderInfo().NoActiveLeader)

	// check runners
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)
	assert.Equal(t, c.currentTerm, c.runner.VoteTerm)
	assert.Equal(t, c.currentTerm, c.runner.AcceptTerm)

	// check leader & follower runners
	assert.Equal(t, c.currentTerm, c.runner.StateMachineTerm)
	assert.Equal(t, StateMachineRunnerInfo{
		Running:  true,
		IsLeader: true,
	}, c.runner.StateMachineInfo)
	assert.Equal(t, c.currentTerm, c.runner.FetchFollowerTerm)
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)

	// get vote request
	voteReq, err := c.core.GetVoteRequest(c.currentTerm, nodeID2)
	assert.Equal(t, nil, err)
	assert.Equal(t, RequestVoteInput{
		Term: TermNum{
			Num:    21,
			NodeID: nodeID1,
		},
		ToNode:  nodeID2,
		FromPos: 2,
	}, voteReq)

	// get vote request for leader node
	voteReq, err = c.core.GetVoteRequest(c.currentTerm, nodeID1)
	assert.Equal(t, nil, err)
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
	c.doStartElection()

	voteOutput := RequestVoteOutput{
		Success: true,
		Term:    c.currentTerm,
		Entries: []VoteLogEntry{
			{
				Pos:     2,
				IsFinal: true,
			},
		},
	}

	// handle vote response
	err := c.core.HandleVoteResponse(c.ctx, nodeID1, voteOutput)
	assert.Equal(t, nil, err)
	assert.Equal(t, StateCandidate, c.core.GetState())

	assert.Equal(t, []NodeID{nodeID2, nodeID3}, c.runner.VoteRunners)

	// switch to leader state
	err = c.core.HandleVoteResponse(c.ctx, nodeID2, voteOutput)
	assert.Equal(t, nil, err)
	assert.Equal(t, StateLeader, c.core.GetState())

	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, c.currentTerm, c.runner.StateMachineTerm)
	assert.Equal(t, StateMachineRunnerInfo{
		Running:       true,
		IsLeader:      true,
		AcceptCommand: true,
	}, c.runner.StateMachineInfo)

	// do nothing
	err = c.core.HandleVoteResponse(c.ctx, nodeID3, voteOutput)
	assert.Equal(t, errors.New("expected state 'Candidate', got: 'Leader'"), err)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Entries__To_Leader(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.doStartElection()

	entry1 := c.newLogEntry("cmd test 01", 19)

	c.doHandleVoteResp(nodeID1, 2, true, entry1)
	c.doHandleVoteResp(nodeID2, 2, true)

	// check get accept req
	acceptReq, err := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
	assert.Equal(t, nil, err)

	entry1.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{
				Pos:   2,
				Entry: entry1,
			},
		},
		NextPos:   3,
		Committed: 1,
	}, acceptReq)

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_2_Entries__Stay_At_Candidate(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 19)
	entry2 := c.newLogEntry("cmd data 02", 19)
	entry3 := c.newLogEntry("cmd data 03", 18)

	c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2)
	c.doHandleVoteResp(nodeID2, 2, true, LogEntry{}, entry3)

	// check get accept req
	acceptReq, err := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
	assert.Equal(t, nil, err)

	entry1.Term = c.currentTerm.ToInf()
	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{
				Pos:   2,
				Entry: entry1,
			},
			{
				Pos:   3,
				Entry: entry2,
			},
		},
		NextPos:   4,
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Both_Null_Entries(t *testing.T) {
	c := newCoreLogicTest(t)

	// start election
	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	c.doHandleVoteResp(nodeID1, 2, false, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, LogEntry{}, entry2)

	// check get accept req
	acceptReq, err := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
	assert.Equal(t, nil, err)

	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
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
		NextPos:   4,
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__With_Prev_Null_Entry__Replaced_By_Other_Entry(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 15)
	entry3 := c.newLogEntry("cmd data 03", 19)

	c.doHandleVoteResp(nodeID1, 2, false, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry2, entry3)

	// check get accept req
	acceptReq, err := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
	assert.Equal(t, nil, err)

	entry2.Term = c.currentTerm.ToInf()
	entry3.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{
				Pos:   2,
				Entry: entry2,
			},
			{
				Pos:   3,
				Entry: entry3,
			},
		},
		NextPos:   4,
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Accept_Pos_Inc_By_One_Only(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	c.doHandleVoteResp(nodeID1, 2, false, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry2)

	// check get accept req
	acceptReq, err := c.core.GetAcceptEntriesRequest(
		c.ctx, c.currentTerm, nodeID1, 1, 0,
	)
	assert.Equal(t, nil, err)

	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{
				Pos:   2,
				Entry: entry2,
			},
		},
		NextPos:   3,
		Committed: 1,
	}, acceptReq)
}

func TestCoreLogic_HandleVoteResponse__Vote_Entry_Wrong_Start_Pos(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)

	c.doHandleVoteResp(nodeID1, 2, true)
	c.doHandleVoteResp(nodeID2, 3, false, entry1)

	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req, first time not wait
	acceptReq, err := c.core.GetAcceptEntriesRequest(
		c.cancelCtx, c.currentTerm, nodeID2, 2, 1,
	)
	assert.Equal(t, nil, err)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID2,
		Term:      c.currentTerm,
		NextPos:   2,
		Committed: 1,
	}, acceptReq)

	// check get accept req, waiting
	acceptReq, err = c.core.GetAcceptEntriesRequest(
		c.cancelCtx, c.currentTerm, nodeID2, 2, 1,
	)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, AcceptEntriesInput{}, acceptReq)
}

func (c *coreLogicTest) firstGetAcceptToSetTimeout(id NodeID) {
	// check get accept req, first time not wait
	acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, id, 2, 1)
	if err != nil {
		panic("Should be ok here, but got: " + err.Error())
	}
	if len(acceptReq.Entries) > 0 {
		panic("Should be empty here")
	}

	c.core.CheckInvariant()
}

func TestCoreLogic_GetAcceptEntries__Waiting__Then_Recv_2_Vote_Outputs(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()
	c.firstGetAcceptToSetTimeout(nodeID2)

	entry1 := c.newLogEntry("cmd data 01", 18)

	synctest.Test(t, func(t *testing.T) {
		resultFn, _ := testutil.RunAsync(t, func() AcceptEntriesInput {
			// check get accept req, waiting
			acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID2, 2, 1)
			assert.Equal(t, nil, err)
			return acceptReq
		})

		// vote response full for entry1
		c.doHandleVoteResp(nodeID1, 2, false, entry1)
		c.doHandleVoteResp(nodeID2, 2, false, entry1)

		acceptEntry1 := entry1
		acceptEntry1.Term = c.currentTerm.ToInf()

		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID2,
			Term:   c.currentTerm,
			Entries: []PosLogEntry{
				{Pos: 2, Entry: acceptEntry1},
			},
			NextPos:   3,
			Committed: 1,
		}, resultFn())
	})
}

func TestCoreLogic_GetAcceptEntries__Current_Node__Wait(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	synctest.Test(t, func(t *testing.T) {
		resultFn, _ := testutil.RunAsync(t, func() AcceptEntriesInput {
			// check get accept req, waiting
			acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 2, 1)
			assert.Equal(t, nil, err)
			return acceptReq
		})

		c.doInsertCmd(
			"cmd test 02",
			"cmd test 03",
		)

		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID1,
			Term:   c.currentTerm,
			Entries: []PosLogEntry{
				{Pos: 2, Entry: c.newAcceptLogEntry("cmd test 02")},
				{Pos: 3, Entry: c.newAcceptLogEntry("cmd test 03")},
			},
			NextPos:   4,
			Committed: 1,
		}, resultFn())
	})
}

func TestCoreLogic_GetAcceptEntries__Waiting__Then_Recv_2_Vote_Outputs__One_Is_Inf(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 19)

	c.firstGetAcceptToSetTimeout(nodeID2)

	synctest.Test(t, func(t *testing.T) {
		resultFn, _ := testutil.RunAsync(t, func() AcceptEntriesInput {
			// check get accept req, waiting
			acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID2, 2, 1)
			assert.Equal(t, nil, err)
			return acceptReq
		})

		// vote response full for entry1
		c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2)
		c.doHandleVoteResp(nodeID2, 2, true)

		// check accept req
		acceptEntry1 := entry1
		acceptEntry1.Term = c.currentTerm.ToInf()

		acceptEntry2 := entry2
		acceptEntry2.Term = c.currentTerm.ToInf()

		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID2,
			Term:   c.currentTerm,
			Entries: []PosLogEntry{
				{Pos: 2, Entry: acceptEntry1},
				{Pos: 3, Entry: acceptEntry2},
			},
			NextPos:   4,
			Committed: 1,
		}, resultFn())
	})
}

func TestCoreLogic_GetAcceptEntries__Waiting__Then_5_Sec_Timeout(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()
	c.firstGetAcceptToSetTimeout(nodeID2)

	synctest.Test(t, func(t *testing.T) {
		fn, _ := testutil.RunAsync(t, func() AcceptEntriesInput {
			// check get accept req, waiting
			acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID2, 2, 1)
			assert.Equal(t, nil, err)
			return acceptReq
		})

		c.now.Add(6_000)
		c.core.CheckTimeout()

		assert.Equal(t, AcceptEntriesInput{
			ToNode:    nodeID2,
			Term:      c.currentTerm,
			Entries:   nil,
			NextPos:   2,
			Committed: 1,
		}, fn())
	})
}

func TestCoreLogic_HandleVoteResponse__Do_Not_Handle_Third_Vote_Response(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 17)
	entry2 := c.newLogEntry("cmd data 02", 18)
	entry3 := c.newLogEntry("cmd data 03", 22)

	c.doHandleVoteResp(nodeID1, 2, false, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, entry2)
	c.doHandleVoteResp(nodeID3, 2, false, entry3)

	assert.Equal(t, StateCandidate, c.core.GetState())

	// check get accept req
	acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID1, 1, 0)
	assert.Equal(t, nil, err)

	entry2.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 2, Entry: entry2},
		},
		NextPos:   3,
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

	req, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID2, 1, 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID2,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
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
		NextPos:   4,
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
	err := c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2))
	assert.Equal(t, nil, err)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept node2
	err = c.core.HandleAcceptEntriesResponse(nodeID2, c.newAcceptOutput(2))
	assert.Equal(t, nil, err)
	assert.Equal(t, LogPos(2), c.core.GetLastCommitted())

	// do accept node3
	err = c.core.HandleAcceptEntriesResponse(nodeID3, c.newAcceptOutput(2))
	assert.Equal(t, nil, err)
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
	err := c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2, 3))
	assert.Equal(t, nil, err)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept node2
	err = c.core.HandleAcceptEntriesResponse(nodeID2, c.newAcceptOutput(2, 3))
	assert.Equal(t, nil, err)
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
	err := c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2, 3))
	assert.Equal(t, nil, err)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	// do accept again
	err = c.core.HandleAcceptEntriesResponse(nodeID1, c.newAcceptOutput(2, 3))
	assert.Equal(t, nil, err)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())
}

func (c *coreLogicTest) doHandleAccept(nodeID NodeID, posList ...LogPos) {
	err := c.core.HandleAcceptEntriesResponse(nodeID, AcceptEntriesOutput{
		Success: true,
		Term:    c.currentTerm,
		PosList: posList,
	})
	if err != nil {
		panic("Do handle accept should be ok, but got: " + err.Error())
	}
	c.core.CheckInvariant()
}

func (c *coreLogicTest) doHandleAcceptWithErr(nodeID NodeID, posList ...LogPos) error {
	err := c.core.HandleAcceptEntriesResponse(nodeID, AcceptEntriesOutput{
		Success: true,
		Term:    c.currentTerm,
		PosList: posList,
	})
	c.core.CheckInvariant()
	return err
}

func TestCoreLogic__Handle_Vote_Resp__Without_More__After_Accept_Pos_Went_Up(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c := newCoreLogicTest(t)

		c.doStartElection()

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

		c.doStartElection()

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

		c.doStartElection()

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
	err := c.core.ChangeMembership(c.ctx, c.currentTerm, []NodeID{nodeID4, nodeID5, nodeID6})
	assert.Equal(t, nil, err)

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

	acceptReq, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID6, 0, 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID6,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
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
		NextPos:   5,
		Committed: 3,
	}, acceptReq)
}

func TestCoreLogic__Candidate__Handle_Vote_Resp_With_Membership_Change(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

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
	req, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID, fromPos, lastCommitted)
	if err != nil {
		panic("Get accept entries req should return ok, but got: " + err.Error())
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

		c.doStartElection()

		newMembers1 := []MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
			{Nodes: []NodeID{nodeID1, nodeID4, nodeID5}, CreatedAt: 4},
		}
		newMembers2 := []MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID4, nodeID5}, CreatedAt: 1},
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

		c.doHandleVoteResp(nodeID2, 2, true, entry1, entry2)
		c.doHandleVoteResp(nodeID3, 2, true)

		// state is still candidate
		assert.Equal(t, StateCandidate, c.core.GetState())

		assert.Equal(t, []NodeID{nodeID1, nodeID4, nodeID5}, c.runner.VoteRunners)
		assert.Equal(t, []NodeID{
			nodeID1, nodeID2, nodeID3,
			nodeID4, nodeID5,
		}, c.runner.AcceptRunners)

		// handle for node 4
		c.doHandleVoteResp(nodeID4, 3, true)
		assert.Equal(t, StateCandidate, c.core.GetState())

		assert.Equal(t, []NodeID{nodeID1, nodeID5}, c.runner.VoteRunners)
		assert.Equal(t, []NodeID{
			nodeID1, nodeID2, nodeID3,
			nodeID4, nodeID5,
		}, c.runner.AcceptRunners)

		// handle for node 5
		c.doHandleVoteResp(nodeID5, 3, true, entry2, entry3)
		assert.Equal(t, StateLeader, c.core.GetState())

		assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
		assert.Equal(t, []NodeID{
			nodeID1, nodeID4, nodeID5,
		}, c.runner.AcceptRunners)

		// get accept requests
		entry1.Term = c.currentTerm.ToInf()
		entry2.Term = c.currentTerm.ToInf()
		entry3.Term = c.currentTerm.ToInf()

		accReq := c.doGetAcceptReq(nodeID5, 2, 0)
		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID5,
			Term:   c.currentTerm,
			Entries: []PosLogEntry{
				{Pos: 2, Entry: entry1},
				{Pos: 3, Entry: entry2},
				{Pos: 4, Entry: entry3},
			},
			NextPos:   5,
			Committed: 1,
		}, accReq)

		// put entries
		c.doHandleAccept(nodeID4, 2, 3, 4)
		c.doHandleAccept(nodeID5, 2, 3, 4)

		// get again empty but not wait
		accReq = c.doGetAcceptReq(nodeID5, 5, 1)
		assert.Equal(t, AcceptEntriesInput{
			ToNode:    nodeID5,
			Term:      c.currentTerm,
			NextPos:   5,
			Committed: 4,
		}, accReq)
	})
}

func (c *coreLogicTest) doChangeMembers(newNodes []NodeID) {
	if err := c.core.ChangeMembership(c.ctx, c.currentTerm, newNodes); err != nil {
		panic("Change members should be OK, but got: " + err.Error())
	}
	c.core.CheckInvariant()
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
		Entries: []PosLogEntry{
			{Pos: 2, Entry: membersEntry},
			{Pos: 3, Entry: newCmdFunc("cmd data 01")},
			{Pos: 4, Entry: newCmdFunc("cmd data 02")},
			{Pos: 5, Entry: newCmdFunc("cmd data 03")},
		},
		NextPos:   6,
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
			Entries: []PosLogEntry{
				{Pos: 6, Entry: newCmdFunc("cmd data 04")},
			},
			NextPos:   7,
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
		Entries: []PosLogEntry{
			{Pos: 2, Entry: newCmdFunc("cmd data 01")},
			{Pos: 3, Entry: newCmdFunc("cmd data 02")},
			{Pos: 4, Entry: newCmdFunc("cmd data 03")},
		},
		NextPos:   5,
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
			NextPos:   5,
			Committed: 3,
		}, accReq)
	})

	// check accept req after committed pos = 3
	acceptReq = c.doGetAcceptReq(nodeID1, 0, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 4, Entry: newCmdFunc("cmd data 03")},
		},
		NextPos:   5,
		Committed: 3,
	}, acceptReq)
}

func (c *coreLogicTest) doStartElection() {
	if err := c.core.StartElection(0); err != nil {
		panic("Should start election OK, but got: " + err.Error())
	}
	c.core.CheckInvariant()
}

func TestCoreLogic__Candidate__Recv_Higher_Accept_Req_Term(t *testing.T) {
	c := newCoreLogicTest(t)
	c.doStartElection()

	accReq := c.doGetAcceptReq(nodeID3, 2, 1)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID3,
		Term:      c.currentTerm,
		NextPos:   2,
		Committed: 1,
	}, accReq)

	assert.Equal(t, false, c.core.GetChoosingLeaderInfo().NoActiveLeader)

	synctest.Test(t, func(t *testing.T) {
		acceptResult, _ := testutil.RunAsync(t, func() error {
			_, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID3, 2, 1)
			return err
		})

		newTerm := TermNum{
			Num:    22,
			NodeID: nodeID2,
		}
		c.core.FollowerReceiveTermNum(newTerm)
		assert.Equal(t, false, c.core.GetChoosingLeaderInfo().NoActiveLeader)

		assert.Equal(t, errors.New("expected state is 'Candidate' or 'Leader', got: 'Follower'"), acceptResult())

		// check state
		assert.Equal(t, StateFollower, c.core.GetState())

		// check runners
		assert.Equal(t, newTerm, c.runner.VoteTerm)
		assert.Equal(t, []NodeID{}, c.runner.VoteRunners)

		assert.Equal(t, newTerm, c.runner.AcceptTerm)
		assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)

		assert.Equal(t, newTerm, c.runner.StateMachineTerm)
		assert.Equal(t, StateMachineRunnerInfo{
			Running: true,
		}, c.runner.StateMachineInfo)

		assert.Equal(t, newTerm, c.runner.FetchFollowerTerm)
		assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	})
}

func TestCoreLogic__Follower__Recv_Higher_Accept_Req_Term(t *testing.T) {
	c := newCoreLogicTest(t)
	assert.Equal(t, true, c.core.GetChoosingLeaderInfo().NoActiveLeader)

	newTerm := TermNum{
		Num:    22,
		NodeID: nodeID2,
	}

	c.core.FollowerReceiveTermNum(newTerm)
	// check follower runner
	assert.Equal(t, newTerm, c.runner.FetchFollowerTerm)
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, false, c.core.GetChoosingLeaderInfo().NoActiveLeader)
}

func TestCoreLogic__Candidate__Recv_Lower_Term(t *testing.T) {
	c := newCoreLogicTest(t)
	c.doStartElection()

	newTerm := TermNum{
		Num:    17,
		NodeID: nodeID2,
	}
	affected := c.core.FollowerReceiveTermNum(newTerm)
	assert.Equal(t, false, affected)

	// no change in state
	assert.Equal(t, StateCandidate, c.core.GetState())

	// same term
	affected = c.core.FollowerReceiveTermNum(c.currentTerm)
	assert.Equal(t, false, affected)

	// no change in state
	assert.Equal(t, StateCandidate, c.core.GetState())
}

func (c *coreLogicTest) doUpdateFullyReplicated(nodeID NodeID, pos LogPos) {
	input := NeedReplicatedInput{
		Term:     c.currentTerm,
		FromNode: nodeID,

		FullyReplicated: pos,
	}
	if _, err := c.core.GetNeedReplicatedLogEntries(input); err != nil {
		panic("Should update OK, but got: " + err.Error())
	}
	c.core.CheckInvariant()
}

func (c *coreLogicTest) doUpdateFullyReplicatedWithErr(nodeID NodeID, pos LogPos) error {
	input := NeedReplicatedInput{
		Term:     c.currentTerm,
		FromNode: nodeID,

		FullyReplicated: pos,
	}
	_, err := c.core.GetNeedReplicatedLogEntries(input)
	return err
}

func TestCoreLogic__Leader__Change_Membership__Update_Fully_Replicated__Finish_Membership_Change(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doInsertCmd("cmd 01", "cmd 02")

	err := c.core.ChangeMembership(c.ctx, c.currentTerm, []NodeID{
		nodeID3, nodeID4, nodeID5,
	})
	assert.Equal(t, nil, err)

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
		NextPos:   5,
		Committed: 4,
	}, accReq)

	// not yet finish change
	c.doUpdateFullyReplicated(nodeID4, 4)
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5,
	}, c.runner.AcceptRunners)

	// finish member change
	c.doUpdateFullyReplicated(nodeID2, 4)
	assert.Equal(t, []NodeID{
		nodeID3, nodeID4, nodeID5,
	}, c.runner.AcceptRunners)

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
		Entries: []PosLogEntry{
			{Pos: 5, Entry: newMembers},
		},
		NextPos:   6,
		Committed: 4,
	}, accReq)
}

func TestCoreLogic__Leader__Fully_Replicated_Faster_Than_Last_Committed(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doInsertCmd("cmd 01", "cmd 02")

	err := c.core.ChangeMembership(c.ctx, c.currentTerm, []NodeID{
		nodeID3, nodeID4, nodeID5,
	})
	assert.Equal(t, nil, err)

	c.doUpdateFullyReplicated(nodeID2, 4)
	c.doUpdateFullyReplicated(nodeID3, 4)
	c.doUpdateFullyReplicated(nodeID4, 4)
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2,
		nodeID3, nodeID4, nodeID5,
	}, c.runner.AcceptRunners)

	// check accept entries
	accReq := c.doGetAcceptReq(nodeID5, 5, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID5,
		Term:      c.currentTerm,
		NextPos:   5,
		Committed: 1,
	}, accReq)

	c.doHandleAccept(nodeID1, 2, 3, 4)
	c.doHandleAccept(nodeID3, 2, 3, 4)
	c.doHandleAccept(nodeID4, 2, 3, 4)
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())

	assert.Equal(t, []NodeID{
		nodeID3, nodeID4, nodeID5,
	}, c.runner.AcceptRunners)

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
		Entries: []PosLogEntry{
			{Pos: 5, Entry: newMembers},
		},
		NextPos:   6,
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
		NextPos:   3,
		Committed: 2,
	}, accReq)
}

func TestCoreLogic__Candidate__Change_Membership__Current_Leader_Not_In_MemberList(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	newMembers1 := []MemberInfo{
		{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		{Nodes: []NodeID{nodeID4}, CreatedAt: 4},
	}
	newMembers2 := []MemberInfo{
		{Nodes: []NodeID{nodeID4}, CreatedAt: 1},
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
	entry3 := c.newLogEntry("cmd data 03", 18) // pos = 4

	c.doHandleVoteResp(nodeID2, 2, true, entry1, entry2)
	c.doHandleVoteResp(nodeID3, 2, true)

	// state is still candidate
	assert.Equal(t, StateCandidate, c.core.GetState())

	assert.Equal(t, []NodeID{nodeID1, nodeID4}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4,
	}, c.runner.AcceptRunners)

	// handle for node 4
	c.doHandleVoteResp(nodeID4, 3, true, entry2, entry3)
	assert.Equal(t, StateLeader, c.core.GetState())

	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID4}, c.runner.AcceptRunners)

	// get accept requests
	entry1.Term = c.currentTerm.ToInf()
	entry2.Term = c.currentTerm.ToInf()
	entry3.Term = c.currentTerm.ToInf()

	accReq := c.doGetAcceptReq(nodeID4, 2, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID4,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 2, Entry: entry1},
			{Pos: 3, Entry: entry2},
			{Pos: 4, Entry: entry3},
		},
		NextPos:   5,
		Committed: 1,
	}, accReq)

	assert.Equal(t, StateLeader, c.core.GetState())

	// try to insert command
	err := c.core.InsertCommand(c.ctx, c.currentTerm, []byte("data test 01"))
	assert.Equal(t, errors.New("current leader is stopping"), err)

	// try to change membership again
	err = c.core.ChangeMembership(c.ctx, c.currentTerm, []NodeID{nodeID5, nodeID6})
	assert.Equal(t, errors.New("current leader is stopping"), err)

	// no log entries in mem log
	c.doHandleAccept(nodeID4, 2, 3, 4)
	assert.Equal(t, StateLeader, c.core.GetState())

	// fully replicated => switch to follower
	err = c.doUpdateFullyReplicatedWithErr(nodeID4, 4)
	assert.Equal(t, errors.New("current leader has just stepped down"), err)

	assert.Equal(t, StateFollower, c.core.GetState())
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)

	assert.Equal(t, c.currentTerm, c.runner.StateMachineTerm)
	assert.Equal(t, StateMachineRunnerInfo{
		Running: true,
	}, c.runner.StateMachineInfo)

	assert.Equal(t, c.currentTerm, c.runner.FetchFollowerTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.FetchFollowers)

	// try to insert command
	err = c.core.InsertCommand(c.ctx, c.currentTerm, []byte("data test 01"))
	assert.Equal(t, errors.New("expected state 'Leader', got: 'Follower'"), err)
}

func TestCoreLogic__Follower__Get_Vote_Req(t *testing.T) {
	c := newCoreLogicTest(t)

	req, err := c.core.GetVoteRequest(c.currentTerm, nodeID1)
	assert.Equal(t, errors.New("expected state 'Candidate', got: 'Follower'"), err)
	assert.Equal(t, RequestVoteInput{}, req)

	c.doStartElection()

	// after becoming candidate
	oldTerm := c.currentTerm
	oldTerm.Num--
	req, err = c.core.GetVoteRequest(oldTerm, nodeID1)
	assert.Equal(t, ErrMismatchTerm(oldTerm, c.currentTerm), err)
	assert.Equal(t, RequestVoteInput{}, req)

	// not found node id 6
	req, err = c.core.GetVoteRequest(c.currentTerm, nodeID6)
	assert.Equal(t, errors.New("node id '64060000000000000000000000000000' is not in current member list"), err)
	assert.Equal(t, RequestVoteInput{}, req)

	c.doHandleVoteResp(nodeID1, 2, true)

	// after remain pos = +infinity
	req, err = c.core.GetVoteRequest(c.currentTerm, nodeID1)
	assert.Equal(t, errors.New("remain pos of node id '64010000000000000000000000000000' is infinite"), err)
	assert.Equal(t, RequestVoteInput{}, req)
}

func TestCoreLogic__Leader__Get_Accept_Entries__Not_In_MemberList(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doInsertCmd("cmd 01", "cmd 02")

	req, err := c.core.GetAcceptEntriesRequest(c.ctx, c.currentTerm, nodeID4, 0, 0)
	assert.Equal(t, errors.New("node id '64040000000000000000000000000000' is not in current member list"), err)
	assert.Equal(t, AcceptEntriesInput{}, req)
}

func TestCoreLogic__Start_Election__When_Already_Leader(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	err := c.core.StartElection(0)
	assert.Equal(t, errors.New("expected state 'Follower', got: 'Leader'"), err)
}

func TestCoreLogic__Start_Election__Current_Node_Not_In_MemberList(t *testing.T) {
	c := newCoreLogicTest(t)

	// setup init members
	initEntry := LogEntry{
		Type: LogTypeMembership,
		Term: InfiniteTerm{},
		Members: []MemberInfo{
			{Nodes: []NodeID{nodeID2, nodeID3}, CreatedAt: 1},
		},
	}
	c.log.UpsertEntries([]PosLogEntry{
		{
			Pos:   2,
			Entry: initEntry,
		},
	}, nil)

	err := c.core.StartElection(0)
	assert.Equal(t, errors.New("current node is not in its membership config"), err)
}

func TestCoreLogic__Candidate__Handle_Vote_Inf_Multi_Times(t *testing.T) {
	c := newCoreLogicTest(t)

	// handle error
	err := c.core.HandleAcceptEntriesResponse(nodeID1, AcceptEntriesOutput{
		Success: true,
		Term:    c.currentTerm,
		PosList: []LogPos{2},
	})
	assert.Equal(t, errors.New("expected state is 'Candidate' or 'Leader', got: 'Follower'"), err)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd test 01", 19)

	c.doHandleVoteResp(nodeID1, 2, true)
	c.doHandleVoteResp(nodeID1, 2, true, entry1)
	c.doHandleVoteResp(nodeID2, 2, true)

	assert.Equal(t, StateLeader, c.core.GetState())

	req := c.doGetAcceptReq(nodeID3, 0, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID3,
		Term:      c.currentTerm,
		NextPos:   2,
		Committed: 1,
	}, req)
}

func TestCoreLogic__Leader__Handle_Accept_Response__Greater_Than_Max_Pos(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()

	c.doHandleAccept(nodeID1, 2)
}

func TestCoreLogic__Follower__Update_Fully_Replicated(t *testing.T) {
	c := newCoreLogicTest(t)

	input := NeedReplicatedInput{
		Term:            c.currentTerm,
		FromNode:        nodeID1,
		FullyReplicated: 1,
	}
	_, err := c.core.GetNeedReplicatedLogEntries(input)
	assert.Equal(t, errors.New("expected state is 'Candidate' or 'Leader', got: 'Follower'"), err)
}

func TestCoreLogic__Leader__Get_Need_Replicated__Not_In_Member(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	input := NeedReplicatedInput{
		Term:            c.currentTerm,
		FromNode:        nodeID4,
		FullyReplicated: 1,
	}
	_, err := c.core.GetNeedReplicatedLogEntries(input)
	assert.Equal(t, errors.New("node id '64040000000000000000000000000000' is not in current member list"), err)
}

func TestCoreLogic__Candidate__Update_Fully_Replicated__Finish_Member_Change(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	newMembers := []MemberInfo{
		{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 2},
	}

	entry1 := LogEntry{
		Type: LogTypeMembership,
		Term: TermNum{
			Num:    19,
			NodeID: nodeID3,
		}.ToInf(),
		Members: newMembers,
	}

	c.doHandleVoteResp(nodeID1, 2, false, entry1)
	c.doHandleVoteResp(nodeID2, 2, false, LogEntry{})

	// do get accept req
	accReq := c.doGetAcceptReq(nodeID3, 0, 0)
	entry1.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID3,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 2, Entry: entry1},
		},
		NextPos:   3,
		Committed: 1,
	}, accReq)

	// handle accept
	c.doHandleAccept(nodeID1, 2)
	c.doHandleAccept(nodeID2, 2)
	assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

	c.doHandleAccept(nodeID4, 2)
	c.doHandleAccept(nodeID5, 2)
	assert.Equal(t, LogPos(2), c.core.GetLastCommitted())

	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	// insert to node 1 disk log
	c.insertToDiskLog(2, entry1)

	// fully replicated
	c.doUpdateFullyReplicated(nodeID1, 2)
	c.doUpdateFullyReplicated(nodeID2, 2)
	c.doUpdateFullyReplicated(nodeID4, 2)
	c.doUpdateFullyReplicated(nodeID5, 2)

	// get accept req again
	accReq = c.doGetAcceptReq(nodeID3, 0, 0)
	assert.Equal(t, AcceptEntriesInput{
		ToNode:    nodeID3,
		Term:      c.currentTerm,
		NextPos:   3,
		Committed: 2,
	}, accReq)

	// switch to leader state
	c.doHandleVoteResp(nodeID1, 3, true)
	c.doHandleVoteResp(nodeID2, 3, true)
	c.doHandleVoteResp(nodeID4, 3, true)
	c.doHandleVoteResp(nodeID5, 3, true)
	assert.Equal(t, StateLeader, c.core.GetState())

	// finally finish member change
	assert.Equal(t, []NodeID{
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	// get accept req again, node6
	accReq = c.doGetAcceptReq(nodeID6, 0, 0)

	newMembers2 := []MemberInfo{
		{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 1},
	}
	entry2 := LogEntry{
		Type:    LogTypeMembership,
		Term:    c.currentTerm.ToInf(),
		Members: newMembers2,
	}
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID6,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 3, Entry: entry2},
		},
		NextPos:   4,
		Committed: 2,
	}, accReq)
}

func TestCoreLogic__Start_Election__With_Max_Term_Value(t *testing.T) {
	c := newCoreLogicTest(t)

	err := c.core.StartElection(23)
	assert.Equal(t, nil, err)

	assert.Equal(t, TermNum{
		Num:    24,
		NodeID: c.persistent.GetNodeID(),
	}, c.runner.VoteTerm)
}

func TestCoreLogic__Candidate__Handle_Vote_Resp__Not_Success__Higher_Term(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	newTerm := TermNum{
		Num:    22,
		NodeID: nodeID2,
	}
	err := c.core.HandleVoteResponse(c.ctx, nodeID3, RequestVoteOutput{
		Success: false,
		Term:    newTerm,
	})
	assert.Equal(t, nil, err)

	assert.Equal(t, StateFollower, c.core.GetState())
	assert.Equal(t, newTerm, c.runner.AcceptTerm)
	assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)
}

func TestCoreLogic__Candidate__Handle_Vote_Resp__Not_Success__Lower_Term__Do_Nothing(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	newTerm := TermNum{
		Num:    15,
		NodeID: nodeID2,
	}
	err := c.core.HandleVoteResponse(c.ctx, nodeID3, RequestVoteOutput{
		Success: false,
		Term:    newTerm,
	})
	assert.Equal(t, nil, err)

	assert.Equal(t, StateCandidate, c.core.GetState())
	assert.Equal(t, c.currentTerm, c.runner.AcceptTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.AcceptRunners)
}

func TestCoreLogic__Leader__Handle_Accept_Resp__Not_Success__Higher_Term(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()

	newTerm := TermNum{
		Num:    22,
		NodeID: nodeID2,
	}
	err := c.core.HandleAcceptEntriesResponse(nodeID3, AcceptEntriesOutput{
		Success: false,
		Term:    newTerm,
	})
	assert.Equal(t, nil, err)

	assert.Equal(t, StateFollower, c.core.GetState())
	assert.Equal(t, newTerm, c.runner.AcceptTerm)
	assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)

	// handle failed resp again, when state = Follower
	newTerm.Num++
	err = c.core.HandleAcceptEntriesResponse(nodeID3, AcceptEntriesOutput{
		Success: false,
		Term:    newTerm,
	})
	assert.Equal(t, nil, err)
}

func TestAssertTrue(t *testing.T) {
	AssertTrue(true)
	assert.PanicsWithValue(t, "Should be true here", func() {
		AssertTrue(false)
	})
}

func (c *coreLogicTest) doGetNeedReplicated(nodeID NodeID, posList ...LogPos) AcceptEntriesInput {
	input, err := c.core.GetNeedReplicatedLogEntries(NeedReplicatedInput{
		Term:     c.currentTerm,
		FromNode: nodeID,
		PosList:  posList,

		FullyReplicated: 0,
	})
	if err != nil {
		panic(err)
	}
	return input
}

func TestCoreLogic__Leader__GetNeedReplicated(t *testing.T) {
	c := newCoreLogicTest(t)

	c.startAsLeader()

	c.doInsertCmd(
		"cmd data 02",
		"cmd data 03",
		"cmd data 04",
		"cmd data 05",
	)

	c.doHandleAccept(nodeID1, 2, 3, 4)
	c.doHandleAccept(nodeID2, 2, 3, 4)
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())

	input := c.doGetNeedReplicated(nodeID1, 2, 4, 5)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 2, Entry: c.newInfLogEntry("cmd data 02")},
			{Pos: 4, Entry: c.newInfLogEntry("cmd data 04")},
			{Pos: 5, Entry: LogEntry{}},
		},
	}, input)
}

func TestCoreLogic__Candidate__With_Max_Buffer_Len__Waiting(t *testing.T) {
	conf := newCoreTestConfig()
	conf.maxBufferLen = 3
	c := newCoreLogicTestWithConfig(t, conf)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18) // pos = 2
	entry2 := c.newLogEntry("cmd data 02", 18) // pos = 3
	entry3 := c.newLogEntry("cmd data 03", 18) // pos = 4
	entry4 := c.newLogEntry("cmd data 04", 18) // pos = 5

	c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2, entry3)

	synctest.Test(t, func(t *testing.T) {
		finishFn, assertNotFinish := testutil.RunAsync(t, func() bool {
			c.doHandleVoteResp(nodeID1, 5, true, entry4)
			return true
		})

		c.doHandleVoteResp(nodeID2, 2, true)
		assertNotFinish()

		c.doHandleAccept(nodeID1, 2)
		assertNotFinish()
		assert.Equal(t, LogPos(1), c.core.GetLastCommitted())

		c.doHandleAccept(nodeID2, 2)
		assertNotFinish()
		assert.Equal(t, LogPos(2), c.core.GetLastCommitted())
		assert.Equal(t, LogPos(2), c.core.GetMinBufferLogPos())

		// update other node id
		c.doUpdateFullyReplicated(nodeID2, 2)
		assertNotFinish()
		assert.Equal(t, LogPos(2), c.core.GetMinBufferLogPos())

		c.insertToDiskLog(2, entry1)
		c.doUpdateFullyReplicated(nodeID1, 2)
		assert.Equal(t, true, finishFn())
		assert.Equal(t, LogPos(2), c.core.GetLastCommitted())
		assert.Equal(t, LogPos(3), c.core.GetMinBufferLogPos())
	})
}

func TestCoreLogic__Candidate__With_Max_Buffer_Len__Waiting__State_Change_To_Follower(t *testing.T) {
	conf := newCoreTestConfig()
	conf.maxBufferLen = 3
	c := newCoreLogicTestWithConfig(t, conf)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd data 01", 18)
	entry2 := c.newLogEntry("cmd data 02", 18)
	entry3 := c.newLogEntry("cmd data 03", 18)
	entry4 := c.newLogEntry("cmd data 04", 18)

	c.doHandleVoteResp(nodeID1, 2, false, entry1, entry2, entry3)

	synctest.Test(t, func(t *testing.T) {
		finishFn, _ := testutil.RunAsync(t, func() bool {
			assert.PanicsWithValue(t, "expected state 'Candidate', got: 'Follower'", func() {
				c.doHandleVoteResp(nodeID1, 5, true, entry4)
			})
			return true
		})

		newTerm := TermNum{
			Num:    c.currentTerm.Num + 1,
			NodeID: nodeID2,
		}
		err := c.core.HandleAcceptEntriesResponse(nodeID1, AcceptEntriesOutput{
			Success: false,
			Term:    newTerm,
		})
		assert.Equal(t, nil, err)

		// unblocked
		assert.Equal(t, true, finishFn())
	})
}

func TestCoreLogic__Leader__Insert_Cmd__With_Waiting(t *testing.T) {
	conf := newCoreTestConfig()
	conf.maxBufferLen = 3
	c := newCoreLogicTestWithConfig(t, conf)

	c.startAsLeader()

	c.doInsertCmd(
		"cmd test 01",
		"cmd test 02",
	)

	synctest.Test(t, func(t *testing.T) {
		finishFn, assertNotFinish := testutil.RunAsync(t, func() bool {
			c.doInsertCmd(
				"cmd test 03",
				"cmd test 04",
			)
			return true
		})

		c.doHandleAccept(nodeID1, 2)
		c.doHandleAccept(nodeID2, 2)
		assert.Equal(t, LogPos(2), c.core.GetLastCommitted())
		assertNotFinish()

		c.insertToDiskLog(2, c.newAcceptLogEntry("cmd test 01"))
		c.doUpdateFullyReplicated(nodeID1, 2)
		assert.Equal(t, true, finishFn())

		accReq := c.doGetAcceptReq(nodeID3, 0, 0)
		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID3,
			Term:   c.currentTerm,
			Entries: []PosLogEntry{
				{Pos: 3, Entry: c.newAcceptLogEntry("cmd test 02")},
				{Pos: 4, Entry: c.newAcceptLogEntry("cmd test 03")},
				{Pos: 5, Entry: c.newAcceptLogEntry("cmd test 04")},
			},
			NextPos:   6,
			Committed: 2,
		}, accReq)
	})
}

func TestCoreLogic__Leader__Insert_Cmd__Waiting__Context_Cancel(t *testing.T) {
	conf := newCoreTestConfig()
	conf.maxBufferLen = 3
	c := newCoreLogicTestWithConfig(t, conf)

	c.startAsLeader()

	c.doInsertCmd(
		"cmd test 01",
		"cmd test 02",
		"cmd test 03",
	)

	err := c.core.InsertCommand(c.cancelCtx, c.currentTerm, []byte("cmd test 04"))
	assert.Equal(t, context.Canceled, err)
}

func (c *coreLogicTest) doHandleLeaderInfo(node NodeID, info ChooseLeaderInfo) {
	err := c.core.HandleChoosingLeaderInfo(node, c.persistent.GetLastTerm(), info)
	if err != nil {
		panic(err)
	}
}

func TestCoreLogic__Follower__HandleChoosingLeaderInfo(t *testing.T) {
	c := newCoreLogicTest(t)

	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.FetchFollowers)
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.FetchFollowerTerm)

	assert.Equal(t, false, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(0), c.runner.ElectionTerm)

	assert.Equal(t, TimestampMilli(20_000), c.core.GetFollowerWakeUpAt())

	info := c.core.GetChoosingLeaderInfo()

	assert.Equal(t, ChooseLeaderInfo{
		NoActiveLeader:  true,
		Members:         c.log.GetCommittedInfo().Members,
		FullyReplicated: 1,
		LastTermVal:     20,
	}, info)

	c.now.Add(50)

	// first handle leader info
	c.doHandleLeaderInfo(nodeID1, info)
	assert.Equal(t, TimestampMilli(20_000), c.core.GetFollowerWakeUpAt())

	// check runners
	assert.Equal(t, []NodeID{nodeID2, nodeID3}, c.runner.FetchFollowers)
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.FetchFollowerTerm)
	assert.Equal(t, 1, c.runner.FetchRetryCount)
	assert.Equal(t, false, c.runner.ElectionStarted)

	// second handle leader info
	c.doHandleLeaderInfo(nodeID2, info)
	assert.Equal(t, TimestampMilli(20_050), c.core.GetFollowerWakeUpAt())

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.FetchFollowerTerm)
	assert.Equal(t, 0, c.runner.FetchRetryCount)

	assert.Equal(t, true, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(20), c.runner.ElectionTerm)
	assert.Equal(t, 1, c.runner.ElectionRetryCount)
	assert.Equal(t, nodeID1, c.runner.ElectionChosen)
}

func TestCoreLogic__Candidate__Check_Start_Election_Runner(t *testing.T) {
	c := newCoreLogicTest(t)

	assert.Equal(t, false, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(0), c.runner.ElectionTerm)

	c.doStartElection()

	assert.Equal(t, false, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(0), c.runner.ElectionTerm)
}

func TestCoreLogic__Follower__HandleChoosingLeaderInfo__Choose_Highest_Replicated_Pos(t *testing.T) {
	c := newCoreLogicTest(t)

	info := c.core.GetChoosingLeaderInfo()

	// first handle leader info
	c.doHandleLeaderInfo(nodeID1, info)

	// second handle leader info
	info.FullyReplicated = 3
	c.doHandleLeaderInfo(nodeID2, info)

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, c.persistent.GetLastTerm(), c.runner.FetchFollowerTerm)
	assert.Equal(t, 0, c.runner.FetchRetryCount)

	assert.Equal(t, true, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(20), c.runner.ElectionTerm)
	assert.Equal(t, 1, c.runner.ElectionRetryCount)
	assert.Equal(t, nodeID2, c.runner.ElectionChosen)

	c.core.FollowerReceiveTermNum(c.currentTerm)

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, c.currentTerm, c.runner.FetchFollowerTerm)
	assert.Equal(t, 0, c.runner.FetchRetryCount)

	assert.Equal(t, false, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(0), c.runner.ElectionTerm)
	assert.Equal(t, 0, c.runner.ElectionRetryCount)
	assert.Equal(t, NodeID{}, c.runner.ElectionChosen)

	// after 8000 ms
	c.now.Add(8000)
	c.core.CheckTimeout()
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)

	// after 11,000 ms timeout
	c.now.Add(3000)
	c.core.CheckTimeout()
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.FetchFollowers)
	assert.Equal(t, false, c.runner.ElectionStarted)
}

func TestCoreLogic__Follower__HandleChoosingLeaderInfo__Not_Choose_Node_Not_In_Membership(t *testing.T) {
	c := newCoreLogicTest(t)

	info1 := c.core.GetChoosingLeaderInfo()
	c.doHandleLeaderInfo(nodeID1, info1)

	// second handle leader info
	info2 := ChooseLeaderInfo{
		NoActiveLeader: true,
		Members: []MemberInfo{
			{
				Nodes:     []NodeID{nodeID4, nodeID5, nodeID6},
				CreatedAt: 4,
			},
		},
		FullyReplicated: 5,
		LastTermVal:     23,
	}
	c.doHandleLeaderInfo(nodeID2, info2)

	// check runners
	assert.Equal(t, []NodeID{
		nodeID4, nodeID5, nodeID6,
	}, c.runner.FetchFollowers)
	assert.Equal(t, false, c.runner.ElectionStarted)

	info3 := info2
	info3.FullyReplicated = 4
	c.doHandleLeaderInfo(nodeID4, info3)
	c.doHandleLeaderInfo(nodeID5, info3)

	// check runners
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, true, c.runner.ElectionStarted)
	assert.Equal(t, 1, c.runner.ElectionRetryCount)
	assert.Equal(t, nodeID4, c.runner.ElectionChosen)
}

func TestCoreLogic__Retry__Handle_Leader_Info(t *testing.T) {
	c := newCoreLogicTest(t)

	info := c.core.GetChoosingLeaderInfo()
	c.doHandleLeaderInfo(nodeID1, info)

	// then timeout
	c.now.Add(10_100)
	c.core.CheckTimeout()

	c.doHandleLeaderInfo(nodeID2, info)
	assert.Equal(t, []NodeID{nodeID1, nodeID3}, c.runner.FetchFollowers)
	assert.Equal(t, 2, c.runner.FetchRetryCount)

	// final handle
	c.doHandleLeaderInfo(nodeID3, info)
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, true, c.runner.ElectionStarted)
	assert.Equal(t, 2, c.runner.ElectionRetryCount)

	// error
	err := c.core.HandleChoosingLeaderInfo(nodeID1, c.persistent.GetLastTerm(), info)
	assert.Equal(t, errors.New("check status is not running, got: 2"), err)
}

func TestCoreLogic__Handle_Leader_Info__With_New_Members(t *testing.T) {
	c := newCoreLogicTest(t)

	info := c.core.GetChoosingLeaderInfo()
	c.doHandleLeaderInfo(nodeID1, info)

	info2 := ChooseLeaderInfo{
		NoActiveLeader:  true,
		FullyReplicated: 2,
		Members: []MemberInfo{
			{CreatedAt: 1, Nodes: []NodeID{nodeID4}},
		},
	}

	c.doHandleLeaderInfo(nodeID3, info2)
	assert.Equal(t, []NodeID{nodeID4}, c.runner.FetchFollowers)
	assert.Equal(t, false, c.runner.ElectionStarted)

	// finally can switch to starting new leader
	info3 := info2
	info3.FullyReplicated = 3
	c.doHandleLeaderInfo(nodeID4, info3)
	assert.Equal(t, []NodeID{}, c.runner.FetchFollowers)
	assert.Equal(t, true, c.runner.ElectionStarted)
	assert.Equal(t, nodeID4, c.runner.ElectionChosen)
}

func TestCoreLogic__Handle_Leader_Info__Invalid_Term(t *testing.T) {
	c := newCoreLogicTest(t)

	info := c.core.GetChoosingLeaderInfo()

	lowerTerm := c.persistent.GetLastTerm()
	lowerTerm.Num--
	err := c.core.HandleChoosingLeaderInfo(nodeID1, lowerTerm, info)
	assert.Equal(t, ErrMismatchTerm(lowerTerm, c.persistent.GetLastTerm()), err)
}

func TestCoreLogic__Leader__Get_Need_Replicated__From_Disk(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doInsertCmd(
		"cmd test 02",
		"cmd test 03",
		"cmd test 04",
	)

	input1 := c.doGetAcceptReq(nodeID1, 0, 0)
	for i := range input1.Entries {
		input1.Entries[i].Entry.Term = InfiniteTerm{}
	}
	c.log.UpsertEntries(input1.Entries, nil)

	c.doHandleAccept(nodeID1, 2, 3, 4)
	c.doHandleAccept(nodeID2, 2, 3, 4)
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())
	assert.Equal(t, LogPos(2), c.core.GetMinBufferLogPos())

	c.doUpdateFullyReplicated(nodeID1, 2)
	assert.Equal(t, LogPos(3), c.core.GetMinBufferLogPos())

	input2 := c.doGetNeedReplicated(nodeID1, 1, 2, 3)
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{
				Pos: 1,
				Entry: NewMembershipLogEntry(
					InfiniteTerm{},
					[]MemberInfo{
						{CreatedAt: 1, Nodes: []NodeID{nodeID1, nodeID2, nodeID3}},
					},
				),
			},
			{
				Pos:   2,
				Entry: c.newInfLogEntry("cmd test 02"),
			},
			{
				Pos:   3,
				Entry: c.newInfLogEntry("cmd test 03"),
			},
		},
	}, input2)
}

func (c *coreLogicTest) doGetCommitted(from LogPos, limit int) GetCommittedEntriesOutput {
	output, err := c.core.GetCommittedEntriesWithWait(c.ctx, c.currentTerm, from, limit)
	if err != nil {
		panic(err)
	}
	return output
}

func TestCoreLogic__Leader__GetEntriesWithWait(t *testing.T) {
	c := newCoreLogicTest(t)

	// get when is follower => error
	_, err := c.core.GetCommittedEntriesWithWait(c.ctx, c.currentTerm, 1, 100)
	assert.Equal(t, errors.New("expected state is 'Candidate' or 'Leader', got: 'Follower'"), err)

	c.startAsLeader()

	// get
	output := c.doGetCommitted(1, 100)
	members := []MemberInfo{
		{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
	}
	assert.Equal(t, GetCommittedEntriesOutput{
		Entries: []PosLogEntry{
			{Pos: 1, Entry: NewMembershipLogEntry(InfiniteTerm{}, members)},
		},
		NextPos: 2,
	}, output)

	// insert and commit
	c.doInsertCmd(
		"cmd test 02",
		"cmd test 03",
		"cmd test 04",
		"cmd test 05",
	)
	c.doHandleAccept(nodeID1, 2, 3, 4)
	c.doHandleAccept(nodeID2, 2, 3, 4)
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())

	// get again
	output = c.doGetCommitted(1, 100)
	assert.Equal(t, GetCommittedEntriesOutput{
		Entries: []PosLogEntry{
			{Pos: 1, Entry: NewMembershipLogEntry(InfiniteTerm{}, members)},
			{Pos: 2, Entry: c.newInfLogEntry("cmd test 02")},
			{Pos: 3, Entry: c.newInfLogEntry("cmd test 03")},
			{Pos: 4, Entry: c.newInfLogEntry("cmd test 04")},
		},
		NextPos: 5,
	}, output)

	// get again with limit
	output = c.doGetCommitted(1, 3)
	assert.Equal(t, GetCommittedEntriesOutput{
		Entries: []PosLogEntry{
			{Pos: 1, Entry: NewMembershipLogEntry(InfiniteTerm{}, members)},
			{Pos: 2, Entry: c.newInfLogEntry("cmd test 02")},
			{Pos: 3, Entry: c.newInfLogEntry("cmd test 03")},
		},
		NextPos: 4,
	}, output)

	// get again with limit, in mem only
	output = c.doGetCommitted(2, 2)
	assert.Equal(t, GetCommittedEntriesOutput{
		Entries: []PosLogEntry{
			{Pos: 2, Entry: c.newInfLogEntry("cmd test 02")},
			{Pos: 3, Entry: c.newInfLogEntry("cmd test 03")},
		},
		NextPos: 4,
	}, output)

	// inc fully replicated
	c.log.UpsertEntries(
		[]PosLogEntry{
			{Pos: 2, Entry: c.newInfLogEntry("cmd test 02")},
			{Pos: 3, Entry: c.newInfLogEntry("cmd test 03")},
			{Pos: 4, Entry: c.newInfLogEntry("cmd test 04")},
		},
		nil,
	)
	c.doUpdateFullyReplicated(nodeID1, 3)

	// get full again
	output = c.doGetCommitted(1, 100)
	assert.Equal(t, GetCommittedEntriesOutput{
		Entries: []PosLogEntry{
			{Pos: 1, Entry: NewMembershipLogEntry(InfiniteTerm{}, members)},
			{Pos: 2, Entry: c.newInfLogEntry("cmd test 02")},
			{Pos: 3, Entry: c.newInfLogEntry("cmd test 03")},
			{Pos: 4, Entry: c.newInfLogEntry("cmd test 04")},
		},
		NextPos: 5,
	}, output)
}

func TestCoreLogic__Leader__GetEntriesWithWait__Waiting(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	synctest.Test(t, func(t *testing.T) {
		// wait
		resultFn, _ := testutil.RunAsync(t, func() GetCommittedEntriesOutput {
			return c.doGetCommitted(2, 100)
		})

		c.doInsertCmd(
			"cmd test 02",
			"cmd test 03",
			"cmd test 04",
		)
		c.doHandleAccept(nodeID2, 2, 3)
		c.doHandleAccept(nodeID3, 2, 3)

		assert.Equal(t, GetCommittedEntriesOutput{
			Entries: []PosLogEntry{
				{Pos: 2, Entry: c.newInfLogEntry("cmd test 02")},
				{Pos: 3, Entry: c.newInfLogEntry("cmd test 03")},
			},
			NextPos: 4,
		}, resultFn())
	})
}

func TestCoreLogic__Leader__GetEntriesWithWait__Waiting__Context_Cancel(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	_, err := c.core.GetCommittedEntriesWithWait(c.cancelCtx, c.currentTerm, 2, 100)
	assert.Equal(t, context.Canceled, err)
}

func TestCoreLogic__Leader__Change_Membership_Waiting(t *testing.T) {
	conf := newCoreTestConfig()
	conf.maxBufferLen = 3
	c := newCoreLogicTestWithConfig(t, conf)

	c.startAsLeader()

	c.doInsertCmd(
		"cmd test 02",
		"cmd test 03",
		"cmd test 04",
	)

	synctest.Test(t, func(t *testing.T) {
		resultFn, assertNotFinish := testutil.RunAsync(t, func() error {
			c.doChangeMembers([]NodeID{
				nodeID4, nodeID5, nodeID6,
			})
			return nil
		})

		c.doHandleAccept(nodeID1, 2, 3)
		c.doHandleAccept(nodeID2, 2, 3)
		assertNotFinish()

		c.insertToDiskLog(2, c.newAcceptLogEntry("cmd test 02"))
		c.doUpdateFullyReplicated(nodeID1, 2)
		assert.Equal(t, nil, resultFn())

		accReq := c.doGetAcceptReq(nodeID3, 2, 0)
		members := []MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
			{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 5},
		}
		assert.Equal(t, AcceptEntriesInput{
			ToNode: nodeID3,
			Term:   c.currentTerm,
			Entries: []PosLogEntry{
				{Pos: 4, Entry: c.newAcceptLogEntry("cmd test 04")},
				{Pos: 5, Entry: NewMembershipLogEntry(c.currentTerm.ToInf(), members)},
			},
			NextPos:   6,
			Committed: 3,
		}, accReq)
	})
}

func TestCoreLogic__Leader__Change_Membership_Waiting__Context_Cancel(t *testing.T) {
	conf := newCoreTestConfig()
	conf.maxBufferLen = 3
	c := newCoreLogicTestWithConfig(t, conf)

	c.startAsLeader()

	c.doInsertCmd(
		"cmd test 02",
		"cmd test 03",
		"cmd test 04",
	)

	err := c.core.ChangeMembership(c.cancelCtx, c.currentTerm, []NodeID{
		nodeID4, nodeID5, nodeID6,
	})
	assert.Equal(t, context.Canceled, err)
}

func TestCoreLogic__Candidate__Change_Membership_3_Nodes__Current_Leader_Not_In_MemberList(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	newMembers1 := []MemberInfo{
		{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 2},
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

	c.doHandleVoteResp(nodeID2, 2, true, entry1, entry2)
	c.doHandleVoteResp(nodeID3, 2, true)

	// state is still candidate
	assert.Equal(t, StateCandidate, c.core.GetState())

	assert.Equal(t, []NodeID{nodeID1, nodeID4, nodeID5, nodeID6}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	// handle for node 4 & 5
	c.doHandleVoteResp(nodeID4, 3, true, entry2)
	c.doHandleVoteResp(nodeID5, 3, true)
	assert.Equal(t, StateLeader, c.core.GetState())

	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{nodeID4, nodeID5, nodeID6}, c.runner.AcceptRunners)

	// try to insert command
	err := c.core.InsertCommand(c.ctx, c.currentTerm, []byte("data test 01"))
	assert.Equal(t, errors.New("current leader is stopping"), err)

	// try to change membership again
	err = c.core.ChangeMembership(c.ctx, c.currentTerm, []NodeID{nodeID5, nodeID6})
	assert.Equal(t, errors.New("current leader is stopping"), err)

	// no log entries in mem log
	c.doHandleAccept(nodeID4, 2, 3)
	c.doHandleAccept(nodeID5, 2, 3)
	assert.Equal(t, LogPos(3), c.core.GetLastCommitted())
	assert.Equal(t, StateLeader, c.core.GetState())

	// fully replicated => switch to follower
	err = c.doUpdateFullyReplicatedWithErr(nodeID4, 3)
	assert.Equal(t, nil, err)
	err = c.doUpdateFullyReplicatedWithErr(nodeID5, 3)
	assert.Equal(t, errors.New("current leader has just stepped down"), err)

	assert.Equal(t, StateFollower, c.core.GetState())
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)

	assert.Equal(t, c.currentTerm, c.runner.StateMachineTerm)
	assert.Equal(t, StateMachineRunnerInfo{
		Running: true,
	}, c.runner.StateMachineInfo)

	assert.Equal(t, c.currentTerm, c.runner.FetchFollowerTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.FetchFollowers)
}

func TestCoreLogic__Candidate__Step_Down_When_No_Longer_In_Member_List__Recv_Replicated_First(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	newMembers1 := []MemberInfo{
		{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		{Nodes: []NodeID{nodeID4, nodeID5, nodeID6}, CreatedAt: 2},
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

	c.doHandleVoteResp(nodeID2, 2, true, entry1, entry2)
	c.doHandleVoteResp(nodeID3, 2, true)

	// state is still candidate
	assert.Equal(t, StateCandidate, c.core.GetState())

	assert.Equal(t, []NodeID{nodeID1, nodeID4, nodeID5, nodeID6}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}, c.runner.AcceptRunners)

	// handle for node 4 & 5
	c.doHandleVoteResp(nodeID4, 3, true, entry2)
	c.doHandleVoteResp(nodeID5, 3, true)
	assert.Equal(t, StateLeader, c.core.GetState())

	// fully replicated
	c.doUpdateFullyReplicated(nodeID4, 3)
	c.doUpdateFullyReplicated(nodeID5, 3)

	// no log entries in mem log => switch to follower
	c.doHandleAccept(nodeID4, 2, 3)
	err := c.doHandleAcceptWithErr(nodeID5, 2, 3)
	assert.Equal(t, errors.New("current leader has just stepped down"), err)
	assert.Equal(t, StateFollower, c.core.GetState())
	assert.Equal(t, []NodeID{}, c.runner.VoteRunners)
	assert.Equal(t, []NodeID{}, c.runner.AcceptRunners)

	assert.Equal(t, c.currentTerm, c.runner.StateMachineTerm)
	assert.Equal(t, StateMachineRunnerInfo{
		Running: true,
	}, c.runner.StateMachineInfo)

	assert.Equal(t, c.currentTerm, c.runner.FetchFollowerTerm)
	assert.Equal(t, []NodeID{nodeID1, nodeID2, nodeID3}, c.runner.FetchFollowers)
}

func TestCoreLogic__Leader__Change_Membership__Current_Leader_Step_Down__Fast_Switch(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doChangeMembers([]NodeID{nodeID4, nodeID5, nodeID6})
	accReq := c.doGetAcceptReq(nodeID1, 2, 0)
	logEntry := accReq.Entries[0]
	logEntry.Entry.Term = InfiniteTerm{}

	c.doHandleAccept(nodeID1, 2)
	c.doHandleAccept(nodeID2, 2)
	c.doHandleAccept(nodeID4, 2)
	c.doHandleAccept(nodeID5, 2)

	assert.Equal(t, LogPos(2), c.core.GetLastCommitted())
	// put to log storage
	c.log.UpsertEntries([]PosLogEntry{logEntry}, nil)

	c.doUpdateFullyReplicated(nodeID1, 2)
	c.doUpdateFullyReplicated(nodeID2, 2)
	c.doUpdateFullyReplicated(nodeID4, 2)
	c.doUpdateFullyReplicated(nodeID5, 2)

	accReq = c.doGetAcceptReq(nodeID4, 3, 0)
	finishEntry := accReq.Entries[0]
	finishEntry.Entry.Term = InfiniteTerm{}

	c.doHandleAccept(nodeID4, 3)
	c.doHandleAccept(nodeID5, 3)
	assert.Equal(t, LogPos(3), c.core.GetLastCommitted())

	// put to log storage
	// c.log.UpsertEntries([]PosLogEntry{finishEntry}, nil)

	c.doUpdateFullyReplicated(nodeID4, 3)
	err := c.doUpdateFullyReplicatedWithErr(nodeID5, 3)
	assert.Equal(t, errors.New("current leader has just stepped down"), err)

	// -------------------------------------
	// Fast Leader Switch
	// -------------------------------------
	info := c.core.GetChoosingLeaderInfo()
	assert.Equal(t, ChooseLeaderInfo{
		NoActiveLeader:  true,
		Members:         logEntry.Entry.Members,
		FullyReplicated: 2,
		LastTermVal:     21,
	}, info)

	info2 := ChooseLeaderInfo{
		NoActiveLeader:  false,
		Members:         finishEntry.Entry.Members,
		FullyReplicated: 3,
		LastTermVal:     21,
	}
	c.doHandleLeaderInfo(nodeID4, info2)
	c.doHandleLeaderInfo(nodeID5, info2)

	assert.Equal(t, true, c.runner.ElectionStarted)
	assert.Equal(t, TermValue(21), c.runner.ElectionTerm)
	assert.Equal(t, nodeID4, c.runner.ElectionChosen)
	assert.Equal(t, 2, c.runner.ElectionRetryCount)
}

func TestCoreLogic__Follower__Handle_Info_With_Active_Leader(t *testing.T) {
	c := newCoreLogicTest(t)

	info := ChooseLeaderInfo{
		NoActiveLeader:  false,
		Members:         c.log.GetCommittedInfo().Members,
		FullyReplicated: 3,
		LastTermVal:     23,
	}
	c.doHandleLeaderInfo(nodeID1, info)
	c.doHandleLeaderInfo(nodeID2, info)

	assert.Equal(t, false, c.runner.ElectionStarted)
}

func TestCoreLogic__Candidate__Vote_Resp_Empty_Entry(t *testing.T) {
	c := newCoreLogicTest(t)

	c.doStartElection()

	entry1 := c.newLogEntry("cmd test 01", 18)
	c.doHandleVoteResp(nodeID1, 2, true, LogEntry{}, entry1)
	c.doHandleVoteResp(nodeID2, 2, true)

	req := c.doGetAcceptReq(nodeID1, 0, 0)

	noopEntry := NewNoOpLogEntry()
	noopEntry.Term = c.currentTerm.ToInf()
	entry1.Term = c.currentTerm.ToInf()
	assert.Equal(t, AcceptEntriesInput{
		ToNode: nodeID1,
		Term:   c.currentTerm,
		Entries: []PosLogEntry{
			{Pos: 2, Entry: noopEntry},
			{Pos: 3, Entry: entry1},
		},
		NextPos:   4,
		Committed: 1,
	}, req)
}

func TestCoreLogic__Leader__Finish_Membership__Increase_Last_Committed(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	c.doChangeMembers([]NodeID{nodeID4})

	c.doInsertCmd(
		"cmd test 03",
		"cmd test 04",
	)

	accReq := c.doGetAcceptReq(nodeID4, 1, 0)
	memberEntry := accReq.Entries[0]
	memberEntry.Entry.Term = InfiniteTerm{}

	c.doHandleAccept(nodeID1, 2)
	c.doHandleAccept(nodeID2, 2)
	c.doHandleAccept(nodeID4, 2, 3, 4)
	assert.Equal(t, LogPos(2), c.core.GetLastCommitted())

	c.log.UpsertEntries([]PosLogEntry{memberEntry}, nil)

	c.doUpdateFullyReplicated(nodeID1, 2)
	assert.Equal(t, LogPos(2), c.core.GetReplicatedPosTest(nodeID1))

	c.doUpdateFullyReplicated(nodeID2, 2)
	c.doUpdateFullyReplicated(nodeID4, 2)
	assert.Equal(t, LogPos(5), c.core.GetMaxLogPos())
	assert.Equal(t, LogPos(4), c.core.GetLastCommitted())
}

func TestCoreLogic__Leader__Change_Membership__Duplicated(t *testing.T) {
	c := newCoreLogicTest(t)
	c.startAsLeader()

	// do change
	err := c.core.ChangeMembership(c.ctx, c.currentTerm, []NodeID{nodeID4, nodeID5, nodeID5})
	assert.Equal(t, errors.New("duplicated node id: 64050000000000000000000000000000"), err)

	// do change empty
	err = c.core.ChangeMembership(c.ctx, c.currentTerm, nil)
	assert.Equal(t, errors.New("can not change membership to empty"), err)
}

func TestCoreLogic__Candidate__Handle_Vote__Not_In_MemberList(t *testing.T) {
	c := newCoreLogicTest(t)
	c.doStartElection()

	voteOutput := RequestVoteOutput{
		Success: true,
		Term:    c.currentTerm,
		Entries: []VoteLogEntry{
			{
				Pos:     2,
				IsFinal: true,
			},
		},
	}

	// handle vote response
	err := c.core.HandleVoteResponse(c.ctx, nodeID4, voteOutput)
	assert.Equal(t, errors.New("node id '64040000000000000000000000000000' is not in current member list"), err)
}
