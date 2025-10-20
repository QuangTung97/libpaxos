package paxos_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
	"github.com/QuangTung97/libpaxos/paxos/testutil"
)

type acceptorLogicTest struct {
	ctx         context.Context
	log         *fake.LogStorageFake
	logic       AcceptorLogic
	currentTerm TermNum
}

func newAcceptorLogicTest(t *testing.T) *acceptorLogicTest {
	s := &acceptorLogicTest{}
	s.ctx = context.Background()
	s.log = &fake.LogStorageFake{}

	s.currentTerm = TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	s.putMembers()
	s.initLogic(100)

	t.Cleanup(func() {
		s.logic.CheckInvariant()
	})

	return s
}

func (*acceptorLogicTest) initMemberLogEntry() LogEntry {
	return NewMembershipLogEntry(
		1,
		InfiniteTerm{},
		[]MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		},
	)
}

func (s *acceptorLogicTest) putMembers() {
	s.log.UpsertEntriesV1([]PosLogEntry{
		{Pos: 1, Entry: s.initMemberLogEntry()},
	}, nil)
}

func (s *acceptorLogicTest) initLogic(limit int) {
	s.logic = NewAcceptorLogic(nodeID2, s.log, limit)
}

func (s *acceptorLogicTest) doHandleVote(term TermNum, fromPos LogPos) []RequestVoteOutput {
	it, err := s.logic.HandleRequestVote(RequestVoteInput{
		ToNode:  nodeID2,
		Term:    term,
		FromPos: fromPos,
	})
	if err != nil {
		panic("Should handle req vote ok, but got: " + err.Error())
	}

	var outputs []RequestVoteOutput
	for x := range it {
		outputs = append(outputs, x)
	}
	return outputs
}

func newPosLogEntries(from LogPos, entries ...LogEntry) []PosLogEntry {
	result := make([]PosLogEntry, 0, len(entries))
	for index, e := range entries {
		pos := from + LogPos(index)
		AssertTrue(e.Pos == pos)
		result = append(result, PosLogEntry{
			Pos:   pos,
			Entry: e,
		})
	}
	return result
}

func newAcceptLogEntries(from LogPos, entries ...LogEntry) []LogEntry {
	result := make([]LogEntry, 0, len(entries))
	for index, e := range entries {
		pos := from + LogPos(index)
		AssertTrue(e.Pos == pos)
		result = append(result, e)
	}
	return result
}

func (s *acceptorLogicTest) newCmd(pos LogPos, cmd string) LogEntry {
	return NewCmdLogEntry(pos, s.currentTerm.ToInf(), []byte(cmd))
}

func (s *acceptorLogicTest) newCmdInf(pos LogPos, cmd string) LogEntry {
	return NewCmdLogEntry(pos, InfiniteTerm{}, []byte(cmd))
}

func (s *acceptorLogicTest) newCmdTerm(pos LogPos, term TermNum, cmd string) LogEntry {
	return NewCmdLogEntry(pos, term.ToInf(), []byte(cmd))
}

func (s *acceptorLogicTest) doAcceptEntries(
	committed LogPos, entries ...LogEntry,
) AcceptEntriesOutput {
	if len(entries) > 0 {
		AssertTrue(committed < entries[0].Pos)
	}

	resp, err := s.logic.AcceptEntries(AcceptEntriesInput{
		ToNode:    nodeID2,
		Term:      s.currentTerm,
		Entries:   entries,
		Committed: committed,
	})
	if err != nil {
		panic("Should accept ok, but got: " + err.Error())
	}
	return resp
}

func TestAcceptorLogic_HandleRequestVote__No_Log_Entries(t *testing.T) {
	s := newAcceptorLogicTest(t)

	outputs := s.doHandleVote(s.currentTerm, 2)
	assert.Equal(t, []RequestVoteOutput{
		{
			Success: true,
			Term:    s.currentTerm,
			Entries: []VoteLogEntry{
				{
					IsFinal: true,
					Entry:   NewNullEntry(2),
				},
			},
		},
	}, outputs)

	assert.Equal(t, s.currentTerm, s.log.GetTerm())

	// get again
	outputs = s.doHandleVote(s.currentTerm, 2)
	assert.Equal(t, []RequestVoteOutput{
		{
			Success: true,
			Term:    s.currentTerm,
			Entries: []VoteLogEntry{
				{
					IsFinal: true,
					Entry:   NewNullEntry(2),
				},
			},
		},
	}, outputs)
	assert.Equal(t, s.currentTerm, s.log.GetTerm())
}

func TestAcceptorLogic_HandleRequestVote__With_Log_Entries(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.putMembers()

	s.log.UpsertEntriesV1(
		newPosLogEntries(2,
			s.newCmd(2, "cmd test 01"),
			s.newCmd(3, "cmd test 02"),
			s.newCmd(4, "cmd test 03"),
		),
		nil,
	)

	s.initLogic(100)

	outputs := s.doHandleVote(s.currentTerm, 2)
	assert.Equal(t, []RequestVoteOutput{
		{
			Success: true,
			Term:    s.currentTerm,
			Entries: []VoteLogEntry{
				{Entry: s.newCmd(2, "cmd test 01")},
				{Entry: s.newCmd(3, "cmd test 02")},
				{Entry: s.newCmd(4, "cmd test 03")},
				{IsFinal: true, Entry: NewNullEntry(5)},
			},
		},
	}, outputs)
}

func TestAcceptorLogic_HandleRequestVote__With_Log_Entries__With_Limit(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.putMembers()

	s.log.UpsertEntriesV1(
		newPosLogEntries(2,
			s.newCmd(2, "cmd test 01"),
			s.newCmd(3, "cmd test 02"),
			s.newCmd(4, "cmd test 03"),
		),
		nil,
	)

	s.initLogic(2)

	outputs := s.doHandleVote(s.currentTerm, 2)
	assert.Equal(t, []RequestVoteOutput{
		{
			Success: true,
			Term:    s.currentTerm,
			Entries: []VoteLogEntry{
				{Entry: s.newCmd(2, "cmd test 01")},
				{Entry: s.newCmd(3, "cmd test 02")},
			},
		},
		{
			Success: true,
			Term:    s.currentTerm,
			Entries: []VoteLogEntry{
				{Entry: s.newCmd(4, "cmd test 03")},
				{IsFinal: true, Entry: NewNullEntry(5)},
			},
		},
	}, outputs)
}

func TestAcceptorLogic_HandleRequestVote__With_Lower_Term__Not_Success(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doHandleVote(s.currentTerm, 2)

	lowerTerm := s.currentTerm
	lowerTerm.Num--
	outputs := s.doHandleVote(lowerTerm, 2)
	assert.Equal(t, []RequestVoteOutput{
		{
			Success: false,
			Term:    s.currentTerm,
		},
	}, outputs)
}

func TestAcceptorLogic_HandleRequestVote__Mismatch_Node_ID(t *testing.T) {
	s := newAcceptorLogicTest(t)

	_, err := s.logic.HandleRequestVote(RequestVoteInput{
		ToNode:  nodeID3,
		Term:    s.currentTerm,
		FromPos: 2,
	})
	assert.Equal(t, errors.New("mismatch node id"), err)
}

func TestAcceptorLogic_AcceptEntries(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 01"),
			s.newCmd(3, "cmd test 02"),
			s.newCmd(4, "cmd test 03"),
		)...,
	)
	assert.Equal(t, AcceptEntriesOutput{
		Success: true,
		Term:    s.currentTerm,
		PosList: []LogPos{2, 3, 4},
	}, resp)

	entries := s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmd(2, "cmd test 01"),
		s.newCmd(3, "cmd test 02"),
		s.newCmd(4, "cmd test 03"),
	), entries)

	// accept again
	resp = s.doAcceptEntries(
		1,
		newAcceptLogEntries(5,
			s.newCmd(5, "cmd test 04"),
		)...,
	)
	assert.Equal(t, AcceptEntriesOutput{
		Success: true,
		Term:    s.currentTerm,
		PosList: []LogPos{5},
	}, resp)

	entries = s.log.GetEntries(4, 100)
	assert.Equal(t, newPosLogEntries(
		4,
		s.newCmd(4, "cmd test 03"),
		s.newCmd(5, "cmd test 04"),
	), entries)

	// accept entries on lower term
	lowerTerm := s.currentTerm
	lowerTerm.Num--
	resp, err := s.logic.AcceptEntries(AcceptEntriesInput{
		ToNode: nodeID2,
		Term:   lowerTerm,
		Entries: newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 05"),
		),
		Committed: 1,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, AcceptEntriesOutput{
		Success: false,
		Term:    s.currentTerm,
	}, resp)
}

func TestAcceptorLogic_AcceptEntries__Increase_Committed_Pos(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 01"),
			s.newCmd(3, "cmd test 02"),
			s.newCmd(4, "cmd test 03"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// increase committed to 3
	resp = s.doAcceptEntries(
		3,
		newAcceptLogEntries(6,
			s.newCmd(6, "cmd test 04"),
			s.newCmd(7, "cmd test 05"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	entries := s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdInf(2, "cmd test 01"),
		s.newCmdInf(3, "cmd test 02"),
		s.newCmd(4, "cmd test 03"),
		NewNullEntry(5),
		s.newCmd(6, "cmd test 04"),
		s.newCmd(7, "cmd test 05"),
	), entries)

	// do accept only increase commit pos
	resp = s.doAcceptEntries(6)
	assert.Equal(t, true, resp.Success)

	// check log entries again
	entries = s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdInf(2, "cmd test 01"),
		s.newCmdInf(3, "cmd test 02"),
		s.newCmdInf(4, "cmd test 03"),
		NewNullEntry(5),
		s.newCmdInf(6, "cmd test 04"),
		s.newCmd(7, "cmd test 05"),
	), entries)

	assert.Equal(t, []fake.UpsertInput{
		{PutList: []LogPos{1}},
		{PutList: []LogPos{2, 3, 4}},
		{PutList: []LogPos{6, 7}, MarkList: []LogPos{2, 3}},
		{MarkList: []LogPos{4, 6}},
	}, s.log.UpsertList)
}

func TestAcceptorLogic_AcceptEntries__Term_And_Committed_Change(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 01"),
			s.newCmd(3, "cmd test 02"),
			s.newCmd(4, "cmd test 03"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// change current term
	oldTerm := s.currentTerm
	s.currentTerm.Num++

	resp = s.doAcceptEntries(
		3,
		newAcceptLogEntries(5,
			s.newCmd(5, "cmd test 04"),
			s.newCmd(6, "cmd test 05"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// check log entries
	entries := s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdTerm(2, oldTerm, "cmd test 01"),
		s.newCmdTerm(3, oldTerm, "cmd test 02"),
		s.newCmdTerm(4, oldTerm, "cmd test 03"),
		s.newCmd(5, "cmd test 04"),
		s.newCmd(6, "cmd test 05"),
	), entries)

	// increase commit pos only
	resp = s.doAcceptEntries(6)
	assert.Equal(t, true, resp.Success)

	// check log entries
	entries = s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdTerm(2, oldTerm, "cmd test 01"),
		s.newCmdTerm(3, oldTerm, "cmd test 02"),
		s.newCmdTerm(4, oldTerm, "cmd test 03"),
		s.newCmdInf(5, "cmd test 04"),
		s.newCmdInf(6, "cmd test 05"),
	), entries)

	assert.Equal(t, []fake.UpsertInput{
		{PutList: []LogPos{1}},
		{PutList: []LogPos{2, 3, 4}},
		{PutList: []LogPos{5, 6}},
		{MarkList: []LogPos{5, 6}},
	}, s.log.UpsertList)
}

func TestAcceptorLogic_AcceptEntries_Then_Get_Replicated_Pos(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(4,
			s.newCmd(4, "cmd test 03"), // pos = 4
			s.newCmd(5, "cmd test 04"), // pos = 5
		)...,
	)
	assert.Equal(t, true, resp.Success)

	resp = s.doAcceptEntries(5)
	assert.Equal(t, true, resp.Success)

	input, err := s.logic.GetNeedReplicatedPos(s.ctx, s.currentTerm, 0, 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, NeedReplicatedInput{
		Term:     s.currentTerm,
		FromNode: nodeID2,
		PosList:  []LogPos{2, 3},
		NextPos:  6,

		FullyReplicated: 1,
	}, input)

	// check log entries
	entries := s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		NewNullEntry(2),
		NewNullEntry(3),
		s.newCmdInf(4, "cmd test 03"),
		s.newCmdInf(5, "cmd test 04"),
	), entries)
}

func TestAcceptorLogic_AcceptEntries_Then_Get_Replicated_Pos__Commit_Index_Increase_Partially(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(4,
			s.newCmd(4, "cmd test 03"), // pos = 4
			s.newCmd(5, "cmd test 04"), // pos = 5
		)...,
	)
	assert.Equal(t, true, resp.Success)

	resp = s.doAcceptEntries(2)
	assert.Equal(t, true, resp.Success)

	input, err := s.logic.GetNeedReplicatedPos(s.ctx, s.currentTerm, 0, 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, NeedReplicatedInput{
		Term:     s.currentTerm,
		FromNode: nodeID2,
		PosList:  []LogPos{2},
		NextPos:  3,

		FullyReplicated: 1,
	}, input)
}

func TestAcceptorLogic__Increase_Committed_Pos_Only__Then_Get_Need_Replicated(t *testing.T) {
	s := newAcceptorLogicTest(t)
	s.putMembers()
	s.initLogic(3)

	resp := s.doAcceptEntries(6)
	assert.Equal(t, true, resp.Success)

	input, err := s.logic.GetNeedReplicatedPos(s.ctx, s.currentTerm, 0, 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, NeedReplicatedInput{
		Term:     s.currentTerm,
		FromNode: nodeID2,
		PosList:  []LogPos{2, 3, 4},
		NextPos:  5,

		FullyReplicated: 1,
	}, input)
}

func TestAcceptorLogic_AcceptEntries__Mark_Committed__Multiple_Batches(t *testing.T) {
	s := newAcceptorLogicTest(t)
	s.initLogic(3)

	s.doAcceptEntries(
		1,
		newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 02"),
			s.newCmd(3, "cmd test 03"),
			s.newCmd(4, "cmd test 04"),
		)...,
	)

	// still at committed = 1
	s.doAcceptEntries(
		1,
		newAcceptLogEntries(6,
			s.newCmd(6, "cmd test 06"),
			s.newCmd(7, "cmd test 07"),
			s.newCmd(8, "cmd test 08"),
		)...,
	)

	assert.Equal(t, []fake.UpsertInput{
		{PutList: []LogPos{1}},
		{PutList: []LogPos{2, 3, 4}},
		{PutList: []LogPos{6, 7, 8}},
	}, s.log.UpsertList)
	s.log.UpsertList = nil

	// increase commit
	s.doAcceptEntries(8,
		newAcceptLogEntries(9,
			s.newCmd(9, "cmd test 09"),
		)...,
	)

	assert.Equal(t, []fake.UpsertInput{
		{MarkList: []LogPos{2, 3, 4}},
		{MarkList: []LogPos{6, 7}},
		{PutList: []LogPos{9}, MarkList: []LogPos{8}},
	}, s.log.UpsertList)
}

func (s *acceptorLogicTest) doGetNeedReplicated(from LogPos, lastFullyReplicated LogPos) NeedReplicatedInput {
	input, err := s.logic.GetNeedReplicatedPos(s.ctx, s.currentTerm, from, lastFullyReplicated)
	if err != nil {
		panic(err)
	}
	return input
}

func TestAcceptorLogic__Get_Need_Replicated_With_Wait(t *testing.T) {
	s := newAcceptorLogicTest(t)

	synctest.Test(t, func(t *testing.T) {
		getFn, _ := testutil.RunAsync(t, func() NeedReplicatedInput {
			return s.doGetNeedReplicated(2, 1)
		})

		resp := s.doAcceptEntries(2)
		assert.Equal(t, true, resp.Success)

		assert.Equal(t, NeedReplicatedInput{
			Term:     s.currentTerm,
			FromNode: nodeID2,
			PosList:  []LogPos{2},
			NextPos:  3,

			FullyReplicated: 1,
		}, getFn())
	})
}

func TestAcceptorLogic__Get_Need_Replicated_With_Wait__From_High_Pos(t *testing.T) {
	s := newAcceptorLogicTest(t)

	synctest.Test(t, func(t *testing.T) {
		getFn, assertNotFinish := testutil.RunAsync(t, func() NeedReplicatedInput {
			return s.doGetNeedReplicated(3, 1)
		})

		resp := s.doAcceptEntries(2)
		assert.Equal(t, true, resp.Success)
		assertNotFinish()

		resp = s.doAcceptEntries(4)
		assert.Equal(t, true, resp.Success)

		assert.Equal(t, NeedReplicatedInput{
			Term:     s.currentTerm,
			FromNode: nodeID2,
			PosList:  []LogPos{3, 4},
			NextPos:  5,

			FullyReplicated: 1,
		}, getFn())
	})
}

func TestAcceptorLogic__Get_Need_Replicated__Not_Wait_Because_Of_Init_Fully_Replicated_Pos(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doAcceptEntries(2)

	input := s.doGetNeedReplicated(3, 0)
	assert.Equal(t, NeedReplicatedInput{
		Term:     s.currentTerm,
		FromNode: nodeID2,
		NextPos:  3,

		FullyReplicated: 1,
	}, input)

	synctest.Test(t, func(t *testing.T) {
		getFn, _ := testutil.RunAsync(t, func() NeedReplicatedInput {
			return s.doGetNeedReplicated(3, 1)
		})

		s.doAcceptEntries(3)

		assert.Equal(t, NeedReplicatedInput{
			Term:            s.currentTerm,
			FromNode:        nodeID2,
			PosList:         []LogPos{3},
			NextPos:         4,
			FullyReplicated: 1,
		}, getFn())
	})
}

func TestAcceptorLogic__Get_Need_Replicated__Wait_For_New_Fully_Replicated(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doAcceptEntries(3)

	synctest.Test(t, func(t *testing.T) {
		getFn, _ := testutil.RunAsync(t, func() NeedReplicatedInput {
			return s.doGetNeedReplicated(4, 1)
		})

		s.doAcceptEntries(0,
			newAcceptLogEntries(2,
				s.newCmd(2, "cmd data 01"),
				s.newCmd(3, "cmd data 02"),
			)...,
		)

		assert.Equal(t, NeedReplicatedInput{
			Term:            s.currentTerm,
			FromNode:        nodeID2,
			NextPos:         4,
			FullyReplicated: 3,
		}, getFn())
	})
}

func TestAcceptorLogic__Get_Need_Replicated__For_Finite_Term_Entries(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doAcceptEntries(
		1,
		newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 02"),
			s.newCmd(3, "cmd test 03"),
			s.newCmd(4, "cmd test 04"),
		)...,
	)

	s.currentTerm.Num++
	s.doAcceptEntries(
		5,
		newAcceptLogEntries(6,
			s.newCmd(6, "cmd test 06"),
			s.newCmd(7, "cmd test 07"),
		)...,
	)
	assert.Equal(t, LogPos(1), s.log.GetFullyReplicated())
	assert.Equal(t, LogPos(5), s.logic.GetLastCommitted())

	req := s.doGetNeedReplicated(0, 0)
	assert.Equal(t, NeedReplicatedInput{
		Term:            s.currentTerm,
		FromNode:        nodeID2,
		PosList:         []LogPos{2, 3, 4, 5},
		NextPos:         6,
		FullyReplicated: 1,
	}, req)
}

func TestAcceptorLogic__Accept_Entries__Increase_Last_Committed__Then_Reset(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doAcceptEntries(3)
	assert.Equal(t, LogPos(3), s.logic.GetLastCommitted())

	s.currentTerm.Num++
	s.doAcceptEntries(2)
	assert.Equal(t, LogPos(2), s.logic.GetLastCommitted())
}

func TestAcceptorLogic__Accept_Entries__GetReplicatedPos__Error(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doAcceptEntries(3)

	oldTerm := s.currentTerm
	oldTerm.Num--

	_, err := s.logic.GetNeedReplicatedPos(s.ctx, oldTerm, 0, 0)
	assert.Equal(t, errors.New("input term is less than actual term"), err)
}

func TestAcceptorLogic__GetReplicated__Wait_On_New_Term(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.doAcceptEntries(1)

	synctest.Test(t, func(t *testing.T) {
		term := s.currentTerm
		getFn, _ := testutil.RunAsync(t, func() error {
			_, err := s.logic.GetNeedReplicatedPos(s.ctx, term, 2, 1)
			return err
		})

		s.currentTerm.Num++
		s.doAcceptEntries(1)

		assert.Equal(t, errors.New("input term is less than actual term"), getFn())
	})
}

func (s *acceptorLogicTest) doGetCommitted(from LogPos, limit int) GetCommittedEntriesOutput {
	output, err := s.logic.GetCommittedEntriesWithWait(s.ctx, s.log.GetTerm(), from, limit)
	if err != nil {
		panic(err)
	}
	return output
}

func TestAcceptorLogic__GetEntriesWithWait(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(2,
			s.newCmd(2, "cmd test 02"),
			s.newCmd(3, "cmd test 03"),
			s.newCmd(4, "cmd test 04"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// increase the fully replicated pos
	resp = s.doAcceptEntries(3)
	assert.Equal(t, true, resp.Success)

	// get entries
	output, err := s.logic.GetCommittedEntriesWithWait(s.ctx, s.log.GetTerm(), 1, 100)
	assert.Equal(t, nil, err)
	assert.Equal(t, []PosLogEntry{
		{Pos: 1, Entry: s.initMemberLogEntry()},
		{Pos: 2, Entry: s.newCmdInf(2, "cmd test 02")},
		{Pos: 3, Entry: s.newCmdInf(3, "cmd test 03")},
	}, output.Entries)
	assert.Equal(t, LogPos(4), output.NextPos)

	// get entries with limit
	output, err = s.logic.GetCommittedEntriesWithWait(s.ctx, s.log.GetTerm(), 1, 2)
	assert.Equal(t, nil, err)
	assert.Equal(t, []PosLogEntry{
		{Pos: 1, Entry: s.initMemberLogEntry()},
		{Pos: 2, Entry: s.newCmdInf(2, "cmd test 02")},
	}, output.Entries)
	assert.Equal(t, LogPos(3), output.NextPos)

	synctest.Test(t, func(t *testing.T) {
		// get with wait
		resultFn, _ := testutil.RunAsync(t, func() GetCommittedEntriesOutput {
			return s.doGetCommitted(4, 100)
		})

		s.doAcceptEntries(
			4,
			newAcceptLogEntries(5,
				s.newCmd(5, "cmd test 05"),
				s.newCmd(6, "cmd test 06"),
			)...,
		)

		output := resultFn()
		assert.Equal(t, []PosLogEntry{
			{Pos: 4, Entry: s.newCmdInf(4, "cmd test 04")},
		}, output.Entries)
		assert.Equal(t, LogPos(5), output.NextPos)

		s.doAcceptEntries(6)

		// get again
		output = s.doGetCommitted(5, 100)
		assert.Equal(t, []PosLogEntry{
			{Pos: 5, Entry: s.newCmdInf(5, "cmd test 05")},
			{Pos: 6, Entry: s.newCmdInf(6, "cmd test 06")},
		}, output.Entries)
		assert.Equal(t, LogPos(7), output.NextPos)
	})
}
