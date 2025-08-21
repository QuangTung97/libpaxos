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

func (s *acceptorLogicTest) putMembers() {
	memberEntry := NewMembershipLogEntry(
		InfiniteTerm{},
		[]MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		},
	)
	s.log.UpsertEntries([]PosLogEntry{
		{Pos: 1, Entry: memberEntry},
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
		result = append(result, PosLogEntry{
			Pos:   from + LogPos(index),
			Entry: e,
		})
	}
	return result
}

func newAcceptLogEntries(from LogPos, entries ...LogEntry) []AcceptLogEntry {
	result := make([]AcceptLogEntry, 0, len(entries))
	for index, e := range entries {
		result = append(result, AcceptLogEntry{
			Pos:   from + LogPos(index),
			Entry: e,
		})
	}
	return result
}

func (s *acceptorLogicTest) newCmd(cmd string) LogEntry {
	return NewCmdLogEntry(s.currentTerm.ToInf(), []byte(cmd))
}

func (s *acceptorLogicTest) newCmdInf(cmd string) LogEntry {
	return NewCmdLogEntry(InfiniteTerm{}, []byte(cmd))
}

func (s *acceptorLogicTest) newCmdTerm(term TermNum, cmd string) LogEntry {
	return NewCmdLogEntry(term.ToInf(), []byte(cmd))
}

func (s *acceptorLogicTest) doAcceptEntries(
	committed LogPos, entries ...AcceptLogEntry,
) AcceptEntriesOutput {
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
					Pos:     2,
					IsFinal: true,
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
					Pos:     2,
					IsFinal: true,
				},
			},
		},
	}, outputs)
	assert.Equal(t, s.currentTerm, s.log.GetTerm())
}

func TestAcceptorLogic_HandleRequestVote__With_Log_Entries(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.putMembers()

	s.log.UpsertEntries(
		newPosLogEntries(2,
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
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
				{Pos: 2, Entry: s.newCmd("cmd test 01")},
				{Pos: 3, Entry: s.newCmd("cmd test 02")},
				{Pos: 4, Entry: s.newCmd("cmd test 03")},
				{Pos: 5, IsFinal: true},
			},
		},
	}, outputs)
}

func TestAcceptorLogic_HandleRequestVote__With_Log_Entries__With_Limit(t *testing.T) {
	s := newAcceptorLogicTest(t)

	s.putMembers()

	s.log.UpsertEntries(
		newPosLogEntries(2,
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
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
				{Pos: 2, Entry: s.newCmd("cmd test 01")},
				{Pos: 3, Entry: s.newCmd("cmd test 02")},
			},
		},
		{
			Success: true,
			Term:    s.currentTerm,
			Entries: []VoteLogEntry{
				{Pos: 4, Entry: s.newCmd("cmd test 03")},
				{Pos: 5, IsFinal: true},
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
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
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
		s.newCmd("cmd test 01"),
		s.newCmd("cmd test 02"),
		s.newCmd("cmd test 03"),
	), entries)

	// accept again
	resp = s.doAcceptEntries(
		1,
		newAcceptLogEntries(5,
			s.newCmd("cmd test 04"),
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
		s.newCmd("cmd test 03"),
		s.newCmd("cmd test 04"),
	), entries)

	// accept entries on lower term
	lowerTerm := s.currentTerm
	lowerTerm.Num--
	resp, err := s.logic.AcceptEntries(AcceptEntriesInput{
		ToNode: nodeID2,
		Term:   lowerTerm,
		Entries: newAcceptLogEntries(2,
			s.newCmd("cmd test 05"),
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
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// increase committed to 3
	resp = s.doAcceptEntries(
		3,
		newAcceptLogEntries(6,
			s.newCmd("cmd test 04"),
			s.newCmd("cmd test 05"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	entries := s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdInf("cmd test 01"),
		s.newCmdInf("cmd test 02"),
		s.newCmd("cmd test 03"),
		LogEntry{},
		s.newCmd("cmd test 04"),
		s.newCmd("cmd test 05"),
	), entries)

	// do accept only increase commit pos
	resp = s.doAcceptEntries(6)
	assert.Equal(t, true, resp.Success)

	// check log entries again
	entries = s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdInf("cmd test 01"),
		s.newCmdInf("cmd test 02"),
		s.newCmdInf("cmd test 03"),
		LogEntry{},
		s.newCmdInf("cmd test 04"),
		s.newCmd("cmd test 05"),
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
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// change current term
	oldTerm := s.currentTerm
	s.currentTerm.Num++

	resp = s.doAcceptEntries(
		3,
		newAcceptLogEntries(5,
			s.newCmd("cmd test 04"),
			s.newCmd("cmd test 05"),
		)...,
	)
	assert.Equal(t, true, resp.Success)

	// check log entries
	entries := s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdTerm(oldTerm, "cmd test 01"),
		s.newCmdTerm(oldTerm, "cmd test 02"),
		s.newCmdTerm(oldTerm, "cmd test 03"),
		s.newCmd("cmd test 04"),
		s.newCmd("cmd test 05"),
	), entries)

	// increase commit pos only
	resp = s.doAcceptEntries(6)
	assert.Equal(t, true, resp.Success)

	// check log entries
	entries = s.log.GetEntries(2, 100)
	assert.Equal(t, newPosLogEntries(
		2,
		s.newCmdTerm(oldTerm, "cmd test 01"),
		s.newCmdTerm(oldTerm, "cmd test 02"),
		s.newCmdTerm(oldTerm, "cmd test 03"),
		s.newCmdInf("cmd test 04"),
		s.newCmdInf("cmd test 05"),
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
			s.newCmd("cmd test 03"), // pos = 4
			s.newCmd("cmd test 04"), // pos = 5
		)...,
	)
	assert.Equal(t, true, resp.Success)

	resp = s.doAcceptEntries(
		5,
	)
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
		LogEntry{},
		LogEntry{},
		s.newCmdInf("cmd test 03"),
		s.newCmdInf("cmd test 04"),
	), entries)
}

func TestAcceptorLogic_AcceptEntries_Then_Get_Replicated_Pos__Commit_Index_Increase_Partially(t *testing.T) {
	s := newAcceptorLogicTest(t)

	resp := s.doAcceptEntries(
		1,
		newAcceptLogEntries(4,
			s.newCmd("cmd test 03"), // pos = 4
			s.newCmd("cmd test 04"), // pos = 5
		)...,
	)
	assert.Equal(t, true, resp.Success)

	resp = s.doAcceptEntries(
		2,
	)
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
				s.newCmd("cmd data 01"),
				s.newCmd("cmd data 02"),
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
