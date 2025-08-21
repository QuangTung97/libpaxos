package paxos_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type acceptorLogicTest struct {
	log         *fake.LogStorageFake
	logic       AcceptorLogic
	currentTerm TermNum
}

func newAcceptorLogicTest() *acceptorLogicTest {
	s := &acceptorLogicTest{}
	s.log = &fake.LogStorageFake{}

	s.currentTerm = TermNum{
		Num:    21,
		NodeID: nodeID1,
	}

	s.putMembers()
	s.initLogic(100)

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
	})
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
	s := newAcceptorLogicTest()

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
	s := newAcceptorLogicTest()

	s.putMembers()

	s.log.UpsertEntries(
		newPosLogEntries(2,
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
		),
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
	s := newAcceptorLogicTest()

	s.putMembers()

	s.log.UpsertEntries(
		newPosLogEntries(2,
			s.newCmd("cmd test 01"),
			s.newCmd("cmd test 02"),
			s.newCmd("cmd test 03"),
		),
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
	s := newAcceptorLogicTest()

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
	s := newAcceptorLogicTest()

	_, err := s.logic.HandleRequestVote(RequestVoteInput{
		ToNode:  nodeID3,
		Term:    s.currentTerm,
		FromPos: 2,
	})
	assert.Equal(t, errors.New("mismatch node id"), err)
}

func TestAcceptorLogic_AcceptEntries(t *testing.T) {
	s := newAcceptorLogicTest()

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
	s := newAcceptorLogicTest()

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
	entry1 := s.newCmd("cmd test 01")
	entry1.Term = InfiniteTerm{}
	entry2 := s.newCmd("cmd test 02")
	entry2.Term = InfiniteTerm{}
	assert.Equal(t, newPosLogEntries(
		2,
		entry1,
		entry2,
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

	entry3 := s.newCmd("cmd test 03")
	entry4 := s.newCmd("cmd test 04")
	entry5 := s.newCmd("cmd test 05")

	entry3.Term = InfiniteTerm{}
	entry4.Term = InfiniteTerm{}

	assert.Equal(t, newPosLogEntries(
		2,
		entry1, entry2, entry3,
		LogEntry{}, entry4, entry5,
	), entries)

	assert.Equal(t, [][]LogPos{
		{1},
		{2, 3, 4},
		{6, 7, 2, 3},
		{4, 6},
	}, s.log.PutPosList)
}
