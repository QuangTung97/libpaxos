package paxos_test

import (
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
	s.logic = NewAcceptorLogic(s.log, limit)
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

func (s *acceptorLogicTest) newCmd(cmd string) LogEntry {
	return NewCmdLogEntry(s.currentTerm.ToInf(), []byte(cmd))
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
