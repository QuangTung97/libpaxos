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

	memberEntry := NewMembershipLogEntry(
		InfiniteTerm{},
		[]MemberInfo{
			{Nodes: []NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		},
	)

	s.log.UpsertEntries([]PosLogEntry{
		{Pos: 1, Entry: memberEntry},
	})
	s.initLogic()

	return s
}

func (s *acceptorLogicTest) initLogic() {
	s.logic = NewAcceptorLogic(s.log)
}

func (s *acceptorLogicTest) doHandleVote(term TermNum, fromPos LogPos) []RequestVoteOutput {
	it := s.logic.HandleRequestVote(RequestVoteInput{
		ToNode:  nodeID2,
		Term:    term,
		FromPos: fromPos,
	})

	var outputs []RequestVoteOutput
	for x := range it {
		outputs = append(outputs, x)
	}
	return outputs
}

func TestAcceptorLogic_HandleRequestVote(t *testing.T) {
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
