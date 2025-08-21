package paxos

import (
	"iter"
)

type AcceptorLogic interface {
	HandleRequestVote(input RequestVoteInput) iter.Seq[RequestVoteOutput]
	AcceptEntries(input AcceptEntriesInput) AcceptEntriesOutput
}

type acceptorLogicImpl struct {
	log LogStorage
}

func NewAcceptorLogic(
	log LogStorage,
) AcceptorLogic {
	return &acceptorLogicImpl{
		log: log,
	}
}

func (s *acceptorLogicImpl) HandleRequestVote(input RequestVoteInput) iter.Seq[RequestVoteOutput] {
	return func(yield func(RequestVoteOutput) bool) {
		yield(RequestVoteOutput{
			Success: true,
			Term:    input.Term,
			Entries: []VoteLogEntry{
				{
					Pos:     input.FromPos,
					IsFinal: true,
				},
			},
		})
	}
}

func (s *acceptorLogicImpl) AcceptEntries(input AcceptEntriesInput) AcceptEntriesOutput {
	return AcceptEntriesOutput{}
}
