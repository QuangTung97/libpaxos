package paxos

import (
	"iter"
	"sync"
)

type AcceptorLogic interface {
	HandleRequestVote(input RequestVoteInput) (iter.Seq[RequestVoteOutput], error)
	AcceptEntries(input AcceptEntriesInput) (AcceptEntriesOutput, error)
}

type acceptorLogicImpl struct {
	mut sync.Mutex
	log LogStorage

	limit int
}

func NewAcceptorLogic(
	log LogStorage,
	limit int,
) AcceptorLogic {
	return &acceptorLogicImpl{
		log:   log,
		limit: limit,
	}
}

func (s *acceptorLogicImpl) HandleRequestVote(
	input RequestVoteInput,
) (iter.Seq[RequestVoteOutput], error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// TODO check input node id

	return func(yield func(RequestVoteOutput) bool) {
		fromPos := input.FromPos

		for {
			entries := s.log.GetEntries(fromPos, s.limit)

			voteEntries := make([]VoteLogEntry, 0, len(entries))
			for _, e := range entries {
				voteEntries = append(voteEntries, VoteLogEntry{
					Pos:     e.Pos,
					IsFinal: false,
					Entry:   e.Entry,
				})
			}

			isFinal := len(entries) < s.limit
			newFromPos := fromPos + LogPos(len(entries))

			if isFinal {
				voteEntries = append(voteEntries, VoteLogEntry{
					Pos:     newFromPos,
					IsFinal: true,
				})
			}

			ok := yield(RequestVoteOutput{
				Success: true,
				Term:    input.Term,
				Entries: voteEntries,
			})
			if !ok {
				return
			}

			if isFinal {
				return
			}

			fromPos = newFromPos
		}
	}, nil
}

func (s *acceptorLogicImpl) AcceptEntries(
	input AcceptEntriesInput,
) (AcceptEntriesOutput, error) {
	return AcceptEntriesOutput{}, nil
}
