package paxos

import (
	"fmt"
	"iter"
	"sync"
)

type AcceptorLogic interface {
	HandleRequestVote(input RequestVoteInput) (iter.Seq[RequestVoteOutput], error)
	AcceptEntries(input AcceptEntriesInput) (AcceptEntriesOutput, error)
}

type acceptorLogicImpl struct {
	currentNode NodeID
	limit       int

	mut sync.Mutex
	log LogStorage
}

func NewAcceptorLogic(
	currentNode NodeID,
	log LogStorage,
	limit int,
) AcceptorLogic {
	return &acceptorLogicImpl{
		currentNode: currentNode,
		limit:       limit,

		log: log,
	}
}

func (s *acceptorLogicImpl) validateNodeID(toNodeID NodeID) error {
	if toNodeID != s.currentNode {
		// TODO testing
		return fmt.Errorf("mismatch node id")
	}
	return nil
}

func (s *acceptorLogicImpl) HandleRequestVote(
	input RequestVoteInput,
) (iter.Seq[RequestVoteOutput], error) {
	if err := s.validateNodeID(input.ToNode); err != nil {
		return nil, err
	}

	return func(yield func(RequestVoteOutput) bool) {
		fromPos := input.FromPos

		for {
			output, newFromPos, isFinal := s.buildVoteResponse(input.Term, fromPos)

			ok := yield(output)
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

func (s *acceptorLogicImpl) buildVoteResponse(
	inputTerm TermNum, fromPos LogPos,
) (RequestVoteOutput, LogPos, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if CompareTermNum(inputTerm, s.log.GetTerm()) < 0 {
		return RequestVoteOutput{
			Success: false,
			Term:    s.log.GetTerm(),
		}, 0, true
	}

	s.log.SetTerm(inputTerm)

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

	return RequestVoteOutput{
		Success: true,
		Term:    inputTerm,
		Entries: voteEntries,
	}, newFromPos, isFinal
}

func (s *acceptorLogicImpl) AcceptEntries(
	input AcceptEntriesInput,
) (AcceptEntriesOutput, error) {
	if err := s.validateNodeID(input.ToNode); err != nil {
		return AcceptEntriesOutput{}, err
	}

	return AcceptEntriesOutput{}, nil
}
