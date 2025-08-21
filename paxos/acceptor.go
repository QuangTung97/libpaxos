package paxos

import (
	"context"
	"fmt"
	"iter"
	"sync"
)

type AcceptorLogic interface {
	HandleRequestVote(input RequestVoteInput) (iter.Seq[RequestVoteOutput], error)
	AcceptEntries(input AcceptEntriesInput) (AcceptEntriesOutput, error)
	GetNeedReplicatedPos(ctx context.Context, from LogPos) (NeedReplicatedInput, error)
}

type acceptorLogicImpl struct {
	currentNode NodeID
	limit       int

	mut           sync.Mutex
	log           LogStorage
	lastCommitted LogPos
}

func NewAcceptorLogic(
	currentNode NodeID,
	log LogStorage,
	limit int,
) AcceptorLogic {
	commitInfo := log.GetCommittedInfo()

	return &acceptorLogicImpl{
		currentNode: currentNode,
		limit:       limit,

		log:           log,
		lastCommitted: commitInfo.FullyReplicatedPos,
	}
}

func (s *acceptorLogicImpl) validateNodeID(toNodeID NodeID) error {
	if toNodeID != s.currentNode {
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

	s.mut.Lock()
	defer s.mut.Unlock()

	if CompareTermNum(input.Term, s.log.GetTerm()) < 0 {
		return AcceptEntriesOutput{
			Success: false,
			Term:    s.log.GetTerm(),
		}, nil
	}

	s.log.SetTerm(input.Term)

	posList := make([]LogPos, 0, len(input.Entries))
	putEntries := make([]PosLogEntry, 0, len(input.Entries))
	for _, entry := range input.Entries {
		posList = append(posList, entry.Pos)
		putEntries = append(putEntries, PosLogEntry{
			Pos:   entry.Pos,
			Entry: entry.Entry,
		})
	}

	putEntries = s.getNeedUpdateTermToInf(input.Committed, putEntries)

	for i := range putEntries {
		s.updateTermToInf(&putEntries[i])
	}
	s.log.UpsertEntries(putEntries)

	return AcceptEntriesOutput{
		Success: true,
		Term:    s.log.GetTerm(),
		PosList: posList,
	}, nil
}

func (s *acceptorLogicImpl) isSameTerm(term InfiniteTerm) bool {
	return term == s.log.GetTerm().ToInf()
}

func (s *acceptorLogicImpl) updateTermToInf(entry *PosLogEntry) {
	if entry.Pos > s.lastCommitted {
		return
	}
	if !s.isSameTerm(entry.Entry.Term) {
		// TODO testing
		return
	}
	entry.Entry.Term = InfiniteTerm{}
}

func (s *acceptorLogicImpl) getNeedUpdateTermToInf(newLastCommitted LogPos, putEntries []PosLogEntry) []PosLogEntry {
	if newLastCommitted <= s.lastCommitted {
		return putEntries
	}

	getLimit := int(newLastCommitted - s.lastCommitted)
	entries := s.log.GetEntries(s.lastCommitted+1, getLimit)

	for _, entry := range entries {
		if entry.Entry.Type == LogTypeNull {
			continue
		}
		if s.isSameTerm(entry.Entry.Term) {
			putEntries = append(putEntries, entry)
		}
	}

	s.lastCommitted = newLastCommitted

	return putEntries
}

func (s *acceptorLogicImpl) GetNeedReplicatedPos(
	ctx context.Context, from LogPos,
) (NeedReplicatedInput, error) {
	return NeedReplicatedInput{}, nil
}
