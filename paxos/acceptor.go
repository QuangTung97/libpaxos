package paxos

import (
	"context"
	"fmt"
	"iter"
	"sync"
)

type AcceptorLogic interface {
	StateMachineLogGetter

	HandleRequestVote(input RequestVoteInput) (iter.Seq[RequestVoteOutput], error)
	AcceptEntriesV1(input AcceptEntriesInputV1) (AcceptEntriesOutput, error)
	AcceptEntries(input AcceptEntriesInput) (AcceptEntriesOutput, error)

	GetNeedReplicatedPos(
		ctx context.Context, term TermNum, from LogPos,
		lastFullyReplicated LogPos,
	) (NeedReplicatedInput, error)

	// CheckInvariant for testing only
	CheckInvariant()

	// GetLastCommitted for testing only
	GetLastCommitted() LogPos
}

type acceptorLogicImpl struct {
	currentNode NodeID
	limit       int

	mut           sync.Mutex
	log           LogStorage
	lastCommitted LogPos
	waitCond      *NodeCond
}

func NewAcceptorLogic(
	currentNode NodeID,
	log LogStorage,
	limit int,
) AcceptorLogic {
	s := &acceptorLogicImpl{
		currentNode: currentNode,
		limit:       limit,

		log:           log,
		lastCommitted: log.GetFullyReplicated(),
	}
	s.waitCond = NewNodeCond(&s.mut)
	return s
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

func (s *acceptorLogicImpl) updateTermNum(inputTerm TermNum) bool {
	cmpVal := CompareTermNum(inputTerm, s.log.GetTerm())
	if cmpVal < 0 {
		return false
	}
	if cmpVal > 0 {
		s.log.SetTerm(inputTerm)
		s.lastCommitted = s.log.GetFullyReplicated()
		s.waitCond.Broadcast()
	}
	return true
}

func (s *acceptorLogicImpl) buildVoteResponse(
	inputTerm TermNum, fromPos LogPos,
) (RequestVoteOutput, LogPos, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if !s.updateTermNum(inputTerm) {
		return RequestVoteOutput{
			Success: false,
			Term:    s.log.GetTerm(),
		}, 0, true
	}

	entries := s.log.GetEntries(fromPos, s.limit)

	voteEntries := make([]VoteLogEntry, 0, len(entries))
	for _, e := range entries {
		voteEntries = append(voteEntries, VoteLogEntry{
			IsFinal: false,
			Entry:   e.Entry,
		})
	}

	isFinal := len(entries) < s.limit
	newFromPos := fromPos + LogPos(len(entries))

	if isFinal {
		voteEntries = append(voteEntries, VoteLogEntry{
			IsFinal: true,
			Entry:   NewNullEntry(newFromPos),
		})
	}

	return RequestVoteOutput{
		Success: true,
		Term:    inputTerm,
		Entries: voteEntries,
	}, newFromPos, isFinal
}

// AcceptEntriesV1 TODO remove
func (s *acceptorLogicImpl) AcceptEntriesV1(
	input AcceptEntriesInputV1,
) (AcceptEntriesOutput, error) {
	newInput := AcceptEntriesInput{
		ToNode:    input.ToNode,
		Term:      input.Term,
		Entries:   UnwrapPosLogEntryList(input.Entries),
		NextPos:   input.NextPos,
		Committed: input.Committed,
	}
	return s.AcceptEntries(newInput)
}

func (s *acceptorLogicImpl) AcceptEntries(
	input AcceptEntriesInput,
) (AcceptEntriesOutput, error) {
	// TODO remove
	for _, e := range input.Entries {
		AssertTrue(e.Pos > 0)
	}

	if err := s.validateNodeID(input.ToNode); err != nil {
		return AcceptEntriesOutput{}, err
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	if !s.updateTermNum(input.Term) {
		return AcceptEntriesOutput{
			Success: false,
			Term:    s.log.GetTerm(),
		}, nil
	}

	posList := make([]LogPos, 0, len(input.Entries))
	putEntries := make([]PosLogEntry, 0, len(input.Entries))
	for _, entry := range input.Entries {
		posList = append(posList, entry.Pos)
		putEntries = append(putEntries, PosLogEntry{
			Pos:   entry.Pos,
			Entry: entry,
		})
	}

	markCommitted := s.getNeedUpdateTermToInf(input.Committed)

	// set term of entries to = +infinity
	for i := range putEntries {
		s.updateTermToInf(&putEntries[i])
	}

	oldFullyReplicated := s.log.GetFullyReplicated()
	s.log.UpsertEntries(putEntries, markCommitted)

	if s.log.GetFullyReplicated() > oldFullyReplicated {
		s.waitCond.Broadcast()
	}

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
	entry.Entry.Term = InfiniteTerm{}
}

func (s *acceptorLogicImpl) getNeedUpdateTermToInf(newLastCommitted LogPos) []LogPos {
	for s.lastCommitted < newLastCommitted {
		getLimit := int(newLastCommitted - s.lastCommitted)
		overLimit := false

		if getLimit > s.limit {
			getLimit = s.limit
			overLimit = true
		}

		entries := s.log.GetEntries(s.lastCommitted+1, getLimit)
		var markCommitted []LogPos

		for _, entry := range entries {
			if entry.Entry.Type == LogTypeNull {
				continue
			}
			if s.isSameTerm(entry.Entry.Term) {
				markCommitted = append(markCommitted, entry.Pos)
			}
		}

		s.lastCommitted += LogPos(getLimit)
		s.waitCond.Broadcast()

		if overLimit {
			s.log.UpsertEntries(nil, markCommitted)
		} else {
			return markCommitted
		}
	}

	return nil
}

func (s *acceptorLogicImpl) GetNeedReplicatedPos(
	ctx context.Context,
	term TermNum, from LogPos,
	lastFullyReplicated LogPos,
) (NeedReplicatedInput, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

StartLoop:
	if !s.updateTermNum(term) {
		return NeedReplicatedInput{}, fmt.Errorf("input term is less than actual term")
	}

	afterFullyReplicated := s.log.GetFullyReplicated() + 1
	if from < afterFullyReplicated {
		from = afterFullyReplicated
	}

	if s.lastCommitted < from && lastFullyReplicated >= s.log.GetFullyReplicated() {
		if err := s.waitCond.Wait(ctx, s.currentNode); err != nil {
			return NeedReplicatedInput{}, err
		}
		goto StartLoop
	}

	maxPos := s.lastCommitted
	getLimit := s.lastCommitted - from + 1
	if getLimit > LogPos(s.limit) {
		getLimit = LogPos(s.limit)
		maxPos = from + getLimit - 1
	}

	entries := s.log.GetEntries(from, int(getLimit))
	var posList []LogPos
	for pos := from; pos <= maxPos; pos++ {
		index := int(pos - from)

		var entry PosLogEntry
		if index < len(entries) {
			entry = entries[index]
		} else {
			entry = PosLogEntry{
				Pos: pos,
			}
		}

		if entry.Entry.Type == LogTypeNull {
			posList = append(posList, entry.Pos)
		} else if entry.Entry.Term.IsFinite {
			posList = append(posList, entry.Pos)
		}
	}

	return NeedReplicatedInput{
		Term:     s.log.GetTerm(),
		FromNode: s.currentNode,
		PosList:  posList,
		NextPos:  maxPos + 1,

		FullyReplicated: s.log.GetFullyReplicated(),
	}, nil
}

func (s *acceptorLogicImpl) GetCommittedEntriesWithWait(
	ctx context.Context, term TermNum,
	fromPos LogPos, limit int,
) (GetCommittedEntriesOutput, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

StartFunction:
	if !s.updateTermNum(term) {
		return GetCommittedEntriesOutput{}, fmt.Errorf("input term is less than actual term")
	}

	if fromPos > s.log.GetFullyReplicated() {
		if err := s.waitCond.Wait(ctx, s.currentNode); err != nil {
			return GetCommittedEntriesOutput{}, err
		}
		goto StartFunction
	}

	maxPos := fromPos + LogPos(limit-1)
	if maxPos > s.log.GetFullyReplicated() {
		maxPos = s.log.GetFullyReplicated()
	}
	newLimit := maxPos - fromPos + 1

	entries := s.log.GetEntries(fromPos, int(newLimit))
	return GetCommittedEntriesOutput{
		Entries: entries,
		NextPos: maxPos + 1,
	}, nil
}

func (s *acceptorLogicImpl) CheckInvariant() {
	s.mut.Lock()
	defer s.mut.Unlock()
	AssertTrue(s.log.GetFullyReplicated() <= s.lastCommitted)
}

func (s *acceptorLogicImpl) GetLastCommitted() LogPos {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.lastCommitted
}
