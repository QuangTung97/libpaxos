package paxos

import (
	"fmt"
	"iter"
	"sync"

	"github.com/QuangTung97/libpaxos/async"
)

type AcceptorLogic interface {
	StateMachineLogGetter

	HandleRequestVote(input RequestVoteInput) (iter.Seq[RequestVoteOutput], error)
	AcceptEntries(input AcceptEntriesInput) (AcceptEntriesOutput, error)

	GetNeedReplicatedPos(
		ctx async.Context, term TermNum, from LogPos,
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

	waiter async.KeyWaiter[NodeID]
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
	s.waiter = async.NewKeyWaiter[NodeID](&s.mut)
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
		s.waiter.Broadcast()
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
			Entry:   e,
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

func (s *acceptorLogicImpl) AcceptEntries(
	input AcceptEntriesInput,
) (AcceptEntriesOutput, error) {
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

	oldFullyReplicated := s.log.GetFullyReplicated()

	posList := make([]LogPos, 0, len(input.Entries))
	putEntries := make([]LogEntry, 0, len(input.Entries))
	for _, entry := range input.Entries {
		posList = append(posList, entry.Pos)

		if entry.Pos > oldFullyReplicated {
			// only update entries with pos > fully replicated
			putEntries = append(putEntries, entry)
		}
	}

	markCommitted := s.getNeedUpdateTermToInf(input.Committed)

	// set term of entries to = +infinity
	for i := range putEntries {
		s.updateTermToInf(&putEntries[i])
	}

	s.log.UpsertEntries(putEntries, markCommitted)

	newReplicatedPos := s.log.GetFullyReplicated()

	// set last committed if less than replicated pos
	if s.lastCommitted < newReplicatedPos {
		s.lastCommitted = newReplicatedPos
	}

	if newReplicatedPos > oldFullyReplicated {
		s.waiter.Broadcast()
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

func (s *acceptorLogicImpl) updateTermToInf(entry *LogEntry) {
	if entry.Pos > s.lastCommitted {
		return
	}
	entry.Term = InfiniteTerm{}
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
			if entry.Type == LogTypeNull {
				continue
			}
			if s.isSameTerm(entry.Term) {
				markCommitted = append(markCommitted, entry.Pos)
			}
		}

		s.lastCommitted += LogPos(getLimit)
		s.waiter.Broadcast()

		if overLimit {
			s.log.UpsertEntries(nil, markCommitted)
		} else {
			return markCommitted
		}
	}

	return nil
}

func (s *acceptorLogicImpl) GetNeedReplicatedPos(
	ctx async.Context,
	term TermNum, from LogPos,
	lastFullyReplicated LogPos,
) (NeedReplicatedInput, error) {
	var resultInput NeedReplicatedInput
	var resultErr error

	s.GetNeedReplicatedPosAsync(
		ctx, term, from, lastFullyReplicated,
		func(input NeedReplicatedInput, err error) {
			resultInput = input
			resultErr = err
		},
	)

	return resultInput, resultErr
}

func (s *acceptorLogicImpl) GetNeedReplicatedPosAsync(
	ctx async.Context,
	term TermNum, from LogPos,
	lastFullyReplicated LogPos,
	callback func(input NeedReplicatedInput, err error),
) {
	s.waiter.Run(ctx, s.currentNode, func(ctx async.Context, err error) (async.WaitStatus, error) {
		if err != nil {
			callback(NeedReplicatedInput{}, err)
			return 0, err
		}

		var output NeedReplicatedInput
		status, err := s.doGetNeedReplicatedPosCallback(term, from, lastFullyReplicated, &output)
		if err != nil {
			callback(NeedReplicatedInput{}, err)
			return 0, err
		}

		if status != async.WaitStatusSuccess {
			return status, nil
		}

		callback(output, nil)
		return async.WaitStatusSuccess, nil
	})
}

func (s *acceptorLogicImpl) doGetNeedReplicatedPosCallback(
	term TermNum, from LogPos,
	lastFullyReplicated LogPos,
	output *NeedReplicatedInput,
) (async.WaitStatus, error) {
	if !s.updateTermNum(term) {
		return 0, fmt.Errorf("input term is less than actual term")
	}

	afterFullyReplicated := s.log.GetFullyReplicated() + 1
	if from < afterFullyReplicated {
		from = afterFullyReplicated
	}

	if s.lastCommitted < from && lastFullyReplicated >= s.log.GetFullyReplicated() {
		return async.WaitStatusWaiting, nil
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

		var entry LogEntry
		if index < len(entries) {
			entry = entries[index]
		} else {
			entry = NewNullEntry(pos)
		}

		if entry.Type == LogTypeNull {
			posList = append(posList, entry.Pos)
		} else if entry.Term.IsFinite {
			posList = append(posList, entry.Pos)
		}
	}

	*output = NeedReplicatedInput{
		Term:     s.log.GetTerm(),
		FromNode: s.currentNode,
		PosList:  posList,
		NextPos:  maxPos + 1,

		FullyReplicated: s.log.GetFullyReplicated(),
	}

	return async.WaitStatusSuccess, nil
}

func (s *acceptorLogicImpl) GetCommittedEntriesWithWait(
	ctx async.Context, term TermNum,
	fromPos LogPos, limit int,
) (GetCommittedEntriesOutput, error) {
	var output GetCommittedEntriesOutput
	var outputErr error

	s.waiter.Run(ctx, s.currentNode, func(ctx async.Context, err error) (async.WaitStatus, error) {
		if err != nil {
			outputErr = err
			return 0, err
		}

		status, err := s.doGetCommittedEntriesWithWaitCallback(term, fromPos, limit, &output)
		if err != nil {
			outputErr = err
			return 0, err
		}

		return status, nil
	})

	return output, outputErr
}

func (s *acceptorLogicImpl) doGetCommittedEntriesWithWaitCallback(
	term TermNum,
	fromPos LogPos, limit int,
	output *GetCommittedEntriesOutput,
) (async.WaitStatus, error) {
	if !s.updateTermNum(term) {
		return 0, fmt.Errorf("input term is less than actual term")
	}

	if fromPos > s.log.GetFullyReplicated() {
		return async.WaitStatusWaiting, nil
	}

	maxPos := fromPos + LogPos(limit-1)
	if maxPos > s.log.GetFullyReplicated() {
		maxPos = s.log.GetFullyReplicated()
	}
	newLimit := maxPos - fromPos + 1

	entries := s.log.GetEntries(fromPos, int(newLimit))
	*output = GetCommittedEntriesOutput{
		Entries: entries,
		NextPos: maxPos + 1,
	}

	return async.WaitStatusSuccess, nil
}

func (s *acceptorLogicImpl) CheckInvariant() {
	s.mut.Lock()
	defer s.mut.Unlock()

	AssertTrue(s.log.GetFullyReplicated() <= s.lastCommitted)

	for pos := LogPos(1); pos <= s.log.GetFullyReplicated(); pos++ {
		entry := s.log.GetEntriesWithPos(pos)[0]
		AssertTrue(entry.Pos == pos)
		AssertTrue(!entry.Term.IsFinite)
		AssertTrue(!entry.IsNull())
	}
}

func (s *acceptorLogicImpl) GetLastCommitted() LogPos {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.lastCommitted
}
