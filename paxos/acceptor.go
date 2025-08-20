package paxos

type AcceptorLogic interface {
	HandleRequestVote(input RequestVoteInput, lastPos LogPos) RequestVoteOutput
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

func (s *acceptorLogicImpl) HandleRequestVote(input RequestVoteInput, lastPos LogPos) RequestVoteOutput {
	return RequestVoteOutput{}
}

func (s *acceptorLogicImpl) AcceptEntries(input AcceptEntriesInput) AcceptEntriesOutput {
	return AcceptEntriesOutput{}
}
