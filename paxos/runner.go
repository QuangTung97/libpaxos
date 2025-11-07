package paxos

import (
	"time"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos/key_runner"
)

type NodeRunner interface {
	StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{}) bool
	StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{}) bool
	StartStateMachine(term TermNum, info StateMachineRunnerInfo) bool

	// StartFetchingFollowerInfoRunners add fast leader switch
	StartFetchingFollowerInfoRunners(term TermNum, nodes map[NodeID]struct{}) bool
	StartElectionRunner(info ElectionRunnerInfo) bool
}

type StateMachineRunnerInfo struct {
	Running       bool
	IsLeader      bool // when state = Candidate or state = Leader
	AcceptCommand bool
}

type ElectionRunnerInfo struct {
	Term         TermNum
	Started      bool
	MaxTermValue TermValue
	Chosen       NodeID
}

type nodeTermInfo struct {
	nodeID       NodeID
	term         TermNum
	info         StateMachineRunnerInfo
	maxTermValue TermValue
}

func (i nodeTermInfo) getNodeID() NodeID {
	return i.nodeID
}

type nodeRunnerImpl struct {
	currentNodeID NodeID

	voters        *key_runner.KeyRunner[NodeID, nodeTermInfo]
	acceptors     *key_runner.KeyRunner[NodeID, nodeTermInfo]
	replicators   *key_runner.KeyRunner[NodeID, nodeTermInfo]
	stateMachine  *key_runner.KeyRunner[NodeID, nodeTermInfo]
	fetchFollower *key_runner.KeyRunner[NodeID, nodeTermInfo]
	startElection *key_runner.KeyRunner[NodeID, nodeTermInfo]
}

func NewNodeRunner(
	currentNodeID NodeID,
	voteRunnerFunc func(ctx async.Context, nodeID NodeID, term TermNum) error,
	acceptorRunnerFunc func(ctx async.Context, nodeID NodeID, term TermNum) error,
	replicateRunnerFunc func(ctx async.Context, nodeID NodeID, term TermNum) error,
	stateMachineFunc func(ctx async.Context, term TermNum, info StateMachineRunnerInfo) error,
	fetchFollowerInfoFunc func(ctx async.Context, nodeID NodeID, term TermNum) error,
	startElectionFunc func(ctx async.Context, nodeID NodeID, maxTermVal TermValue) error,
) (NodeRunner, func()) {
	r := &nodeRunnerImpl{
		currentNodeID: currentNodeID,
	}

	loopWithSleep := func(ctx async.Context, callback func(ctx async.Context) error) {
		for {
			_ = callback(ctx)
			sleepWithContext(ctx, 1000*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
	}

	r.voters = key_runner.New(nodeTermInfo.getNodeID, func(ctx async.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx async.Context) error {
			return voteRunnerFunc(ctx, val.nodeID, val.term)
		})
	})

	r.acceptors = key_runner.New(nodeTermInfo.getNodeID, func(ctx async.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx async.Context) error {
			return acceptorRunnerFunc(ctx, val.nodeID, val.term)
		})
	})

	r.replicators = key_runner.New(nodeTermInfo.getNodeID, func(ctx async.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx async.Context) error {
			return replicateRunnerFunc(ctx, val.nodeID, val.term)
		})
	})

	r.stateMachine = key_runner.New(nodeTermInfo.getNodeID, func(ctx async.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx async.Context) error {
			return stateMachineFunc(ctx, val.term, val.info)
		})
	})

	r.fetchFollower = key_runner.New(nodeTermInfo.getNodeID, func(ctx async.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx async.Context) error {
			return fetchFollowerInfoFunc(ctx, val.nodeID, val.term)
		})
	})

	r.startElection = key_runner.New(nodeTermInfo.getNodeID, func(ctx async.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx async.Context) error {
			return startElectionFunc(ctx, val.nodeID, val.maxTermValue)
		})
	})

	return r, func() {
		r.voters.Shutdown()
		r.acceptors.Shutdown()
		r.replicators.Shutdown()
		r.stateMachine.Shutdown()
		r.fetchFollower.Shutdown()
		r.startElection.Shutdown()
	}
}

func sleepWithContext(ctx async.Context, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.ToContext().Done():
	}
}

func (r *nodeRunnerImpl) StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{}) bool {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID: id,
			term:   term,
		})
	}
	return r.voters.Upsert(infos)
}

func (r *nodeRunnerImpl) StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{}) bool {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID: id,
			term:   term,
		})
	}
	updated := r.acceptors.Upsert(infos)
	r.replicators.Upsert(infos)
	return updated
}

func (r *nodeRunnerImpl) StartStateMachine(term TermNum, info StateMachineRunnerInfo) bool {
	if info.Running {
		infos := []nodeTermInfo{
			{
				nodeID: r.currentNodeID,
				term:   term,
				info:   info,
			},
		}
		return r.stateMachine.Upsert(infos)
	}

	return r.stateMachine.Upsert(nil)
}

func (r *nodeRunnerImpl) StartFetchingFollowerInfoRunners(
	term TermNum, nodes map[NodeID]struct{},
) bool {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID: id,
			term:   term,
		})
	}
	return r.fetchFollower.Upsert(infos)
}

func (r *nodeRunnerImpl) StartElectionRunner(info ElectionRunnerInfo) bool {
	if info.Started {
		infos := []nodeTermInfo{
			{
				nodeID:       info.Chosen,
				term:         info.Term,
				maxTermValue: info.MaxTermValue,
			},
		}
		return r.startElection.Upsert(infos)
	}

	return r.startElection.Upsert(nil)
}
