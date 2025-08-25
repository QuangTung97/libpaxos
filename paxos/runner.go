package paxos

import (
	"context"
	"time"

	"github.com/QuangTung97/libpaxos/paxos/key_runner"
)

type NodeRunner interface {
	StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{})
	StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{})
	StartStateMachine(term TermNum, info StateMachineRunnerInfo)

	// StartFetchingFollowerInfoRunners add fast leader switch
	StartFetchingFollowerInfoRunners(term TermNum, nodes map[NodeID]struct{}, retryCount int)
	StartElectionRunner(termValue TermValue, started bool, chosen NodeID, retryCount int)
}

type StateMachineRunnerInfo struct {
	Running       bool
	IsLeader      bool // when state = Candidate or state = Leader
	AcceptCommand bool
}

type nodeTermInfo struct {
	nodeID     NodeID
	term       TermNum
	retryCount int
	info       StateMachineRunnerInfo
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
	voteRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	acceptorRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	replicateRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	stateMachineFunc func(ctx context.Context, term TermNum, info StateMachineRunnerInfo) error,
	fetchFollowerInfoFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	startElectionFunc func(ctx context.Context, nodeID NodeID, maxTermVal TermValue) error,
) (NodeRunner, func()) {
	r := &nodeRunnerImpl{
		currentNodeID: currentNodeID,
	}

	loopWithSleep := func(ctx context.Context, callback func(ctx context.Context) error) {
		for {
			_ = callback(ctx)
			sleepWithContext(ctx, 1000*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
	}

	r.voters = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return voteRunnerFunc(ctx, val.nodeID, val.term)
		})
	})

	r.acceptors = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return acceptorRunnerFunc(ctx, val.nodeID, val.term)
		})
	})

	r.replicators = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return replicateRunnerFunc(ctx, val.nodeID, val.term)
		})
	})

	r.stateMachine = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return stateMachineFunc(ctx, val.term, val.info)
		})
	})

	r.fetchFollower = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return fetchFollowerInfoFunc(ctx, val.nodeID, val.term)
		})
	})

	r.startElection = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return startElectionFunc(ctx, val.nodeID, val.term.Num)
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

func sleepWithContext(ctx context.Context, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.Done():
	}
}

func (r *nodeRunnerImpl) StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{}) {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID: id,
			term:   term,
		})
	}
	r.voters.Upsert(infos)
}

func (r *nodeRunnerImpl) StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{}) {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID: id,
			term:   term,
		})
	}
	r.acceptors.Upsert(infos)
	r.replicators.Upsert(infos)
}

func (r *nodeRunnerImpl) StartStateMachine(term TermNum, info StateMachineRunnerInfo) {
	if info.Running {
		infos := []nodeTermInfo{
			{
				nodeID: r.currentNodeID,
				term:   term,
				info:   info,
			},
		}
		r.stateMachine.Upsert(infos)
	} else {
		r.stateMachine.Upsert(nil)
	}
}

func (r *nodeRunnerImpl) StartFetchingFollowerInfoRunners(
	term TermNum, nodes map[NodeID]struct{}, retryCount int,
) {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID:     id,
			term:       term,
			retryCount: retryCount,
		})
	}
	r.fetchFollower.Upsert(infos)
}

func (r *nodeRunnerImpl) StartElectionRunner(
	termValue TermValue, started bool, chosen NodeID, retryCount int,
) {
	if started {
		infos := []nodeTermInfo{
			{
				nodeID: chosen,
				term: TermNum{
					Num: termValue,
				},
				retryCount: retryCount,
			},
		}
		r.startElection.Upsert(infos)
	} else {
		r.startElection.Upsert(nil)
	}
}
