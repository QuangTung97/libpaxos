package paxos

import (
	"context"
	"time"

	"github.com/QuangTung97/libpaxos/paxos/key_runner"
)

type NodeRunner interface {
	StartVoteRequestRunners(term TermNum, nodes map[NodeID]struct{})
	StartAcceptRequestRunners(term TermNum, nodes map[NodeID]struct{})
	SetLeader(term TermNum, isLeader bool)
	StartFetchingFollowerInfoRunners(term TermNum, nodes map[NodeID]struct{})
}

type nodeTermInfo struct {
	nodeID   NodeID
	term     TermNum
	isLeader bool
}

func (i nodeTermInfo) getNodeID() NodeID {
	return i.nodeID
}

type nodeRunnerImpl struct {
	currentNodeID NodeID

	voters        *key_runner.KeyRunner[NodeID, nodeTermInfo]
	acceptors     *key_runner.KeyRunner[NodeID, nodeTermInfo]
	stateMachine  *key_runner.KeyRunner[NodeID, nodeTermInfo]
	fetchFollower *key_runner.KeyRunner[NodeID, nodeTermInfo]

	// TODO add replicate runner
}

func NewNodeRunner(
	currentNodeID NodeID,
	voteRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	acceptorRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	stateMachineFunc func(ctx context.Context, term TermNum, isLeader bool) error,
	fetchFollowerInfoFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
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

	r.stateMachine = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return stateMachineFunc(ctx, val.term, val.isLeader)
		})
	})

	r.fetchFollower = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		loopWithSleep(ctx, func(ctx context.Context) error {
			return fetchFollowerInfoFunc(ctx, val.nodeID, val.term)
		})
	})

	return r, func() {
		r.voters.Shutdown()
		r.acceptors.Shutdown()
		r.stateMachine.Shutdown()
		r.fetchFollower.Shutdown()
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
}

func (r *nodeRunnerImpl) SetLeader(term TermNum, isLeader bool) {
	infos := []nodeTermInfo{
		{
			nodeID:   r.currentNodeID,
			term:     term,
			isLeader: isLeader,
		},
	}
	r.stateMachine.Upsert(infos)
}

func (r *nodeRunnerImpl) StartFetchingFollowerInfoRunners(
	term TermNum, nodes map[NodeID]struct{},
) {
	infos := make([]nodeTermInfo, 0, len(nodes))
	for id := range nodes {
		infos = append(infos, nodeTermInfo{
			nodeID: id,
			term:   term,
		})
	}
	r.fetchFollower.Upsert(infos)
}
