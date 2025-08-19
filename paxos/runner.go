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
	StartFollowerRunner(term TermNum, isRunning bool)
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

	voters       *key_runner.KeyRunner[NodeID, nodeTermInfo]
	acceptors    *key_runner.KeyRunner[NodeID, nodeTermInfo]
	stateMachine *key_runner.KeyRunner[NodeID, nodeTermInfo]
	follower     *key_runner.KeyRunner[NodeID, nodeTermInfo]
}

func NewNodeRunner(
	currentNodeID NodeID,
	voteRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	acceptorRunnerFunc func(ctx context.Context, nodeID NodeID, term TermNum) error,
	stateMachineFunc func(ctx context.Context, term TermNum, isLeader bool) error,
	followerRunnerFunc func(ctx context.Context, term TermNum) error,
) (NodeRunner, func()) {
	r := &nodeRunnerImpl{
		currentNodeID: currentNodeID,
	}

	r.voters = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		for {
			_ = voteRunnerFunc(ctx, val.nodeID, val.term)
			sleepWithContext(ctx, 1000*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
	})

	r.acceptors = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		for {
			_ = acceptorRunnerFunc(ctx, val.nodeID, val.term)
			sleepWithContext(ctx, 1000*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
	})

	r.stateMachine = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		for {
			_ = stateMachineFunc(ctx, val.term, val.isLeader)
			sleepWithContext(ctx, 1000*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
	})

	r.follower = key_runner.New(nodeTermInfo.getNodeID, func(ctx context.Context, val nodeTermInfo) {
		for {
			_ = followerRunnerFunc(ctx, val.term)
			sleepWithContext(ctx, 1000*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
	})

	return r, func() {
		r.voters.Shutdown()
		r.acceptors.Shutdown()
		r.stateMachine.Shutdown()
		r.follower.Shutdown()
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
	r.voters.Upsert(infos)
}

func (r *nodeRunnerImpl) SetLeader(term TermNum, isLeader bool) {
	infos := []nodeTermInfo{
		{
			nodeID:   r.currentNodeID,
			term:     term,
			isLeader: isLeader,
		},
	}
	r.voters.Upsert(infos)
}

func (r *nodeRunnerImpl) StartFollowerRunner(term TermNum, isRunning bool) {
	if isRunning {
		infos := []nodeTermInfo{
			{
				nodeID: r.currentNodeID,
				term:   term,
			},
		}
		r.voters.Upsert(infos)
	} else {
		r.voters.Upsert(nil)
	}
}
