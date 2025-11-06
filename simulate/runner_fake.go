package simulate

import (
	"slices"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
)

type RunnerFake struct {
	rt *async.SimulateRuntime

	voteRunnerFunc          func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum)
	acceptorRunnerFunc      func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum)
	fetchFollowerRunnerFunc func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum, retryCount int)

	voteMap          map[paxos.NodeID]basicRunnerInfo
	acceptorMap      map[paxos.NodeID]basicRunnerInfo
	fetchFollowerMap map[paxos.NodeID]fetchFollowerInfo
}

type basicRunnerInfo struct {
	ctx  async.Context
	term paxos.TermNum
}

type fetchFollowerInfo struct {
	ctx   async.Context
	term  paxos.TermNum
	retry int
}

var _ paxos.NodeRunner = &RunnerFake{}

func NewRunnerFake(
	rt *async.SimulateRuntime,
	voteRunnerFunc func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum),
	acceptorRunnerFunc func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum),
	fetchFollowerRunnerFunc func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum, retryCount int),
) *RunnerFake {
	return &RunnerFake{
		rt: rt,

		voteRunnerFunc:          voteRunnerFunc,
		acceptorRunnerFunc:      acceptorRunnerFunc,
		fetchFollowerRunnerFunc: fetchFollowerRunnerFunc,

		voteMap:          map[paxos.NodeID]basicRunnerInfo{},
		acceptorMap:      map[paxos.NodeID]basicRunnerInfo{},
		fetchFollowerMap: map[paxos.NodeID]fetchFollowerInfo{},
	}
}

func (r *RunnerFake) StartVoteRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var changed bool

	for id, oldState := range r.voteMap {
		_, ok := nodes[id]
		if ok {
			continue
		}

		oldState.ctx.Cancel()
		delete(r.voteMap, id)
		changed = true
	}

	for _, id := range nodesToSlice(nodes) {
		oldState, ok := r.voteMap[id]
		if ok && oldState.term == term {
			continue
		}

		ctx := r.rt.NewThread(func(ctx async.Context) {
			r.voteRunnerFunc(ctx, id, term)
		})

		r.voteMap[id] = basicRunnerInfo{
			ctx:  ctx,
			term: term,
		}
		changed = true
	}

	return changed
}

func (r *RunnerFake) StartAcceptRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var changed bool

	for id, oldState := range r.acceptorMap {
		_, ok := nodes[id]
		if ok {
			continue
		}

		oldState.ctx.Cancel()
		delete(r.acceptorMap, id)
		changed = true
	}

	for _, id := range nodesToSlice(nodes) {
		oldState, ok := r.acceptorMap[id]
		if ok && oldState.term == term {
			continue
		}

		ctx := r.rt.NewThread(func(ctx async.Context) {
			r.acceptorRunnerFunc(ctx, id, term)
		})

		r.acceptorMap[id] = basicRunnerInfo{
			ctx:  ctx,
			term: term,
		}
		changed = true
	}

	return changed
}

func (r *RunnerFake) StartStateMachine(
	term paxos.TermNum, info paxos.StateMachineRunnerInfo,
) bool {
	return false
}

func (r *RunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{}, retryCount int,
) bool {
	var changed bool

	for id, oldState := range r.fetchFollowerMap {
		_, ok := nodes[id]
		if ok {
			continue
		}

		oldState.ctx.Cancel()
		delete(r.fetchFollowerMap, id)
		changed = true
	}

	for _, id := range nodesToSlice(nodes) {
		oldState, ok := r.fetchFollowerMap[id]
		if ok && oldState.term == term && oldState.retry == retryCount {
			continue
		}

		ctx := r.rt.NewThread(func(ctx async.Context) {
			r.fetchFollowerRunnerFunc(ctx, id, term, retryCount)
		})

		r.fetchFollowerMap[id] = fetchFollowerInfo{
			ctx:   ctx,
			term:  term,
			retry: retryCount,
		}
		changed = true
	}

	return changed
}

func (r *RunnerFake) StartElectionRunner(
	termValue paxos.TermValue, started bool, chosen paxos.NodeID, retryCount int,
) bool {
	return false
}

func nodesToSlice(nodes map[paxos.NodeID]struct{}) []paxos.NodeID {
	result := make([]paxos.NodeID, 0, len(nodes))
	for id := range nodes {
		result = append(result, id)
	}
	slices.SortFunc(result, paxos.CompareNodeID)
	return result
}
