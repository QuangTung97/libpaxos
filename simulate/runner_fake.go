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
	fetchFollowerRunnerFunc func(
		ctx async.Context, nodeID paxos.NodeID,
		term paxos.TermNum, generation paxos.FollowerGeneration,
	)
	stateMachineFunc  func(ctx async.Context, term paxos.TermNum, info paxos.StateMachineRunnerInfo)
	startElectionFunc func(ctx async.Context, chosen paxos.NodeID, termValue paxos.TermValue)

	voteMap          map[paxos.NodeID]basicRunnerInfo
	acceptorMap      map[paxos.NodeID]basicRunnerInfo
	fetchFollowerMap map[paxos.NodeID]basicRunnerInfo

	stateMachineTerm paxos.TermNum
	stateMachineInfo paxos.StateMachineRunnerInfo
	stateMachineCtx  async.Context

	electionCtx  async.Context
	electionInfo paxos.ElectionRunnerInfo
}

type basicRunnerInfo struct {
	ctx        async.Context
	term       paxos.TermNum
	generation paxos.FollowerGeneration
}

var _ paxos.NodeRunner = &RunnerFake{}

func NewRunnerFake(
	rt *async.SimulateRuntime,
	voteRunnerFunc func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum),
	acceptorRunnerFunc func(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum),
	fetchFollowerRunnerFunc func(
		ctx async.Context, nodeID paxos.NodeID,
		term paxos.TermNum, generation paxos.FollowerGeneration,
	),
	stateMachineFunc func(ctx async.Context, term paxos.TermNum, info paxos.StateMachineRunnerInfo),
	startElectionFunc func(ctx async.Context, chosen paxos.NodeID, termValue paxos.TermValue),
) *RunnerFake {
	return &RunnerFake{
		rt: rt,

		voteRunnerFunc:          voteRunnerFunc,
		acceptorRunnerFunc:      acceptorRunnerFunc,
		fetchFollowerRunnerFunc: fetchFollowerRunnerFunc,
		stateMachineFunc:        stateMachineFunc,
		startElectionFunc:       startElectionFunc,

		voteMap:          map[paxos.NodeID]basicRunnerInfo{},
		acceptorMap:      map[paxos.NodeID]basicRunnerInfo{},
		fetchFollowerMap: map[paxos.NodeID]basicRunnerInfo{},
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
	if term == r.stateMachineTerm && info == r.stateMachineInfo {
		return false
	}

	r.stateMachineTerm = term
	r.stateMachineInfo = info

	if r.stateMachineCtx != nil {
		r.stateMachineCtx.Cancel()
		r.stateMachineCtx = nil
	}

	if !info.Running {
		return true
	}

	r.stateMachineCtx = r.rt.NewThread(func(ctx async.Context) {
		r.stateMachineFunc(ctx, term, info)
	})

	return true
}

func (r *RunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, generation paxos.FollowerGeneration, nodes map[paxos.NodeID]struct{},
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
		if ok && oldState.term == term && oldState.generation == generation {
			continue
		}

		ctx := r.rt.NewThread(func(ctx async.Context) {
			r.fetchFollowerRunnerFunc(ctx, id, term, generation)
		})

		r.fetchFollowerMap[id] = basicRunnerInfo{
			ctx:        ctx,
			term:       term,
			generation: generation,
		}
		changed = true
	}

	return changed
}

func (r *RunnerFake) StartElectionRunner(newInfo paxos.ElectionRunnerInfo) bool {
	if r.electionInfo == newInfo {
		return false
	}

	r.electionInfo = newInfo
	if r.electionCtx != nil {
		r.electionCtx.Cancel()
		r.electionCtx = nil
	}

	if !newInfo.Started {
		return true
	}

	r.electionCtx = r.rt.NewThread(func(ctx async.Context) {
		r.startElectionFunc(ctx, newInfo.Chosen, newInfo.MaxTermValue)
	})

	return true
}

func nodesToSlice(nodes map[paxos.NodeID]struct{}) []paxos.NodeID {
	result := make([]paxos.NodeID, 0, len(nodes))
	for id := range nodes {
		result = append(result, id)
	}
	slices.SortFunc(result, paxos.CompareNodeID)
	return result
}
