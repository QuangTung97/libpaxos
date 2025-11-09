package simulate

import (
	"fmt"
	"slices"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
)

type RunnerFake struct {
	currentNodeID paxos.NodeID

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
	currentNodeID paxos.NodeID,
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
		currentNodeID: currentNodeID,

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

func buildVoteDetail(from, to paxos.NodeID) string {
	return fmt.Sprintf("vote[%s=>%s]", nodeString(from), nodeString(to))
}

func buildAcceptDetail(from, to paxos.NodeID) string {
	return fmt.Sprintf("accept[%s=>%s]", nodeString(from), nodeString(to))
}

func (r *RunnerFake) StartVoteRequestRunners(
	term paxos.TermNum, nodes map[paxos.NodeID]struct{},
) bool {
	var changed bool

	for _, id := range nodesToSlice(r.voteMap) {
		_, ok := nodes[id]
		if ok {
			continue
		}

		oldState := r.voteMap[id]
		oldState.ctx.Cancel()

		delete(r.voteMap, id)
		changed = true
	}

	for _, id := range nodesToSlice(nodes) {
		oldState, ok := r.voteMap[id]
		if ok && oldState.term == term {
			continue
		}

		detail := buildVoteDetail(r.currentNodeID, id)
		ctx := r.rt.NewThread(detail, func(ctx async.Context) {
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

	for _, id := range nodesToSlice(r.acceptorMap) {
		_, ok := nodes[id]
		if ok {
			continue
		}

		oldState := r.acceptorMap[id]
		oldState.ctx.Cancel()

		delete(r.acceptorMap, id)
		changed = true
	}

	for _, id := range nodesToSlice(nodes) {
		oldState, ok := r.acceptorMap[id]
		if ok && oldState.term == term {
			continue
		}

		detail := buildAcceptDetail(r.currentNodeID, id)
		ctx := r.rt.NewThread(detail, func(ctx async.Context) {
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

func buildStateMachineDetail(id paxos.NodeID) string {
	return fmt.Sprintf("state-machine[%s]", nodeString(id))
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

	detail := buildStateMachineDetail(r.currentNodeID)
	r.stateMachineCtx = r.rt.NewThread(detail, func(ctx async.Context) {
		r.stateMachineFunc(ctx, term, info)
	})

	return true
}

func buildFetchDetail(from, to paxos.NodeID) string {
	return fmt.Sprintf("fetch[%s=>%s]", nodeString(from), nodeString(to))
}

func (r *RunnerFake) StartFetchingFollowerInfoRunners(
	term paxos.TermNum, generation paxos.FollowerGeneration, nodes map[paxos.NodeID]struct{},
) bool {
	var changed bool

	for _, id := range nodesToSlice(r.fetchFollowerMap) {
		_, ok := nodes[id]
		if ok {
			continue
		}

		oldState := r.fetchFollowerMap[id]
		oldState.ctx.Cancel()

		delete(r.fetchFollowerMap, id)
		changed = true
	}

	for _, id := range nodesToSlice(nodes) {
		oldState, ok := r.fetchFollowerMap[id]
		if ok && oldState.term == term && oldState.generation == generation {
			continue
		}

		detail := buildFetchDetail(r.currentNodeID, id)
		ctx := r.rt.NewThread(detail, func(ctx async.Context) {
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

func buildStartElectionDetail(from, to paxos.NodeID) string {
	return fmt.Sprintf("start-election[%s=>%s]", nodeString(from), nodeString(to))
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

	detail := buildStartElectionDetail(r.currentNodeID, newInfo.Chosen)
	r.electionCtx = r.rt.NewThread(detail, func(ctx async.Context) {
		r.startElectionFunc(ctx, newInfo.Chosen, newInfo.MaxTermValue)
	})

	return true
}

func (r *RunnerFake) getAllContexts() []async.Context {
	result := make([]async.Context, 0, 6)
	for _, id := range nodesToSlice(r.voteMap) {
		ctx := r.voteMap[id].ctx
		if ctx != nil {
			result = append(result, ctx)
		}
	}

	for _, id := range nodesToSlice(r.acceptorMap) {
		ctx := r.acceptorMap[id].ctx
		if ctx != nil {
			result = append(result, ctx)
		}
	}

	for _, id := range nodesToSlice(r.fetchFollowerMap) {
		ctx := r.fetchFollowerMap[id].ctx
		if ctx != nil {
			result = append(result, ctx)
		}
	}

	if r.stateMachineCtx != nil {
		result = append(result, r.stateMachineCtx)
	}

	if r.electionCtx != nil {
		result = append(result, r.electionCtx)
	}

	return result
}

func nodesToSlice[T any](nodes map[paxos.NodeID]T) []paxos.NodeID {
	result := make([]paxos.NodeID, 0, len(nodes))
	for id := range nodes {
		result = append(result, id)
	}
	slices.SortFunc(result, paxos.CompareNodeID)
	return result
}

func nodeString(id paxos.NodeID) string {
	return id.String()[:4]
}
