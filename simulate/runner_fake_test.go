package simulate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
)

var testTerm01 = paxos.TermNum{
	Num:    21,
	NodeID: nodeID2,
}

func newNodeSet(nodes ...paxos.NodeID) map[paxos.NodeID]struct{} {
	result := make(map[paxos.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		result[n] = struct{}{}
	}
	return result
}

type runnerFakeTest struct {
	rt      *async.SimulateRuntime
	actions *actionListTest
	runner  *RunnerFake

	ctxList []async.Context
}

func newRunnerFakeTest() *runnerFakeTest {
	r := &runnerFakeTest{}

	r.rt = async.NewSimulateRuntime()
	r.actions = newActionListTest()

	r.runner = NewRunnerFake(
		r.rt,
		r.voteFunc,
		r.acceptorFunc,
		r.fetchFollowerFunc,
		r.stateMachineFunc,
		r.startElectionFunc,
	)

	return r
}

func (r *runnerFakeTest) voteFunc(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
	r.ctxList = append(r.ctxList, ctx)
	r.actions.add("start-vote:%s,%d", nodeID.String()[:4], term.Num)
	r.rt.AddNext(ctx, func(ctx async.Context) {
		r.actions.add("vote-action01:%s,%d", nodeID.String()[:4], term.Num)
	})
}

func (r *runnerFakeTest) acceptorFunc(ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum) {
	r.ctxList = append(r.ctxList, ctx)
	r.actions.add("start-accept:%s,%d", nodeID.String()[:4], term.Num)
	r.rt.AddNext(ctx, func(ctx async.Context) {
		r.actions.add("accept-action01:%s,%d", nodeID.String()[:4], term.Num)
	})
}

func (r *runnerFakeTest) fetchFollowerFunc(
	ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum,
) {
	r.ctxList = append(r.ctxList, ctx)
	r.actions.add("start-fetch-follower:%s,%d", nodeID.String()[:4], term.Num)
	r.rt.AddNext(ctx, func(ctx async.Context) {
		r.actions.add("fetch-follower-action01:%s,%d", nodeID.String()[:4], term.Num)
	})
}

func (r *runnerFakeTest) stateMachineFunc(
	ctx async.Context, term paxos.TermNum, info paxos.StateMachineRunnerInfo,
) {
	r.ctxList = append(r.ctxList, ctx)
	r.actions.add("start-state-machine:%d,running=%v", term.Num, info.Running)
	r.rt.AddNext(ctx, func(ctx async.Context) {
		r.actions.add("state-machine-action01:%d", term.Num)
	})
}

func (r *runnerFakeTest) startElectionFunc(
	ctx async.Context, chosen paxos.NodeID, termValue paxos.TermValue,
) {
	r.ctxList = append(r.ctxList, ctx)
	r.actions.add("start-election:%v,term=%d", chosen.String()[:4], termValue)
	r.rt.AddNext(ctx, func(ctx async.Context) {
		r.actions.add("election-action01:%d", termValue)
	})
}

func TestRunnerFake_AcceptRequestRunners(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartAcceptRequestRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-accept:6401,20",
		"start-accept:6402,20",
	}, r.actions.getList())

	// run again with same set of values
	ok = r.runner.StartAcceptRequestRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"accept-action01:6401,20",
		"accept-action01:6402,20",
	}, r.actions.getList())

	// run again with empty nodes
	ok = r.runner.StartAcceptRequestRunners(initTerm, newNodeSet())
	assert.Equal(t, true, ok)

	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, context.Canceled, r.ctxList[1].Err())
}

func TestRunnerFake_AcceptRequestRunners__Different_Term(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartAcceptRequestRunners(initTerm, newNodeSet(nodeID1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	// run again with term changed
	ok = r.runner.StartAcceptRequestRunners(testTerm01, newNodeSet(nodeID1))
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"start-accept:6401,20",
		"start-accept:6401,21",
		"accept-action01:6401,20",
		"accept-action01:6401,21",
	}, r.actions.getList())
}

func TestRunnerFake_AcceptRequestRunners__Empty(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartAcceptRequestRunners(initTerm, newNodeSet())
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{}, r.actions.getList())
}

func TestRunnerFake_VoteRequestRunners(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartVoteRequestRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-vote:6401,20",
		"start-vote:6402,20",
	}, r.actions.getList())

	// run again with same set of values
	ok = r.runner.StartVoteRequestRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"vote-action01:6401,20",
		"vote-action01:6402,20",
	}, r.actions.getList())

	// run again with empty nodes
	ok = r.runner.StartVoteRequestRunners(initTerm, newNodeSet())
	assert.Equal(t, true, ok)

	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, context.Canceled, r.ctxList[1].Err())
}

func TestRunnerFake_FetchingFollowerInfoRunners(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-fetch-follower:6401,20",
		"start-fetch-follower:6402,20",
	}, r.actions.getList())

	// run again with same set of values
	ok = r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"fetch-follower-action01:6401,20",
		"fetch-follower-action01:6402,20",
	}, r.actions.getList())

	// run again with empty nodes
	ok = r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet())
	assert.Equal(t, true, ok)

	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, context.Canceled, r.ctxList[1].Err())
}

func TestRunnerFake_FetchingFollowerInfoRunners__Increase_Retry(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-fetch-follower:6401,20",
		"start-fetch-follower:6402,20",
	}, r.actions.getList())

	// run again with new term
	ok = r.runner.StartFetchingFollowerInfoRunners(testTerm01, newNodeSet(nodeID1, nodeID2))
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"fetch-follower-action01:6401,20",
		"fetch-follower-action01:6402,20",
		"start-fetch-follower:6401,21",
		"start-fetch-follower:6402,21",
		"fetch-follower-action01:6401,21",
		"fetch-follower-action01:6402,21",
	}, r.actions.getList())
}

func TestRunnerFake_StartStateMachine(t *testing.T) {
	r := newRunnerFakeTest()

	info := paxos.StateMachineRunnerInfo{
		Running:       true,
		IsLeader:      true,
		AcceptCommand: true,
	}

	// start
	ok := r.runner.StartStateMachine(initTerm, info)
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-state-machine:20,running=true",
	}, r.actions.getList())

	// with same params
	ok = r.runner.StartStateMachine(initTerm, info)
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"state-machine-action01:20",
	}, r.actions.getList())

	// with different term
	ok = r.runner.StartStateMachine(testTerm01, info)
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"start-state-machine:21,running=true",
		"state-machine-action01:21",
	}, r.actions.getList())

	// with different info, stop
	ok = r.runner.StartStateMachine(testTerm01, paxos.StateMachineRunnerInfo{})
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{}, r.actions.getList())
	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, context.Canceled, r.ctxList[1].Err())

	// start again
	ok = r.runner.StartStateMachine(initTerm, info)
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"start-state-machine:20,running=true",
		"state-machine-action01:20",
	}, r.actions.getList())
}

func TestRunnerFake_StartElectionRunner(t *testing.T) {
	r := newRunnerFakeTest()

	// start
	info := paxos.ElectionRunnerInfo{
		Term:         initTerm,
		Started:      true,
		MaxTermValue: 22,
		Chosen:       nodeID4,
	}
	ok := r.runner.StartElectionRunner(info)
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"start-election:6404,term=22",
		"election-action01:22",
	}, r.actions.getList())

	// start same params
	ok = r.runner.StartElectionRunner(info)
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{}, r.actions.getList())

	// start different term value
	newInfo := info
	newInfo.MaxTermValue++
	ok = r.runner.StartElectionRunner(newInfo)
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"start-election:6404,term=23",
		"election-action01:23",
	}, r.actions.getList())
	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, nil, r.ctxList[1].Err())

	// stop
	ok = r.runner.StartElectionRunner(paxos.ElectionRunnerInfo{Term: initTerm})
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{}, r.actions.getList())
	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, context.Canceled, r.ctxList[1].Err())
}
