package simulate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
)

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
	ctx async.Context, nodeID paxos.NodeID, term paxos.TermNum, retryCount int,
) {
	r.ctxList = append(r.ctxList, ctx)
	r.actions.add("start-fetch-follower:%s,%d,retry=%d", nodeID.String()[:4], term.Num, retryCount)
	r.rt.AddNext(ctx, func(ctx async.Context) {
		r.actions.add("fetch-follower-action01:%s,%d", nodeID.String()[:4], term.Num)
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

	ok := r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2), 1)
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-fetch-follower:6401,20,retry=1",
		"start-fetch-follower:6402,20,retry=1",
	}, r.actions.getList())

	// run again with same set of values
	ok = r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2), 1)
	assert.Equal(t, false, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"fetch-follower-action01:6401,20",
		"fetch-follower-action01:6402,20",
	}, r.actions.getList())

	// run again with empty nodes
	ok = r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(), 1)
	assert.Equal(t, true, ok)

	assert.Equal(t, 2, len(r.ctxList))
	assert.Equal(t, context.Canceled, r.ctxList[0].Err())
	assert.Equal(t, context.Canceled, r.ctxList[1].Err())
}

func TestRunnerFake_FetchingFollowerInfoRunners__Increase_Retry(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2), 1)
	assert.Equal(t, true, ok)
	assert.Equal(t, []string{}, r.actions.getList())

	r.rt.RunNext()
	r.rt.RunNext()
	assert.Equal(t, []string{
		"start-fetch-follower:6401,20,retry=1",
		"start-fetch-follower:6402,20,retry=1",
	}, r.actions.getList())

	// run again with retry = 2
	ok = r.runner.StartFetchingFollowerInfoRunners(initTerm, newNodeSet(nodeID1, nodeID2), 2)
	assert.Equal(t, true, ok)
	runAllActions(r.rt)
	assert.Equal(t, []string{
		"fetch-follower-action01:6401,20",
		"fetch-follower-action01:6402,20",
		"start-fetch-follower:6401,20,retry=2",
		"start-fetch-follower:6402,20,retry=2",
		"fetch-follower-action01:6401,20",
		"fetch-follower-action01:6402,20",
	}, r.actions.getList())
}

func TestRunnerFake_StartStateMachine(t *testing.T) {
	r := newRunnerFakeTest()

	ok := r.runner.StartStateMachine(initTerm, paxos.StateMachineRunnerInfo{
		Running:       true,
		IsLeader:      true,
		AcceptCommand: true,
	})
	// TODO
	assert.Equal(t, false, ok)
}
