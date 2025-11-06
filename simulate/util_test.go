package simulate

import (
	"fmt"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

var (
	nodeID1 = fake.NewNodeID(1)
	nodeID2 = fake.NewNodeID(2)
	nodeID3 = fake.NewNodeID(3)
	nodeID4 = fake.NewNodeID(4)
	nodeID5 = fake.NewNodeID(5)
	nodeID6 = fake.NewNodeID(6)

	initTerm = paxos.TermNum{
		Num:    20,
		NodeID: nodeID3,
	}
)

type actionListTest struct {
	actions []string
}

func newActionListTest() *actionListTest {
	return &actionListTest{
		actions: make([]string, 0),
	}
}

func (a *actionListTest) add(format string, args ...any) {
	a.actions = append(a.actions, fmt.Sprintf(format, args...))
}

func (a *actionListTest) getList() []string {
	result := a.actions
	a.actions = make([]string, 0)
	return result
}

func runAllActions(rt *async.SimulateRuntime) {
	for rt.RunNext() {
	}
}
