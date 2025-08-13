package paxos_test

import (
	"testing"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type coreLogicTest struct {
	persistent *fake.PersistentStateFake
	log        *fake.LogStorageFake
	runner     *fake.NodeRunnerFake

	core CoreLogic
}

func newCoreLogicTest() *coreLogicTest {
	c := &coreLogicTest{}

	c.persistent = &fake.PersistentStateFake{
		NodeID:    fake.NewNodeID(1),
		LastValue: 20,
	}
	c.log = &fake.LogStorageFake{}
	c.runner = &fake.NodeRunnerFake{}

	c.core = NewCoreLogic(
		c.persistent,
		c.log,
		c.runner,
	)
	return c
}

func TestCoreLogic_StartElection(t *testing.T) {
	c := newCoreLogicTest()

	c.core.StartElection()
}
