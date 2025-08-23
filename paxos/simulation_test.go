package paxos_test

import (
	"sync/atomic"
	"testing"
	"testing/synctest"

	. "github.com/QuangTung97/libpaxos/paxos"
	"github.com/QuangTung97/libpaxos/paxos/fake"
)

type simulationTestCase struct {
	now     atomic.Int64
	nodeMap map[NodeID]*nodeState
}

type nodeState struct {
	core       CoreLogic
	acceptor   AcceptorLogic
	log        *fake.LogStorageFake
	persistent *fake.PersistentStateFake

	runner       NodeRunner
	runnerFinish func()
}

type simulationTestConfig struct {
	maxBufferLen int
}

func defaultSimulationConfig() simulationTestConfig {
	return simulationTestConfig{
		maxBufferLen: 5,
	}
}

func newSimulationTestCase(
	t *testing.T,
	allNodes []NodeID,
	initNodes []NodeID,
	conf simulationTestConfig,
) *simulationTestCase {
	s := &simulationTestCase{}
	s.now.Store(10_000)

	initNodeSet := map[NodeID]struct{}{}
	for _, id := range initNodes {
		initNodeSet[id] = struct{}{}
	}

	initTerm := TermNum{
		Num:    19,
		NodeID: nodeID1,
	}

	initMembersEntry := NewMembershipLogEntry(
		initTerm.ToInf(),
		[]MemberInfo{
			{CreatedAt: 1, Nodes: initNodes},
		},
	)

	nodeMap := map[NodeID]*nodeState{}
	for _, id := range allNodes {
		persistent := &fake.PersistentStateFake{
			NodeID: id,
			LastTerm: TermNum{
				Num:    20,
				NodeID: id,
			},
		}

		log := &fake.LogStorageFake{}

		_, ok := initNodeSet[id]
		if ok {
			log.UpsertEntries([]PosLogEntry{
				{Pos: 1, Entry: initMembersEntry},
			}, nil)
		}

		runner, finish := s.newRunnerForNode(id)

		core := NewCoreLogic(
			persistent,
			log,
			runner,
			func() TimestampMilli {
				return TimestampMilli(s.now.Load())
			},
			LogPos(conf.maxBufferLen),
		)

		nodeMap[id] = &nodeState{
			persistent: persistent,
			log:        log,
			core:       core,

			runner:       runner,
			runnerFinish: finish,
		}
	}

	s.nodeMap = nodeMap

	return s
}

func (s *simulationTestCase) newRunnerForNode(id NodeID) (NodeRunner, func()) {
	return NewNodeRunner(
		id,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
}

func TestPaxos__Single_Node(t *testing.T) {
	t.Skip()
	synctest.Test(t, func(t *testing.T) {
		newSimulationTestCase(
			t,
			[]NodeID{nodeID1},
			[]NodeID{nodeID1},
			defaultSimulationConfig(),
		)
	})
}
