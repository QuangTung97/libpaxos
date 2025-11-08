package simulate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/async"
	"github.com/QuangTung97/libpaxos/paxos"
)

func (s *Simulation) getLeader() *NodeState {
	leaderCount := 0
	var current *NodeState
	for _, state := range s.stateMap {
		if state.core.GetState() == paxos.StateLeader {
			leaderCount++
			current = state
		}
	}

	if leaderCount != 1 {
		panic(fmt.Sprintf("number of leader is not 1, actual: %d", leaderCount))
	}

	return current
}

func TestPaxos_Simple_Three_Nodes(t *testing.T) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)

	runAllActions(s.runtime)
	state := s.getLeader()

	cmdSeq := s.runtime.NewSequence()
	s.runtime.NewThreadDetail("setup-cmd", func(ctx async.Context) {
		term := state.persistent.GetLastTerm()
		for i := range 20 {
			s.runtime.SequenceAddNextDetail(ctx, cmdSeq, "cmd::request", func(ctx async.Context) {
				_ = state.core.InsertCommand(ctx, term, []byte(fmt.Sprintf("cmd-test:%02d", i)))
			})
		}
	})

	runAllActions(s.runtime)

	// check logs
	assert.Equal(t, paxos.CommittedInfo{
		Members: []paxos.MemberInfo{
			{Nodes: []paxos.NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		},
		FullyReplicated: 21,
		PrevPointer: paxos.PreviousPointer{
			Pos: 21,
			Term: paxos.TermNum{
				Num:    21,
				NodeID: nodeID2,
			},
		},
	}, s.stateMap[nodeID1].log.GetCommittedInfo())
}
