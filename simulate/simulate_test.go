package simulate

import (
	"fmt"
	"slices"
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

	s.runtime.NewThread("setup-cmd", func(ctx async.Context) {
		term := state.persistent.GetLastTerm()

		for cmdIndex := range 20 {
			s.runtime.SeqAddNext(
				ctx, cmdSeq, "cmd::request",
				func(ctx async.Context, finishFunc func()) {
					nextCmd := []byte(fmt.Sprintf("cmd-test:%02d", cmdIndex))
					state.core.InsertCommandAsync(
						ctx, term, [][]byte{nextCmd},
						func(err error) {
							finishFunc()
						},
					)
				},
			)
		}
	})

	runAllActions(s.runtime)

	state1 := s.stateMap[nodeID1]
	state2 := s.stateMap[nodeID1]
	state3 := s.stateMap[nodeID1]

	// check committed info
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
	}, state1.log.GetCommittedInfo())

	// check log equal
	assert.Equal(t, true, slices.EqualFunc(
		state1.log.GetEntries(1, 100),
		state2.log.GetEntries(1, 100),
		paxos.LogEntryEqual,
	))
	assert.Equal(t, true, slices.EqualFunc(
		state2.log.GetEntries(1, 100),
		state3.log.GetEntries(1, 100),
		paxos.LogEntryEqual,
	))

	// check log values
	entries := state1.log.GetEntries(1, 100)
	assert.Equal(t, 21, len(entries))
	for _, entry := range entries {
		assert.Equal(t, false, entry.Term.IsFinite)
	}
	for index, entry := range entries[1:] {
		assert.Equal(t, paxos.LogTypeCmd, entry.Type)
		assert.Equal(t, fmt.Sprintf("cmd-test:%02d", index), string(entry.CmdData))
	}
}
