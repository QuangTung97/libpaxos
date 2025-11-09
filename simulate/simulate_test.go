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

	if leaderCount == 1 {
		return current
	}
	panic(fmt.Sprintf("number of leader is not 1, actual: %d", leaderCount))
}

func (s *Simulation) assertLogMatch(t *testing.T, nodeIDList ...paxos.NodeID) int {
	firstID := nodeIDList[0]
	firstLog := s.stateMap[firstID].log.GetEntries(1, 10_000)

	// check all entries is committed
	for _, entry := range firstLog {
		assert.Equal(t, false, entry.Term.IsFinite)
	}

	// check disk log
	for _, id := range nodeIDList[1:] {
		checkedLog := s.stateMap[id].log.GetEntries(1, 10_000)
		assert.Equal(t, true, slices.EqualFunc(firstLog, checkedLog, paxos.LogEntryEqual))
	}

	// check state machine log
	for _, id := range nodeIDList {
		checkedLog := s.stateMap[id].stateLog
		assert.Equal(t, true, slices.EqualFunc(firstLog, checkedLog, paxos.LogEntryEqual))
	}

	return len(firstLog)
}

func TestPaxos_Simple_Three_Nodes__Always_Elect_One_Leader(t *testing.T) {
	totalActions := 0
	for range 1000 {
		doTestPaxosSingleThreeNodesElectALeader(t, &totalActions)
		if t.Failed() {
			return
		}
	}
	fmt.Println("TOTAL ACTIONS:", totalActions)
}

func doTestPaxosSingleThreeNodesElectALeader(t *testing.T, totalActions *int) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)

	s.runRandomAllActions()

	leaderState := s.getLeader()
	assert.Equal(t, paxos.StateLeader, leaderState.core.GetState())

	for id, state := range s.stateMap {
		if id != leaderState.currentID {
			assert.Equal(t, paxos.StateFollower, state.core.GetState())
		}
	}

	*totalActions += s.numTotalActions
}

func TestPaxos_Simple_Three_Nodes__Replicate_Cmd(t *testing.T) {
	totalActions := 0
	for range 1000 {
		doTestPaxosSingleThreeNodesReplicateCmd(t, &totalActions)
		if t.Failed() {
			return
		}
	}
	fmt.Println("TOTAL ACTIONS:", totalActions)
}

func doTestPaxosSingleThreeNodesReplicateCmd(t *testing.T, totalActions *int) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)
	runAllActions(s.runtime)

	leaderState := s.getLeader()
	cmdSeq := s.runtime.NewSequence()

	const maxCmd = 12

	s.runtime.NewThread("setup-cmd", func(ctx async.Context) {
		term := leaderState.persistent.GetLastTerm()

		for cmdIndex := range maxCmd {
			s.runtime.SeqAddNext(
				ctx, cmdSeq, "cmd::request",
				func(ctx async.Context, finishFunc func()) {
					nextCmd := []byte(fmt.Sprintf("cmd-test:%02d", cmdIndex))
					leaderState.core.InsertCommandAsync(
						ctx, term, [][]byte{nextCmd},
						func(err error) {
							finishFunc()
						},
					)
				},
			)
		}
	})

	// do insert commands
	s.runRandomAllActions()
	assert.Equal(t, paxos.LogPos(maxCmd+1), leaderState.log.GetFullyReplicated())

	// check committed info
	state1 := s.stateMap[nodeID1]
	assert.Equal(t, paxos.CommittedInfo{
		Members: []paxos.MemberInfo{
			{Nodes: []paxos.NodeID{nodeID1, nodeID2, nodeID3}, CreatedAt: 1},
		},
		FullyReplicated: maxCmd + 1,
		PrevPointer: paxos.PreviousPointer{
			Pos: maxCmd + 1,
			Term: paxos.TermNum{
				Num:    21,
				NodeID: leaderState.currentID,
			},
		},
	}, state1.log.GetCommittedInfo())

	// check log match
	s.assertLogMatch(t, nodeID1, nodeID2, nodeID3)

	// check log values
	entries := state1.log.GetEntries(1, 100)
	assert.Equal(t, maxCmd+1, len(entries))
	for _, entry := range entries {
		assert.Equal(t, false, entry.Term.IsFinite)
	}
	for index, entry := range entries[1:] {
		assert.Equal(t, paxos.LogTypeCmd, entry.Type)
		assert.Equal(t, fmt.Sprintf("cmd-test:%02d", index), string(entry.CmdData))
	}

	*totalActions += s.numTotalActions
}

func TestPaxos_With_Membership_Changes(t *testing.T) {
	totalActions := 0
	for range 1000 {
		doTestPaxosWithMembershipChanges(t, &totalActions)
		if t.Failed() {
			return
		}
	}
	fmt.Println("TOTAL ACTIONS:", totalActions)
}

func (s *Simulation) getRandomLeader() (*NodeState, bool) {
	var possibleStates []*NodeState
	for _, state := range s.stateMap {
		if state.core.GetState() == paxos.StateLeader {
			possibleStates = append(possibleStates, state)
		}
	}

	if possibleStates == nil {
		return nil, false
	}

	index := s.randObj.Intn(len(possibleStates))
	return possibleStates[index], true
}

func (s *Simulation) doInsertCommands() {
	const maxCmd = 12
	s.runtime.NewThread("setup-cmd", func(ctx async.Context) {
		cmdSeq := s.runtime.NewSequence()
		for cmdIndex := range maxCmd {
			s.runtime.SeqAddNext(
				ctx, cmdSeq, "cmd::request",
				func(ctx async.Context, finishFunc func()) {
					leaderState, ok := s.getRandomLeader()
					if !ok {
						finishFunc()
						return
					}

					term := leaderState.persistent.GetLastTerm()

					nextCmd := []byte(fmt.Sprintf("cmd-test:%02d", cmdIndex))

					leaderState.core.InsertCommandAsync(
						ctx, term, [][]byte{nextCmd},
						func(err error) {
							finishFunc()
						},
					)
				},
			)
		}
	})
}
func (s *Simulation) chooseNewRandomMembers() []paxos.NodeID {
	tmpNodes := slices.Clone(s.allNodes)
	s.randObj.Shuffle(len(tmpNodes), func(i, j int) {
		tmpNodes[i], tmpNodes[j] = tmpNodes[j], tmpNodes[i]
	})

	maxNodes := min(len(tmpNodes), 5)
	newSize := 1 + s.randObj.Intn(maxNodes)

	result := slices.Clone(tmpNodes[:newSize])
	slices.SortFunc(result, paxos.CompareNodeID)
	return result
}

func (s *Simulation) doChangeMembershipMultiTimes(maxNumTimes int) {
	s.runtime.NewThread("setup-member-update", func(ctx async.Context) {
		changeSeq := s.runtime.NewSequence()

		for range maxNumTimes {
			s.runtime.SeqAddNext(
				ctx, changeSeq, "cmd::update-members",
				func(ctx async.Context, finishFunc func()) {
					leaderState, ok := s.getRandomLeader()
					if !ok {
						finishFunc()
						return
					}

					term := leaderState.persistent.GetLastTerm()
					newMembers := s.chooseNewRandomMembers()

					leaderState.core.ChangeMembershipAsync(
						ctx, term, newMembers,
						func(err error) {
							finishFunc()
						},
					)
				},
			)
		}
	})
}

func (s *Simulation) findLastLeader() *NodeState {
	var maxTerm paxos.TermNum
	var result *NodeState

	for _, state := range s.stateMap {
		if state.core.GetState() != paxos.StateLeader {
			continue
		}

		term := state.persistent.GetLastTerm()
		if paxos.CompareTermNum(term, maxTerm) > 0 {
			term = maxTerm
			result = state
		}
	}

	return result
}

func doTestPaxosWithMembershipChanges(t *testing.T, totalActions *int) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3, nodeID4, nodeID5, nodeID6},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)
	s.runRandomAllActions()

	// setup random actions
	s.doInsertCommands()
	s.doChangeMembershipMultiTimes(4)

	// do execute random actions
	s.runRandomAllActions()

	lastLeader := s.findLastLeader()
	committed := lastLeader.log.GetCommittedInfo()
	assert.Equal(t, 1, len(committed.Members))
	assert.Equal(t, paxos.LogPos(1), committed.Members[0].CreatedAt)

	memberNodes := committed.Members[0].Nodes
	s.assertLogMatch(t, memberNodes...)

	*totalActions += s.numTotalActions
}

func TestPaxos_With_Membership_Changes__And_Timeout(t *testing.T) {
	totalActions := 0
	for range 1000 {
		doTestPaxosWithMembershipChangesAndTimeout(t, &totalActions)
		if t.Failed() {
			return
		}
	}
	fmt.Println("TOTAL ACTIONS:", totalActions)
}

func doTestPaxosWithMembershipChangesAndTimeout(t *testing.T, totalActions *int) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3, nodeID4, nodeID5, nodeID6},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)
	s.runRandomAllActions()

	// setup random actions
	s.doInsertCommands()
	s.doChangeMembershipMultiTimes(4)
	s.doTimeoutMultiTimes(10)

	// do execute random actions
	s.runRandomAllActions()

	lastLeader := s.findLastLeader()
	committed := lastLeader.log.GetCommittedInfo()
	assert.Equal(t, 1, len(committed.Members))
	assert.Equal(t, paxos.LogPos(1), committed.Members[0].CreatedAt)

	memberNodes := committed.Members[0].Nodes
	s.assertLogMatch(t, memberNodes...)

	*totalActions += s.numTotalActions
}

func (s *Simulation) doTimeoutMultiTimes(maxNumTimes int) {
	s.runtime.NewThread("setup-timeout", func(ctx async.Context) {
		seqNum := s.runtime.NewSequence()

		for range maxNumTimes {
			s.runtime.SeqAddNext(
				ctx, seqNum, "system::timeout",
				func(ctx async.Context, finishFunc func()) {
					index := s.randObj.Intn(len(s.allNodes))
					nodeID := s.allNodes[index]
					s.now += 11_000
					s.stateMap[nodeID].core.CheckTimeout()
				},
			)
		}
	})
}
