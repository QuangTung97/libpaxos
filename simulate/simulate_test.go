package simulate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/paxos"
)

func TestPaxos_Simple_Three_Nodes(t *testing.T) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)

	s.runtime.RunNext()
	s.runtime.RunNext()
	s.runActionIndex(10)
	s.runActionIndex(10)
	s.runActionIndex(10)
	s.runActionIndex(10)

	s.runActionIndex(10)
	s.runActionIndex(16)
	s.runActionIndex(16)
	s.runActionIndex(16)

	s.runActionIndex(11)
	s.runActionIndex(15)
	s.runActionIndex(15)
	s.runActionIndex(15)

	s.runtime.RemoveAllCanceled()

	s.runActionIndex(5)
	s.runActionIndex(8)
	s.runtime.RemoveAllCanceled()
	s.runActionIndex(6)
	s.runActionIndex(6)
	s.runActionIndex(4)

	runAllActions(s.runtime)

	assert.Equal(t, paxos.StateFollower, s.stateMap[nodeID1].core.GetState())
	assert.Equal(t, paxos.StateLeader, s.stateMap[nodeID2].core.GetState())
	assert.Equal(t, paxos.StateFollower, s.stateMap[nodeID3].core.GetState())

	s.printAllActions()
}
