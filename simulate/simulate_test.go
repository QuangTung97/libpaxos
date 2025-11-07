package simulate

import (
	"testing"

	"github.com/QuangTung97/libpaxos/paxos"
)

func TestPaxos_Simple_Three_Nodes(t *testing.T) {
	s := NewSimulation(
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
		[]paxos.NodeID{nodeID1, nodeID2, nodeID3},
	)
	s.runtime.RunNext()

	s.printAllActions()
}
