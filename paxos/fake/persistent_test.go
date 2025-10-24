package fake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/libpaxos/paxos"
)

func TestPersistentStateFake(t *testing.T) {
	s := &PersistentStateFake{
		NodeID:   nodeID3,
		LastTerm: testCreatedTerm,
	}

	s.UpdateLastTerm(paxos.TermNum{
		Num:    21,
		NodeID: nodeID1,
	})

	assert.Equal(t, paxos.TermNum{
		Num:    21,
		NodeID: nodeID1,
	}, s.GetLastTerm())

	assert.Equal(t, nodeID3, s.GetNodeID())
}
