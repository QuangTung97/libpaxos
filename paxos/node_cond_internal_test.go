package paxos

import "testing"

func TestNoCopy(t *testing.T) {
	var cond noCopy
	var x int
	cond.Lock()
	x++
	cond.Unlock()
}
