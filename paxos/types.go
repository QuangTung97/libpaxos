package paxos

// ----------------------------------------------------------

type NodeID [16]byte

// ----------------------------------------------------------

type LogPos int64 // start from 1

type InfiniteLogPos struct {
	IsFinite bool
	Pos      LogPos
}

// ----------------------------------------------------------

type TermNum int64

type InfiniteTerm struct {
	IsFinite bool
	Term     TermNum
}

// ----------------------------------------------------------

type State int

const (
	StateFollower State = iota + 1
	StateCandidate
	StateLeader
)
