package paxos

// ----------------------------------------------------------

type NodeID [16]byte

// ----------------------------------------------------------

type LogPos int64 // start from one (instead of zero)

type InfiniteLogPos struct {
	IsFinite bool
	Pos      LogPos
}

// ----------------------------------------------------------

type TermValue int64

type TermNum struct {
	Num    TermValue
	NodeID NodeID
}

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

// ----------------------------------------------------------

type LogEntry struct {
	Type LogType
	Term InfiniteTerm

	// Members valid only when Type = LogTypeMembership
	Members []MemberInfo

	// CmdData valid only when Type = LogTypeCmd
	CmdData []byte
}

type MemberInfo struct {
	Nodes      []NodeID
	ActiveFrom LogPos
}

type LogType int

const (
	LogTypeMembership LogType = iota + 1
	LogTypeCmd
	LogTypeNull
)
