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

type LogType int

const (
	LogTypeNull LogType = iota
	LogTypeMembership
	LogTypeCmd
	LogTypeNoOp
)

// ----------------------------------------------------------

type MemberInfo struct {
	Nodes      []NodeID
	ActiveFrom LogPos
}

func isQuorumOf(universe []NodeID, checkSet map[NodeID]struct{}) bool {
	factor := len(universe)/2 + 1

	universeSet := map[NodeID]struct{}{}
	for _, id := range universe {
		universeSet[id] = struct{}{}
	}

	numElems := 0
	for id := range checkSet {
		_, ok := universeSet[id]
		if !ok {
			continue
		}
		numElems++
	}

	return numElems >= factor
}

func IsQuorum(members []MemberInfo, nodes map[NodeID]struct{}, pos LogPos) bool {
	for _, info := range members {
		if pos < info.ActiveFrom {
			continue
		}
		if !isQuorumOf(info.Nodes, nodes) {
			return false
		}
	}
	return true
}

func GetAllMembers(members []MemberInfo, pos LogPos) map[NodeID]struct{} {
	resultSet := map[NodeID]struct{}{}
	for _, info := range members {
		if pos < info.ActiveFrom {
			continue
		}

		for _, node := range info.Nodes {
			resultSet[node] = struct{}{}
		}
	}

	return resultSet
}
