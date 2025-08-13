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
	LogTypeMembership LogType = iota + 1
	LogTypeCmd
	LogTypeNull
)

// ----------------------------------------------------------

type MemberInfo struct {
	Nodes      []NodeID
	ActiveFrom LogPos
}

func GetAllMembers(members []MemberInfo, pos LogPos) []NodeID {
	var result []NodeID
	resultSet := map[NodeID]struct{}{}

	addNode := func(id NodeID) {
		_, existed := resultSet[id]
		if existed {
			return
		}
		resultSet[id] = struct{}{}
		result = append(result, id)
	}

	for _, info := range members {
		if pos < info.ActiveFrom {
			continue
		}

		for _, node := range info.Nodes {
			addNode(node)
		}
	}

	return result
}
