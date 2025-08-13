package paxos

type ReplicatedLog interface {
}

type LogEntry struct {
	Type LogType
	Term InfiniteTerm

	// Members valid only when Type = LogTypeMembership
	Members []MemberInfo
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
