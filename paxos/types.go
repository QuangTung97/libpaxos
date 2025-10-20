package paxos

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"slices"
)

// ----------------------------------------------------------

type NodeID [16]byte

func (n NodeID) String() string {
	return hex.EncodeToString(n[:])
}

// ----------------------------------------------------------

type LogPos int64 // start from one (instead of zero)

type InfiniteLogPos struct {
	IsFinite bool
	Pos      LogPos
}

type NullLogPos struct {
	Valid bool
	Pos   LogPos
}

// ----------------------------------------------------------

type TermValue int64

type TermNum struct {
	Num    TermValue
	NodeID NodeID
}

func (t TermNum) String() string {
	return fmt.Sprintf("%d:%s", t.Num, t.NodeID.String())
}

func (t TermNum) ToInf() InfiniteTerm {
	return InfiniteTerm{
		IsFinite: true,
		Term:     t,
	}
}

type InfiniteTerm struct {
	IsFinite bool
	Term     TermNum
}

func CompareTermNum(a, b TermNum) int {
	if a.Num != b.Num {
		return cmp.Compare(a.Num, b.Num)
	}
	return slices.Compare(a.NodeID[:], b.NodeID[:])
}

func CompareInfiniteTerm(a, b InfiniteTerm) int {
	if a.IsFinite != b.IsFinite {
		if !a.IsFinite {
			return 1
		} else {
			return -1
		}
	}
	return CompareTermNum(a.Term, b.Term)
}

// ----------------------------------------------------------

type PreviousPointer struct {
	Pos  LogPos
	Term TermNum
}

func (e LogEntry) NextPreviousPointer() PreviousPointer {
	return PreviousPointer{
		Pos:  e.Pos,
		Term: e.CreatedTerm,
	}
}

// ----------------------------------------------------------

type State int

const (
	StateFollower State = iota + 1
	StateCandidate
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ----------------------------------------------------------

func NewNullEntry(pos LogPos) LogEntry {
	return LogEntry{
		Pos:  pos,
		Type: LogTypeNull,
	}
}

type LogEntry struct {
	Pos  LogPos
	Type LogType
	Term InfiniteTerm

	// Members valid only when Type = LogTypeMembership
	Members []MemberInfo

	// CmdData valid only when Type = LogTypeCmd
	CmdData []byte

	CreatedTerm TermNum
	PrevPointer PreviousPointer
}

func (t LogType) WithPreviousPointer() bool {
	switch t {
	case LogTypeNull:
		return false
	case LogTypeNoOp:
		return false
	case LogTypeMembership:
		return false
	default:
		return true
	}
}

func ValidateCreatedTerm(entry LogEntry) {
	if !entry.Type.WithPreviousPointer() {
		return
	}
	AssertTrue(entry.CreatedTerm != TermNum{})
}

func (e LogEntry) IsNull() bool {
	return e.Type == LogTypeNull
}

func LogEntryEqual(a, b LogEntry) bool {
	if a.Pos != b.Pos {
		return false
	}

	if a.Type != b.Type {
		return false
	}
	if a.Term != b.Term {
		return false
	}

	if !slices.Equal(a.CmdData, b.CmdData) {
		return false
	}

	if !slices.EqualFunc(a.Members, b.Members, MemberInfoEqual) {
		return false
	}

	return true
}

type LogType int

const (
	LogTypeNull LogType = iota
	LogTypeMembership
	LogTypeCmd
	LogTypeNoOp
)

func NewNoOpLogEntry(pos LogPos) LogEntry {
	term := TermNum{
		Num: -1,
	}
	return LogEntry{
		Pos:  pos,
		Type: LogTypeNoOp,
		Term: term.ToInf(),
	}
}

func NewCmdLogEntry(pos LogPos, term InfiniteTerm, data []byte, createdTerm TermNum) LogEntry {
	return LogEntry{
		Pos:         pos,
		Type:        LogTypeCmd,
		Term:        term,
		CmdData:     data,
		CreatedTerm: createdTerm,
	}
}

func NewMembershipLogEntry(
	pos LogPos,
	term InfiniteTerm,
	members []MemberInfo,
) LogEntry {
	return LogEntry{
		Pos:     pos,
		Type:    LogTypeMembership,
		Term:    term,
		Members: members,
	}
}

// ----------------------------------------------------------

type MemberInfo struct {
	Nodes     []NodeID
	CreatedAt LogPos
}

func MemberInfoEqual(a, b MemberInfo) bool {
	if !slices.Equal(a.Nodes, b.Nodes) {
		return false
	}
	if a.CreatedAt != b.CreatedAt {
		return false
	}
	return true
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

func IsQuorum(members []MemberInfo, nodes map[NodeID]struct{}) bool {
	for _, info := range members {
		if !isQuorumOf(info.Nodes, nodes) {
			return false
		}
	}
	return true
}

func GetAllMembers(members []MemberInfo) map[NodeID]struct{} {
	resultSet := map[NodeID]struct{}{}
	for _, info := range members {
		for _, node := range info.Nodes {
			resultSet[node] = struct{}{}
		}
	}
	return resultSet
}

func IsNodeInMembers(members []MemberInfo, nodeID NodeID) bool {
	for _, conf := range members {
		for _, id := range conf.Nodes {
			if id == nodeID {
				return true
			}
		}
	}
	return false
}

// ----------------------------------------------------------

type TimestampMilli int64

// ----------------------------------------------------------

type ChooseLeaderInfo struct {
	NoActiveLeader  bool
	Members         []MemberInfo
	FullyReplicated LogPos
	LastTermVal     TermValue
}
