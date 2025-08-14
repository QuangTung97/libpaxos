package paxos

type MemLog struct {
	lastCommitted *LogPos

	queueData  []memLogEntry
	frontIndex int
	queueLen   int
}

type memLogEntry struct {
	voted map[NodeID]struct{}
	entry LogEntry
}

func NewMemLog(lastCommitted *LogPos, sizeLog int) *MemLog {
	return &MemLog{
		lastCommitted: lastCommitted,

		queueData:  make([]memLogEntry, 1<<sizeLog),
		frontIndex: 0,
		queueLen:   0,
	}
}

func (m *MemLog) Put(pos LogPos, entry LogEntry) {
	newLen := m.queueLen
	memPos := int(pos - *m.lastCommitted)

	if memPos > newLen {
		newLen = memPos
	}

	for m.queueLen < newLen {
		m.pushToQueue(memLogEntry{
			voted: map[NodeID]struct{}{},
		})
	}

	index := m.getQueueIndex(memPos - 1)
	m.queueData[index].entry = entry
}

func (m *MemLog) GetFrontVoted() (LogPos, map[NodeID]struct{}) {
	pos := *m.lastCommitted + 1
	entry := m.getByPos(pos)
	return pos, entry.voted
}

func (m *MemLog) PopFront() {
	mask := len(m.queueData) - 1
	m.frontIndex = (m.frontIndex + 1) & mask
	*m.lastCommitted++
}

func (m *MemLog) getQueueIndex(offset int) int {
	mask := len(m.queueData) - 1
	return (m.frontIndex + offset) & mask
}

func (m *MemLog) pushToQueue(e memLogEntry) {
	oldCap := len(m.queueData)
	if m.queueLen >= oldCap {
		newCap := oldCap << 1

		// extend the queue data
		newData := make([]memLogEntry, newCap)

		remain := oldCap - m.frontIndex
		copy(newData, m.queueData[m.frontIndex:])
		copy(newData[remain:], m.queueData[:m.frontIndex])

		m.frontIndex = 0
		m.queueData = newData
	}

	index := m.getQueueIndex(m.queueLen)
	m.queueData[index] = e
	m.queueLen++
}

func (m *MemLog) Get(pos LogPos) LogEntry {
	return m.getByPos(pos).entry
}

func (m *MemLog) GetVoted(pos LogPos) map[NodeID]struct{} {
	return m.getByPos(pos).voted
}

func (m *MemLog) getByPos(pos LogPos) memLogEntry {
	memPos := int(pos - *m.lastCommitted)
	if memPos <= 0 {
		panic("Invalid log pos in mem log")
	}
	if memPos > m.queueLen {
		return memLogEntry{}
	}

	index := m.getQueueIndex(memPos - 1)
	return m.queueData[index]
}

func (m *MemLog) AddVoted(pos LogPos, nodeID NodeID) {
	entry := m.getByPos(pos)
	entry.voted[nodeID] = struct{}{}
}

func (m *MemLog) MaxLogPos() LogPos {
	return *m.lastCommitted + LogPos(m.queueLen)
}

func (m *MemLog) GetQueueSize() int {
	return m.queueLen
}
