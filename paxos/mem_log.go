package paxos

type MemLog struct {
	lastCommitted *LogPos

	queueData  []LogEntry
	frontIndex int
	queueLen   int
}

func NewMemLog(lastCommitted *LogPos, sizeLog int) *MemLog {
	return &MemLog{
		lastCommitted: lastCommitted,

		queueData:  make([]LogEntry, 1<<sizeLog),
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
		m.pushToQueue(LogEntry{})
	}

	index := m.getQueueIndex(memPos - 1)
	m.queueData[index] = entry
}

func (m *MemLog) Front() LogEntry {
	return m.Get(*m.lastCommitted + 1)
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

func (m *MemLog) pushToQueue(e LogEntry) {
	oldCap := len(m.queueData)
	if m.queueLen >= oldCap {
		newCap := oldCap << 1

		// extend the queue data
		newData := make([]LogEntry, newCap)

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
	memPos := int(pos - *m.lastCommitted)
	if memPos <= 0 {
		panic("Invalid log pos in mem log")
	}
	if memPos > m.queueLen {
		panic("Exceeded mem log size")
	}

	index := m.getQueueIndex(memPos - 1)
	return m.queueData[index]
}

func (m *MemLog) MaxLogPos() LogPos {
	return *m.lastCommitted + LogPos(m.queueLen)
}
