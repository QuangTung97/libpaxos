package paxos

type LogBuffer struct {
	lastCommitted *LogPos
	startIndex    bufferRealIndex
	queueSize     int
	queueData     []LogEntry
}

type bufferRealIndex int

func NewLogBuffer(lastCommitted *LogPos, sizeLog int) *LogBuffer {
	return &LogBuffer{
		lastCommitted: lastCommitted,

		startIndex: 0,
		queueSize:  0,
		queueData:  make([]LogEntry, 1<<sizeLog),
	}
}

func (b *LogBuffer) computeRealIndex(queueIndex int) bufferRealIndex {
	mask := bufferRealIndex(len(b.queueData) - 1)
	return (b.startIndex + bufferRealIndex(queueIndex)) & mask
}

func (b *LogBuffer) Insert(entry LogEntry) {
	ValidateCreatedTerm(entry)

	if b.queueSize >= len(b.queueData) {
		oldCap := bufferRealIndex(len(b.queueData))
		newCap := oldCap << 1

		newData := make([]LogEntry, newCap)
		rightPart := oldCap - b.startIndex

		copy(newData, b.queueData[b.startIndex:])
		copy(newData[rightPart:], b.queueData[:b.startIndex])

		b.queueData = newData
		b.startIndex = 0
	}

	appendIndex := b.queueSize
	b.queueSize++
	realIndex := b.computeRealIndex(appendIndex)
	b.queueData[realIndex] = entry
}

func (b *LogBuffer) Size() int {
	return b.queueSize
}

func (b *LogBuffer) GetFrontPos() LogPos {
	return *b.lastCommitted - LogPos(b.queueSize-1)
}

func (b *LogBuffer) PopFront() {
	if b.queueSize <= 0 {
		panic("queue size must not be empty")
	}

	realIndex := b.computeRealIndex(0)
	b.queueData[realIndex] = LogEntry{}
	b.startIndex++
	b.queueSize--
}

func (b *LogBuffer) GetEntries(posList ...LogPos) []LogEntry {
	result := make([]LogEntry, 0, len(posList))

	for _, pos := range posList {
		queueIndex := int(pos + LogPos(b.queueSize-1) - *b.lastCommitted)
		if queueIndex < 0 || queueIndex >= b.queueSize {
			result = append(result, NewNullEntry(pos))
		} else {
			realIndex := b.computeRealIndex(queueIndex)
			result = append(result, b.queueData[realIndex])
		}
	}

	return result
}
