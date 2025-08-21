package paxos

type LogBuffer struct {
	lastCommitted *LogPos
}

func NewLogBuffer(lastCommitted *LogPos) *LogBuffer {
	return &LogBuffer{
		lastCommitted: lastCommitted,
	}
}

func (b *LogBuffer) Insert() {
}

func (b *LogBuffer) Size() int {
	return 0
}

func (b *LogBuffer) GetFrontPos() LogPos {
	return 0
}

func (b *LogBuffer) PopFront() {
}

func (b *LogBuffer) GetEntries(posList ...LogPos) []LogEntry {
	return nil
}
