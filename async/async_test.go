package async

import (
	"sync"
	"testing"
)

func BenchmarkMutex(b *testing.B) {
	var mut sync.Mutex
	var x *int
	for range b.N {
		mut.Lock()
		x = new(int)
		mut.Unlock()
	}
	*x = 100
}
