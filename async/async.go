package async

type Promise[T any] struct {
}

func (p Promise[T]) Get() T {
	var empty T
	return empty
}

type WaitStatus int

const (
	WaitStatusSuccess WaitStatus = iota + 1
	WaitStatusWaiting
)

func RunWithWait[T any](
	callback func() (T, WaitStatus, error),
) Promise[T] {
	return Promise[T]{}
}

func AndThen[A, B any](input Promise[A], handler func(input A) Promise[B]) {
}
