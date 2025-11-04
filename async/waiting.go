package async

type WaitStatus int

const (
	WaitStatusSuccess WaitStatus = iota + 1
	WaitStatusWaiting
)

type KeyWaiter[T comparable] interface {
	Run(ctx Context, key T, callback func(rt Runtime) (WaitStatus, error)) error
	Signal(key T)
	Broadcast()
}
