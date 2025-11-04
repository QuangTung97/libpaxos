package async

type Runtime interface {
	NewThread(callback func(ctx Context)) Context
	AddNext(ctx Context, callback func(ctx Context))
}

func NewSimulateRuntime() *SimulateRuntime {
	return &SimulateRuntime{}
}

type SimulateRuntime struct {
}

var _ Runtime = &SimulateRuntime{}

func (r *SimulateRuntime) NewThread(callback func(ctx Context)) Context {
	return nil
}

func (r *SimulateRuntime) AddNext(ctx Context, callback func(ctx Context)) {
}
