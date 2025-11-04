package async

type ThreadID int64

type Context interface {
	GetThreadID() ThreadID
	Cancel()
}
