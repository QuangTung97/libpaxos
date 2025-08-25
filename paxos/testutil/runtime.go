package testutil

import (
	"unsafe"
)

// IsAssociated reports whether p is associated with the current bubble.
func IsAssociated[T any](p *T) bool {
	return isAssociated(unsafe.Pointer(p))
}

//go:linkname isAssociated internal/synctest.isAssociated
func isAssociated(p unsafe.Pointer) bool
