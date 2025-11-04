package async

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRealContext(t *testing.T) {
	ctx := NewContext()

	realCtx := ctx.ToContext()
	assert.Equal(t, nil, realCtx.Err())

	ctx.Cancel()
	assert.Equal(t, context.Canceled, realCtx.Err())
}
