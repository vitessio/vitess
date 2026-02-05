package smartconnpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testPooled struct {
	Connection
}

func TestStackPop(t *testing.T) {
	s := &connStack[testPooled]{}

	first := &Pooled[testPooled]{}
	s.Push(first)

	second := &Pooled[testPooled]{}
	s.Push(second)

	c, ok := s.Pop()
	assert.True(t, ok)

	assert.Nil(t, c.next.Load())
}
