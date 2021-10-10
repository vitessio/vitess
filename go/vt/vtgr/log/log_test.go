package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVTGRLogger(t *testing.T) {
	logger := NewVTGRLogger("ks", "0")
	s1 := logger.annotate("abc")
	assert.Equal(t, "shard=ks/0 abc", s1)
	s2 := fmt.Sprintf(logger.annotate("abc %s"), "def")
	assert.Equal(t, "shard=ks/0 abc def", s2)
}
