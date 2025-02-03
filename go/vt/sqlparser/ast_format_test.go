package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColNamesInSlice(t *testing.T) {
	c1 := NewColName("a1")
	c2 := NewColName("a2")
	exprs := []Expr{
		c1,
		c2,
	}

	String(c1)

	result := SliceString(exprs)
	assert.Equal(t, "a1, a2", result)
}
