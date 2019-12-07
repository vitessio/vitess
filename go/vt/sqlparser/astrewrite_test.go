package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	statement, err := Parse("select 1 + 2")
	assert.NoError(t, err)

	result := Walk2(statement, func(node SQLNode) (SQLNode, bool) {

		a, ok := node.(*BinaryExpr)
		if ok {
			tmp := a.Left
			a.Left = a.Right
			a.Right = tmp
			return a, true
		}

		return node, true
	})

	expected, err := Parse("select 2 + 1")
	assert.NoError(t, err)

	buf := NewTrackedBuffer(nil)
	result.Format(buf)
	fmt.Println(buf.String())
	assert.Equal(t, expected, result)
}
