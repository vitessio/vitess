package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEquals(t *testing.T) {
	for idxA, objA := range createObjs() {
		for idxB, objB := range createObjs() {
			t.Run(fmt.Sprintf("%s == %s", name(objA), name(objB)), func(t *testing.T) {
				if idxA == idxB {
					require.True(t, EqualsAST(objA, objB))
				} else {
					require.False(t, EqualsAST(objA, objB))
				}
			})
		}
	}
}

func createObjs() []AST {
	t := true
	return []AST{
		nil,
		&Leaf{1},
		&Leaf{2},
		&RefContainer{ASTType: &Leaf{1}, ASTImplementationType: &Leaf{2}},
		ValueContainer{ASTType: ValueContainer{ASTType: &Leaf{1}, ASTImplementationType: &Leaf{2}}},
		&RefSliceContainer{ASTElements: []AST{&Leaf{1}, &Leaf{2}}, ASTImplementationElements: []*Leaf{{3}, {4}}},
		ValueSliceContainer{ASTElements: []AST{&Leaf{1}, &Leaf{2}}, ASTImplementationElements: []*Leaf{{3}, {4}}},
		InterfaceSlice{
			&RefContainer{
				ASTType:               &RefContainer{NotASTType: 12},
				ASTImplementationType: &Leaf{2},
			},
			&Leaf{2},
			&Leaf{3},
		},
		&SubImpl{
			inner: &SubImpl{},
			field: &t,
		},
	}
}

func name(a AST) string {
	if a == nil {
		return "nil"
	}
	return a.String()
}
