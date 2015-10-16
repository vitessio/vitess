// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"fmt"
	"testing"
)

func readable(node Expr) string {
	switch node := node.(type) {
	case *OrExpr:
		return fmt.Sprintf("(%s or %s)", readable(node.Left), readable(node.Right))
	case *AndExpr:
		return fmt.Sprintf("(%s and %s)", readable(node.Left), readable(node.Right))
	case *BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", readable(node.Left), node.Operator, readable(node.Right))
	case *IsExpr:
		return fmt.Sprintf("(%s %s)", readable(node.Expr), node.Operator)
	default:
		return String(node)
	}
}

func TestAndOrPrecedence(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select * from a where a=b and c=d or e=f",
		output: "((a = b and c = d) or e = f)",
	}, {
		input:  "select * from a where a=b or c=d and e=f",
		output: "(a = b or (c = d and e = f))",
	}}
	for _, tcase := range validSQL {
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Error(err)
			continue
		}
		expr := readable(tree.(*Select).Where.Expr)
		if expr != tcase.output {
			t.Errorf("Parse: \n%s, want: \n%s", expr, tcase.output)
		}
	}
}

func TestPlusStarPrecedence(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select 1+2*3 from a",
		output: "(1 + (2 * 3))",
	}, {
		input:  "select 1*2+3 from a",
		output: "((1 * 2) + 3)",
	}}
	for _, tcase := range validSQL {
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Error(err)
			continue
		}
		expr := readable(tree.(*Select).SelectExprs[0].(*NonStarExpr).Expr)
		if expr != tcase.output {
			t.Errorf("Parse: \n%s, want: \n%s", expr, tcase.output)
		}
	}
}

func TestIsPrecedence(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select * from a where a+b is true",
		output: "((a + b) is true)",
	}, {
		input:  "select * from a where a=1 and b=2 is true",
		output: "(a = 1 and (b = 2 is true))",
	}, {
		input:  "select * from a where (a=1 and b=2) is true",
		output: "((a = 1 and b = 2) is true)",
	}}
	for _, tcase := range validSQL {
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Error(err)
			continue
		}
		expr := readable(tree.(*Select).Where.Expr)
		if expr != tcase.output {
			t.Errorf("Parse: \n%s, want: \n%s", expr, tcase.output)
		}
	}
}
