/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"fmt"
	"testing"
)

func readable(node Expr) string {
	switch node := node.(type) {
	case *OrExpr:
		return fmt.Sprintf("(%s or %s)", readable(node.Left), readable(node.Right))
	case *XorExpr:
		return fmt.Sprintf("(%s xor %s)", readable(node.Left), readable(node.Right))
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

func TestLogicOperatorPrecedence(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select * from a where a=b and c=d or e=f",
		output: "((a = b and c = d) or e = f)",
	}, {
		input:  "select * from a where a=b or c=d and e=f",
		output: "(a = b or (c = d and e = f))",
	}, {
		input:  "select * from a where f=g xor a=b and e=f",
		output: "(f = g xor (a = b and e = f))",
	}, {
		input:  "select * from a where f=g xor a=b or c=d and e=f",
		output: "((f = g xor a = b) or (c = d and e = f))",
	}, {
		input:  "select * from t where a = 5 or b xor d = 3 and c is not null",
		output: "(a = 5 or (b xor (d = 3 and (c is not null))))",
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
		expr := readable(tree.(*Select).SelectExprs[0].(*AliasedExpr).Expr)
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

func fmtSetOp(s SelectStatement) string {
	switch s := s.(type) {
	case *SetOp:
		return fmt.Sprintf("(%s %s %s)", fmtSetOp(s.Left), s.Type, fmtSetOp(s.Right))
	case *Select:
		return String(s)
	case *ParenSelect:
		return String(s)
	}
	return ""
}

func TestSetOperatorPrecedence(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		{
			input:  "select 1 union select 2 union select 3 union select 4",
			output: "(((select 1 union select 2) union select 3) union select 4)",
		},
		{
			input:  "select 1 intersect select 2 intersect select 3 intersect select 4",
			output: "(((select 1 intersect select 2) intersect select 3) intersect select 4)",
		},
		{
			input:  "select 1 except select 2 except select 3 except select 4",
			output: "(((select 1 except select 2) except select 3) except select 4)",
		},

		{
			input:  "select 1 union select 2 intersect select 3 except select 4",
			output: "((select 1 union (select 2 intersect select 3)) except select 4)",
		},
		{
			input:  "select 1 union select 2 except select 3 intersect select 4",
			output: "((select 1 union select 2) except (select 3 intersect select 4))",
		},

		{
			input:  "select 1 intersect select 2 union select 3 except select 4",
			output: "(((select 1 intersect select 2) union select 3) except select 4)",
		},
		{
			input:  "select 1 intersect select 2 except select 3 union select 4",
			output: "(((select 1 intersect select 2) except select 3) union select 4)",
		},

		{
			input:  "select 1 except select 2 intersect select 3 union select 4",
			output: "((select 1 except (select 2 intersect select 3)) union select 4)",
		},
		{
			input:  "select 1 except select 2 union select 3 intersect select 4",
			output: "((select 1 except select 2) union (select 3 intersect select 4))",
		},

		{
			input:  "(table a) union (table b)",
			output: "((select * from a) union (select * from b))",
		},
		{
			input:  "(table a) intersect (table b)",
			output: "((select * from a) intersect (select * from b))",
		},
		{
			input:  "(table a) except (table b)",
			output: "((select * from a) except (select * from b))",
		},
		{
			input:  "select 1 intersect (select 2 union select 3)",
			output: "(select 1 intersect (select 2 union select 3))",
		},

	}
	for _, tcase := range validSQL {
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Error(err)
			continue
		}
		expr := fmtSetOp(tree.(SelectStatement))
		if expr != tcase.output {
			t.Errorf("Parse: \n%s, want: \n%s", expr, tcase.output)
		}
	}
}