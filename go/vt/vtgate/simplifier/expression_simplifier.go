/*
Copyright 2021 The Vitess Authors.

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

package simplifier

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// CheckF is used to see if the given expression exhibits the sought after issue
type CheckF = func(sqlparser.Expr) bool

func SimplifyExpr(in sqlparser.Expr, test CheckF) sqlparser.Expr {
	// since we can't rewrite the top level, wrap the expr in an Exprs object
	smallestKnown := sqlparser.Exprs{sqlparser.CloneExpr(in)}

	alwaysVisit := func(node, parent sqlparser.SQLNode) bool {
		return true
	}

	up := func(cursor *sqlparser.Cursor) bool {
		node := sqlparser.CloneSQLNode(cursor.Node())
		s := &shrinker{orig: node}
		expr := s.Next()
		for expr != nil {
			cursor.Replace(expr)

			valid := test(smallestKnown[0])
			if valid {
				break // we will still continue trying to simplify other expressions at this level
			} else {
				log.Errorf("failed attempt: tried changing {%s} to {%s} in {%s}", sqlparser.String(node), sqlparser.String(expr), sqlparser.String(in))
				// undo the change
				cursor.Replace(node)
			}
			expr = s.Next()
		}
		return true
	}

	// loop until rewriting introduces no more changes
	for {
		prevSmallest := sqlparser.CloneExprs(smallestKnown)
		sqlparser.SafeRewrite(smallestKnown, alwaysVisit, up)
		if sqlparser.Equals.Exprs(prevSmallest, smallestKnown) {
			break
		}
	}

	return smallestKnown[0]
}

type shrinker struct {
	orig  sqlparser.SQLNode
	queue []sqlparser.SQLNode
}

func (s *shrinker) Next() sqlparser.SQLNode {
	for {
		// first we check if there is already something in the queue.
		// note that we are doing a nil check and not a length check here.
		// once something has been added to the queue, we are no longer
		// going to add expressions to the queue
		if s.queue != nil {
			if len(s.queue) == 0 {
				return nil
			}
			nxt := s.queue[0]
			s.queue = s.queue[1:]
			return nxt
		}
		if s.fillQueue() {
			continue
		}
		return nil
	}
}

func (s *shrinker) fillQueue() bool {
	before := len(s.queue)
	switch e := s.orig.(type) {
	case *sqlparser.AndExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *sqlparser.OrExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *sqlparser.ComparisonExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *sqlparser.BinaryExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *sqlparser.BetweenExpr:
		s.queue = append(s.queue, e.Left, e.From, e.To)
	case *sqlparser.Literal:
		switch e.Type {
		case sqlparser.StrVal:
			half := len(e.Val) / 2
			if half >= 1 {
				s.queue = append(s.queue, &sqlparser.Literal{Type: sqlparser.StrVal, Val: e.Val[:half]})
				s.queue = append(s.queue, &sqlparser.Literal{Type: sqlparser.StrVal, Val: e.Val[half:]})
			} else {
				return false
			}
		case sqlparser.IntVal:
			num, err := strconv.ParseInt(e.Val, 0, 64)
			if err != nil {
				panic(err)
			}
			if num == 0 {
				// can't simplify this more
				return false
			}

			// we'll simplify by halving the current value and decreasing it by one
			half := num / 2
			oneLess := num - 1
			if num < 0 {
				oneLess = num + 1
			}

			s.queue = append(s.queue, sqlparser.NewIntLiteral(fmt.Sprintf("%d", half)))
			if oneLess != half {
				s.queue = append(s.queue, sqlparser.NewIntLiteral(fmt.Sprintf("%d", oneLess)))
			}
		case sqlparser.FloatVal, sqlparser.DecimalVal:
			fval, err := strconv.ParseFloat(e.Val, 64)
			if err != nil {
				panic(err)
			}

			if e.Type == sqlparser.DecimalVal {
				// if it's a decimal, try to simplify as float
				fval := strconv.FormatFloat(fval, 'e', -1, 64)
				s.queue = append(s.queue, sqlparser.NewFloatLiteral(fval))
			}

			// add the value as an integer
			intval := int(fval)
			s.queue = append(s.queue, sqlparser.NewIntLiteral(fmt.Sprintf("%d", intval)))

			// we'll simplify by halving the current value and decreasing it by one
			half := fval / 2
			oneLess := fval - 1
			if fval < 0 {
				oneLess = fval + 1
			}

			s.queue = append(s.queue, sqlparser.NewFloatLiteral(fmt.Sprintf("%f", half)))
			if oneLess != half {
				s.queue = append(s.queue, sqlparser.NewFloatLiteral(fmt.Sprintf("%f", oneLess)))
			}
		default:
			panic(fmt.Sprintf("unhandled literal type %v", e.Type))
		}
	case sqlparser.ValTuple:
		// first we'll try the individual elements first
		for _, v := range e {
			s.queue = append(s.queue, v)
		}
		// then we'll try to use the slice but lacking elements
		for i := range e {
			s.queue = append(s.queue, append(e[:i], e[i+1:]...))
		}
	case *sqlparser.FuncExpr:
		for _, expr := range e.Exprs {
			s.queue = append(s.queue, expr)
		}
	case sqlparser.AggrFunc:
		for _, ae := range e.GetArgs() {
			s.queue = append(s.queue, ae)
		}

		clone := sqlparser.CloneAggrFunc(e)
		if da, ok := clone.(sqlparser.DistinctableAggr); ok {
			if da.IsDistinct() {
				da.SetDistinct(false)
				s.queue = append(s.queue, clone)
			}
		}
	case *sqlparser.ColName:
		// we can try to replace the column with a literal value
		s.queue = append(s.queue, sqlparser.NewIntLiteral("0"))
	case *sqlparser.CaseExpr:
		s.queue = append(s.queue, e.Expr, e.Else)
		for _, when := range e.Whens {
			s.queue = append(s.queue, when.Cond, when.Val)
		}

		if len(e.Whens) > 1 {
			for i := range e.Whens {
				whensCopy := sqlparser.CloneSliceOfRefOfWhen(e.Whens)
				// replace ith element with last element, then truncate last element
				whensCopy[i] = whensCopy[len(whensCopy)-1]
				whensCopy = whensCopy[:len(whensCopy)-1]
				s.queue = append(s.queue, sqlparser.NewCaseExpr(e.Expr, whensCopy, e.Else))
			}
		}

		if e.Else != nil {
			s.queue = append(s.queue, sqlparser.NewCaseExpr(e.Expr, e.Whens, nil))
		}
		if e.Expr != nil {
			s.queue = append(s.queue, sqlparser.NewCaseExpr(nil, e.Whens, e.Else))
		}
	default:
		return false
	}
	return len(s.queue) > before
}
