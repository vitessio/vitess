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

func SimplifyExpr(in sqlparser.Expr, test CheckF) (smallestKnown sqlparser.Expr) {
	var maxDepth, level int
	resetTo := func(e sqlparser.Expr) {
		smallestKnown = e
		maxDepth = depth(e)
		level = 0
	}
	resetTo(in)
	for level <= maxDepth {
		current := sqlparser.CloneExpr(smallestKnown)
		nodes, replaceF := getNodesAtLevel(current, level)
		replace := func(e sqlparser.Expr, idx int) {
			// if we are at the first level, we are replacing the root,
			// not rewriting something deep in the tree
			if level == 0 {
				current = e
			} else {
				// replace `node` in current with the simplified expression
				replaceF[idx](e)
			}
		}
		simplified := false
		for idx, node := range nodes {
			// simplify each element and create a new expression with the node replaced by the simplification
			// this means that we not only need the node, but also a way to replace the node
			s := &shrinker{orig: node}
			expr := s.Next()
			for expr != nil {
				replace(expr, idx)

				valid := test(current)
				log.Errorf("test: %t - %s", valid, sqlparser.String(current))
				if valid {
					simplified = true
					break // we will still continue trying to simplify other expressions at this level
				} else {
					// undo the change
					replace(node, idx)
				}
				expr = s.Next()
			}
		}
		if simplified {
			resetTo(current)
		} else {
			level++
		}
	}
	return smallestKnown
}

func getNodesAtLevel(e sqlparser.Expr, level int) (result []sqlparser.Expr, replaceF []func(node sqlparser.SQLNode)) {
	lvl := 0
	pre := func(cursor *sqlparser.Cursor) bool {
		if expr, isExpr := cursor.Node().(sqlparser.Expr); level == lvl && isExpr {
			result = append(result, expr)
			replaceF = append(replaceF, cursor.ReplacerF())
		}
		lvl++
		return true
	}
	post := func(cursor *sqlparser.Cursor) bool {
		lvl--
		return true
	}
	sqlparser.Rewrite(e, pre, post)
	return
}

func depth(e sqlparser.Expr) (depth int) {
	lvl := 0
	pre := func(cursor *sqlparser.Cursor) bool {
		lvl++
		if lvl > depth {
			depth = lvl
		}
		return true
	}
	post := func(cursor *sqlparser.Cursor) bool {
		lvl--
		return true
	}
	sqlparser.Rewrite(e, pre, post)
	return
}

type shrinker struct {
	orig  sqlparser.Expr
	queue []sqlparser.Expr
}

func (s *shrinker) Next() sqlparser.Expr {
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
	case *sqlparser.ComparisonExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *sqlparser.BinaryExpr:
		s.queue = append(s.queue, e.Left, e.Right)
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
		for _, ae := range e.Exprs {
			expr, ok := ae.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			s.queue = append(s.queue, expr.Expr)
		}
	case sqlparser.AggrFunc:
		for _, ae := range e.GetArgs() {
			s.queue = append(s.queue, ae)
		}
	case *sqlparser.ColName:
		// we can try to replace the column with a literal value
		s.queue = []sqlparser.Expr{sqlparser.NewIntLiteral("0")}
	default:
		return false
	}
	return len(s.queue) > before
}
