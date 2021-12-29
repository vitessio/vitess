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

package sqlparser

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/log"
)

// CheckF is used to see if the given expression exhibits the sought after issue
type CheckF = func(Expr) bool

func SimplifyExpr(in Expr, test CheckF) (smallestKnown Expr) {
	var maxDepth, level int
	resetTo := func(e Expr) {
		smallestKnown = e
		maxDepth = depth(e)
		level = 0
	}
	resetTo(in)
	for level <= maxDepth {
		current := CloneExpr(smallestKnown)
		nodes, replaceF := getNodesAtLevel(current, level)
		replace := func(e Expr, idx int) {
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
			s := &Shrinker{Orig: node}
			expr := s.Next()
			for expr != nil {
				replace(expr, idx)

				valid := test(current)
				log.Errorf("test: %t - %s", valid, String(current))
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

func getNodesAtLevel(e Expr, level int) (result []Expr, replaceF []func(node SQLNode)) {
	lvl := 0
	pre := func(cursor *Cursor) bool {

		if expr, isExpr := cursor.Node().(Expr); level == lvl && isExpr {
			result = append(result, expr)
			replaceF = append(replaceF, cursor.ReplacerF())
		}
		lvl++
		return true
	}
	post := func(cursor *Cursor) bool {
		lvl--
		return true
	}
	Rewrite(e, pre, post)
	return
}

func depth(e Expr) (depth int) {
	lvl := 0
	pre := func(cursor *Cursor) bool {
		lvl++
		if lvl > depth {
			depth = lvl
		}
		return true
	}
	post := func(cursor *Cursor) bool {
		lvl--
		return true
	}
	Rewrite(e, pre, post)
	return
}

type Shrinker struct {
	Orig  Expr
	queue []Expr
}

func (s *Shrinker) Next() Expr {
	if s.queue != nil {
		if len(s.queue) == 0 {
			return nil
		}
		nxt := s.queue[0]
		s.queue = s.queue[1:]
		return nxt
	}

	switch e := s.Orig.(type) {
	case *ComparisonExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *BinaryExpr:
		s.queue = append(s.queue, e.Left, e.Right)
	case *Literal:
		switch e.Type {
		case StrVal:
			half := len(e.Val) / 2
			if half >= 1 {
				s.queue = append(s.queue, &Literal{Type: StrVal, Val: e.Val[:half]})
				s.queue = append(s.queue, &Literal{Type: StrVal, Val: e.Val[half:]})
			} else {
				return nil
			}
		case IntVal:
			num, err := strconv.ParseInt(e.Val, 0, 64)
			if err != nil {
				panic(err)
			}
			if num == 0 {
				// can't simplify this more
				return nil
			}

			// we'll simplify by halving the current value and decreasing it by one
			half := num / 2
			oneLess := num - 1
			if num < 0 {
				oneLess = num + 1
			}

			s.queue = append(s.queue, NewIntLiteral(fmt.Sprintf("%d", half)))
			if oneLess != half {
				s.queue = append(s.queue, NewIntLiteral(fmt.Sprintf("%d", oneLess)))
			}
		default:
			panic(fmt.Sprintf("unhandled type %v", e.Type))
		}
	case ValTuple:
		// first we'll try the individual elements first
		for _, v := range e {
			s.queue = append(s.queue, v)
		}
		// then we'll try to use the slice but lacking elements
		for i := range e {
			s.queue = append(s.queue, append(e[:i], e[i+1:]...))
		}
	case *NullVal:
		return nil
	case *FuncExpr:
		for _, ae := range e.Exprs {
			expr, ok := ae.(*AliasedExpr)
			if !ok {
				continue
			}
			s.queue = append(s.queue, expr.Expr)
		}
		if s.queue == nil {
			return nil
		}
	case *ColName:
		// we can try to replace the column with a literal value
		s.queue = []Expr{NewIntLiteral("0")}
	default:
		panic(fmt.Sprintf("%T", e))
	}
	return s.Next()
}
