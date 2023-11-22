/*
Copyright 2022 The Vitess Authors.

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

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
func RewritePredicate(ast SQLNode) SQLNode {
	original, initialSize := cloneAndSize(ast)

	r := &rewriter{
		initialSize: initialSize,
		currentSize: initialSize,
	}
	for {
		if r.currentSize > initialSize*10 {
			// the rewritten expression is growing too much.
			// we'll abort here and return the original expression instead
			return original
		}
		exprChanged := false
		stopOnChange := func(SQLNode, SQLNode) bool {
			return !exprChanged
		}
		ast = SafeRewrite(ast, stopOnChange, func(cursor *Cursor) bool {
			e, isExpr := cursor.node.(Expr)
			if !isExpr {
				return true
			}

			rewritten, changed := r.simplifyExpression(e)
			if changed {
				exprChanged = true
				cursor.Replace(rewritten)
			}
			return !exprChanged
		})

		if !exprChanged {
			return ast
		}
	}
}

func cloneAndSize(ast SQLNode) (SQLNode, int) {
	initialSize := 0
	original := CopyOnRewrite(ast, nil, func(cursor *CopyOnWriteCursor) {
		node := cursor.Node()
		_, ok := node.(Expr)
		if !ok {
			return
		}
		initialSize++

		col, ok := node.(*ColName)
		if !ok {
			return
		}

		// we'll clone these just to make the CopyOnRewrite give us a clone of the entire expression
		klone := *col
		cursor.Replace(&klone)
	}, nil)
	return original, initialSize
}

type rewriter struct {
	initialSize, currentSize int
}

func (r *rewriter) simplifyExpression(expr Expr) (Expr, bool) {
	switch expr := expr.(type) {
	case *NotExpr:
		return r.simplifyNot(expr)
	case *OrExpr:
		return r.simplifyOr(expr)
	case *XorExpr:
		return r.simplifyXor(expr)
	case *AndExpr:
		return r.simplifyAnd(expr)
	}
	return expr, false
}

func (r *rewriter) simplifyNot(expr *NotExpr) (Expr, bool) {
	switch child := expr.Expr.(type) {
	case *NotExpr:
		r.currentSize--
		return child.Expr, true
	case *OrExpr:
		// we start with 4 expressions and finish with 5
		// not(or(a,b)) => and(not(a),not(b))
		r.currentSize++
		return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
	case *AndExpr:
		// we start with 4 expressions and finish with 5
		// not(and(a,b)) => or(not(a), not(b))
		r.currentSize++
		return &OrExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
	}
	return expr, false
}

func (r *rewriter) simplifyOr(expr *OrExpr) (Expr, bool) {
	or := expr

	// first we search for ANDs and see how they can be simplified
	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)

	if lok && rok {
		// (<> AND <>) OR (<> AND <>)
		// or(and(T1,T2), and(T2, T3)) => and(T1, or(T2, T2))
		// All of these rewrites goes from 7 expressions to 5
		var a, b, c Expr
		switch {
		case Equals.Expr(land.Left, rand.Left):
			a, b, c = land.Left, land.Right, rand.Right
			r.currentSize -= 2
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		case Equals.Expr(land.Left, rand.Right):
			a, b, c = land.Left, land.Right, rand.Left
			r.currentSize -= 2
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		case Equals.Expr(land.Right, rand.Left):
			a, b, c = land.Right, land.Left, rand.Right
			r.currentSize -= 2
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		case Equals.Expr(land.Right, rand.Right):
			a, b, c = land.Right, land.Left, rand.Left
			r.currentSize -= 2
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		}
	}

	// (<> AND <>) OR <>
	if lok {
		// Simplification
		if Equals.Expr(or.Right, land.Left) || Equals.Expr(or.Right, land.Right) {
			// or(and(a,b), c) => c   where c=a or c=b   5 -> 1
			r.currentSize -= 4
			return or.Right, true
		}

		// Distribution Law
		//  or(c, and(a,b)) => and(or(c,a), or(c,b)) 5 -> 7
		r.currentSize += 2
		return &AndExpr{
			Left: &OrExpr{
				Left:  land.Left,
				Right: or.Right,
			},
			Right: &OrExpr{
				Left:  land.Right,
				Right: or.Right,
			},
		}, true
	}

	// <> OR (<> AND <>)
	if rok {
		// Simplification
		if Equals.Expr(or.Left, rand.Left) || Equals.Expr(or.Left, rand.Right) {
			// or(a,and(b,c)) => a
			r.currentSize -= 4
			return or.Left, true
		}

		// Distribution Law
		//  or(and(a,b), c) => and(or(c,a), or(c,b)) 5 -> 7
		r.currentSize += 2
		return &AndExpr{
			Left:  &OrExpr{Left: or.Left, Right: rand.Left},
			Right: &OrExpr{Left: or.Left, Right: rand.Right},
		}, true
	}

	// next, we want to try to turn multiple ORs into an IN when possible
	lftCmp, lok := or.Left.(*ComparisonExpr)
	rgtCmp, rok := or.Right.(*ComparisonExpr)
	if lok && rok {
		newExpr, rewritten := tryTurningOrIntoIn(lftCmp, rgtCmp)
		if rewritten {
			// or(a=x,a=y) => in(a,[x,y]) 7 -> 4
			r.currentSize -= 3
			return newExpr, rewritten
		}
	}

	// Try to make distinct
	result, removed := distinctOr(expr)
	r.currentSize -= removed
	return result, removed > 0
}

func (r *rewriter) simplifyXor(expr *XorExpr) (Expr, bool) {
	// xor(a,b) => and(or(a,b), not(and(a,b)) 3 -> 8
	r.currentSize += 8
	return &AndExpr{
		Left:  &OrExpr{Left: expr.Left, Right: expr.Right},
		Right: &NotExpr{Expr: &AndExpr{Left: expr.Left, Right: expr.Right}},
	}, true
}

func (r *rewriter) simplifyAnd(expr *AndExpr) (Expr, bool) {
	res, rewritten := distinctAnd(expr)
	if rewritten > 0 {
		r.currentSize -= rewritten
		return res, true
	}
	and := expr
	if or, ok := and.Left.(*OrExpr); ok {
		// Simplification
		// and(or(a,b),c) => c when c=a or c=b
		// 5 -> 1
		if Equals.Expr(or.Left, and.Right) {
			r.currentSize -= 4
			return and.Right, true
		}
		if Equals.Expr(or.Right, and.Right) {
			r.currentSize -= 4
			return and.Right, true
		}
	}
	if or, ok := and.Right.(*OrExpr); ok {
		// Simplification
		if Equals.Expr(or.Left, and.Left) {
			r.currentSize -= 4
			return and.Left, true
		}
		if Equals.Expr(or.Right, and.Left) {
			r.currentSize -= 4
			return and.Left, true
		}
	}

	return expr, false
}

// ExtractINFromOR will add additional predicated to an OR.
// this rewriter should not be used in a fixed point way, since it returns the original expression with additions,
// and it will therefor OOM before it stops rewriting
func ExtractINFromOR(expr *OrExpr) []Expr {
	// we check if we have two comparisons on either side of the OR
	// that we can add as an ANDed comparison.
	// WHERE (a = 5 and B) or (a = 6 AND C) =>
	// WHERE (a = 5 AND B) OR (a = 6 AND C) AND a IN (5,6)
	// This rewrite makes it possible to find a better route than Scatter if the `a` column has a helpful vindex
	lftPredicates := SplitAndExpression(nil, expr.Left)
	rgtPredicates := SplitAndExpression(nil, expr.Right)
	var ins []Expr
	for _, lft := range lftPredicates {
		l, ok := lft.(*ComparisonExpr)
		if !ok {
			continue
		}
		for _, rgt := range rgtPredicates {
			r, ok := rgt.(*ComparisonExpr)
			if !ok {
				continue
			}
			in, changed := tryTurningOrIntoIn(l, r)
			if changed {
				ins = append(ins, in)
			}
		}
	}

	return uniquefy(ins)
}

func tryTurningOrIntoIn(l, r *ComparisonExpr) (Expr, bool) {
	// looks for A = X OR A = Y and turns them into A IN (X, Y)
	col, ok := l.Left.(*ColName)
	if !ok || !Equals.Expr(col, r.Left) {
		return nil, false
	}

	var tuple ValTuple

	switch l.Operator {
	case EqualOp:
		tuple = ValTuple{l.Right}
	case InOp:
		lft, ok := l.Right.(ValTuple)
		if !ok {
			return nil, false
		}
		tuple = lft
	default:
		return nil, false
	}

	switch r.Operator {
	case EqualOp:
		tuple = append(tuple, r.Right)

	case InOp:
		lft, ok := r.Right.(ValTuple)
		if !ok {
			return nil, false
		}
		tuple = append(tuple, lft...)

	default:
		return nil, false
	}

	return &ComparisonExpr{
		Operator: InOp,
		Left:     col,
		Right:    uniquefy(tuple),
	}, true
}

func uniquefy(tuple ValTuple) (output ValTuple) {
outer:
	for _, expr := range tuple {
		for _, seen := range output {
			if Equals.Expr(expr, seen) {
				continue outer
			}
		}
		output = append(output, expr)
	}
	return
}

func distinctOr(in *OrExpr) (result Expr, removed int) {

	todo := []*OrExpr{in}
	var leaves []Expr
	for len(todo) > 0 {
		curr := todo[0]
		todo = todo[1:]
		addAnd := func(in Expr) {
			and, ok := in.(*OrExpr)
			if ok {
				todo = append(todo, and)
			} else {
				leaves = append(leaves, in)
			}
		}
		addAnd(curr.Left)
		addAnd(curr.Right)
	}

	var predicates []Expr

outer1:
	for len(leaves) > 0 {
		curr := leaves[0]
		leaves = leaves[1:]
		for _, alreadyIn := range predicates {
			if Equals.Expr(alreadyIn, curr) {
				removed++
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if removed == 0 {
		return in, 0
	}

	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &OrExpr{Left: result, Right: curr}
	}

	return result, removed
}

func distinctAnd(in *AndExpr) (result Expr, removed int) {
	todo := []*AndExpr{in}
	var leaves []Expr
	for len(todo) > 0 {
		curr := todo[0]
		todo = todo[1:]
		addExpr := func(in Expr) {
			if and, ok := in.(*AndExpr); ok {
				todo = append(todo, and)
			} else {
				leaves = append(leaves, in)
			}
		}
		addExpr(curr.Left)
		addExpr(curr.Right)
	}
	var predicates []Expr

outer1:
	for _, curr := range leaves {
		for _, alreadyIn := range predicates {
			if Equals.Expr(alreadyIn, curr) {
				removed++
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if removed == 0 {
		return in, 0
	}
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &AndExpr{Left: result, Right: curr}
	}
	return AndExpressions(leaves...), removed
}
