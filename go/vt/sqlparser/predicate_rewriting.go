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

import "slices"

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
func RewritePredicate(ast SQLNode) SQLNode {
	original := CloneSQLNode(ast)

	// Beware: converting to CNF in this loop might cause exponential formula growth.
	// We bail out early to prevent going overboard.
	for loop := 0; loop < 15; loop++ {
		exprChanged := false
		stopOnChange := func(SQLNode, SQLNode) bool {
			return !exprChanged
		}
		ast = SafeRewrite(ast, stopOnChange, func(cursor *Cursor) bool {
			e, isExpr := cursor.node.(Expr)
			if !isExpr {
				return true
			}

			rewritten, changed := simplifyExpression(e)
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

	return original
}

func simplifyExpression(expr Expr) (Expr, bool) {
	switch expr := expr.(type) {
	case *NotExpr:
		return simplifyNot(expr)
	case *OrExpr:
		return simplifyOr(expr)
	case *XorExpr:
		return simplifyXor(expr)
	case *AndExpr:
		return simplifyAnd(expr)
	}
	return expr, false
}

func simplifyNot(expr *NotExpr) (Expr, bool) {
	switch child := expr.Expr.(type) {
	case *NotExpr:
		return child.Expr, true
	case *OrExpr:
		// not(or(a,b)) => and(not(a),not(b))
		return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
	case *AndExpr:
		// not(and(a,b)) => or(not(a), not(b))
		return &OrExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
	}
	return expr, false
}

func simplifyOr(expr *OrExpr) (Expr, bool) {
	res, rewritten := distinctOr(expr)
	if rewritten {
		return res, true
	}

	or := expr

	// first we search for ANDs and see how they can be simplified
	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)

	if lok && rok {
		// (<> AND <>) OR (<> AND <>)
		// or(and(T1,T2), and(T2, T3)) => and(T1, or(T2, T2))
		var a, b, c Expr
		switch {
		case Equals.Expr(land.Left, rand.Left):
			a, b, c = land.Left, land.Right, rand.Right
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		case Equals.Expr(land.Left, rand.Right):
			a, b, c = land.Left, land.Right, rand.Left
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		case Equals.Expr(land.Right, rand.Left):
			a, b, c = land.Right, land.Left, rand.Right
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		case Equals.Expr(land.Right, rand.Right):
			a, b, c = land.Right, land.Left, rand.Left
			return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
		}
	}

	// (<> AND <>) OR <>
	if lok {
		// Simplification
		if Equals.Expr(or.Right, land.Left) || Equals.Expr(or.Right, land.Right) {
			// or(and(a,b), c) => c   where c=a or c=b
			return or.Right, true
		}

		// Distribution Law
		//  or(c, and(a,b)) => and(or(c,a), or(c,b))
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
			return or.Left, true
		}

		// Distribution Law
		//  or(and(a,b), c) => and(or(c,a), or(c,b))
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
			// or(a=x,a=y) => in(a,[x,y])
			return newExpr, true
		}
	}

	// Try to make distinct
	result, changed := distinctOr(expr)
	if changed {
		return result, true
	}
	return result, false
}

func simplifyXor(expr *XorExpr) (Expr, bool) {
	// xor(a,b) => and(or(a,b), not(and(a,b))
	return &AndExpr{
		Left:  &OrExpr{Left: expr.Left, Right: expr.Right},
		Right: &NotExpr{Expr: &AndExpr{Left: expr.Left, Right: expr.Right}},
	}, true
}

func simplifyAnd(expr *AndExpr) (Expr, bool) {
	res, rewritten := distinctAnd(expr)
	if rewritten {
		return res, true
	}
	and := expr
	if or, ok := and.Left.(*OrExpr); ok {
		// Simplification
		// and(or(a,b),c) => c when c=a or c=b
		if Equals.Expr(or.Left, and.Right) {
			return and.Right, true
		}
		if Equals.Expr(or.Right, and.Right) {
			return and.Right, true
		}
	}
	if or, ok := and.Right.(*OrExpr); ok {
		// Simplification
		if Equals.Expr(or.Left, and.Left) {
			return and.Left, true
		}
		if Equals.Expr(or.Right, and.Left) {
			return and.Left, true
		}
	}

	return expr, false
}

// ExtractINFromOR rewrites the OR expression into an IN clause.
// Each side of each ORs has to be an equality comparison expression and the column names have to
// match for all sides of each comparison.
// This rewriter takes a query that looks like this WHERE a = 1 and b = 11 or a = 2 and b = 12 or a = 3 and b = 13
// And rewrite that to WHERE (a, b) IN ((1,11), (2,12), (3,13))
func ExtractINFromOR(expr *OrExpr) []Expr {
	var varNames []*ColName
	var values []Exprs
	orSlice := orToSlice(expr)
	for _, expr := range orSlice {
		andSlice := andToSlice(expr)
		if len(andSlice) == 0 {
			return nil
		}

		var currentVarNames []*ColName
		var currentValues []Expr
		for _, comparisonExpr := range andSlice {
			if comparisonExpr.Operator != EqualOp {
				return nil
			}

			var colName *ColName
			if left, ok := comparisonExpr.Left.(*ColName); ok {
				colName = left
				currentValues = append(currentValues, comparisonExpr.Right)
			}

			if right, ok := comparisonExpr.Right.(*ColName); ok {
				if colName != nil {
					return nil
				}
				colName = right
				currentValues = append(currentValues, comparisonExpr.Left)
			}

			if colName == nil {
				return nil
			}

			currentVarNames = append(currentVarNames, colName)
		}

		if len(varNames) == 0 {
			varNames = currentVarNames
		} else if !slices.EqualFunc(varNames, currentVarNames, func(col1, col2 *ColName) bool { return col1.Equal(col2) }) {
			return nil
		}

		values = append(values, currentValues)
	}

	var nameTuple ValTuple
	for _, name := range varNames {
		nameTuple = append(nameTuple, name)
	}

	var valueTuple ValTuple
	for _, value := range values {
		valueTuple = append(valueTuple, ValTuple(value))
	}

	return []Expr{&ComparisonExpr{
		Operator: InOp,
		Left:     nameTuple,
		Right:    valueTuple,
	}}
}

func orToSlice(expr *OrExpr) []Expr {
	var exprs []Expr

	handleOrSide := func(e Expr) {
		switch e := e.(type) {
		case *OrExpr:
			exprs = append(exprs, orToSlice(e)...)
		default:
			exprs = append(exprs, e)
		}
	}

	handleOrSide(expr.Left)
	handleOrSide(expr.Right)
	return exprs
}

func andToSlice(expr Expr) []*ComparisonExpr {
	var andExpr *AndExpr
	switch expr := expr.(type) {
	case *AndExpr:
		andExpr = expr
	case *ComparisonExpr:
		return []*ComparisonExpr{expr}
	default:
		return nil
	}

	var exprs []*ComparisonExpr
	handleAndSide := func(e Expr) bool {
		switch e := e.(type) {
		case *AndExpr:
			slice := andToSlice(e)
			if slice == nil {
				return false
			}
			exprs = append(exprs, slice...)
		case *ComparisonExpr:
			exprs = append(exprs, e)
		default:
			return false
		}
		return true
	}

	if !handleAndSide(andExpr.Left) {
		return nil
	}
	if !handleAndSide(andExpr.Right) {
		return nil
	}

	return exprs
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

func distinctOr(in *OrExpr) (result Expr, changed bool) {
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
	for _, curr := range leaves {
		for _, alreadyIn := range predicates {
			if Equals.Expr(alreadyIn, curr) {
				changed = true
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if !changed {
		return in, false
	}

	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &OrExpr{Left: result, Right: curr}
	}

	return
}

func distinctAnd(in *AndExpr) (result Expr, changed bool) {
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
				changed = true
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}

	if !changed {
		return in, false
	}

	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &AndExpr{Left: result, Right: curr}
	}
	return AndExpressions(leaves...), true
}
