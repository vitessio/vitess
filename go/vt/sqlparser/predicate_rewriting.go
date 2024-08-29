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

import (
	"fmt"
)

var DebugRewrite = false
var prefix = " >> "

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
func RewritePredicate(ast SQLNode) SQLNode {
	return RewritePredicateInternal(ast, nil)
}

func RewritePredicateInternal(ast SQLNode, view func(SQLNode)) SQLNode {
	original := Clone(ast)

	// Beware: converting to CNF in this loop might cause exponential formula growth.
	// We bail out early to prevent going overboard.
	for loop := 0; loop < 15; loop++ {
		exprChanged := false
		stopOnChange := func(SQLNode, SQLNode) bool {
			return !exprChanged
		}
		if DebugRewrite {
			fmt.Println(String(ast))
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

		if view != nil {
			view(Clone(ast))
		}

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
		if DebugRewrite {
			fmt.Println(prefix, "not (a or b) => not a and not b")
		}
		return AndExpressions(&NotExpr{Expr: child.Left}, &NotExpr{Expr: child.Right}), true
	case *AndExpr:
		var curr Expr
		for i, p := range child.Predicates {
			if i == 0 {
				curr = &NotExpr{Expr: p}
			} else {
				curr = &OrExpr{Left: curr, Right: &NotExpr{Expr: p}}
			}
		}
		if DebugRewrite {
			fmt.Println(prefix, "not (a and b) => not a or not b")
		}
		return curr, true
	}
	return expr, false
}

func createOrs(exprs ...Expr) Expr {
	if len(exprs) == 1 {
		return exprs[0]
	}
	return &OrExpr{Left: exprs[0], Right: createOrs(exprs[1:]...)}
}

func simplifyOredAnds(or *OrExpr, lhs, rhs *AndExpr) (Expr, bool) {
	var commonPredicates []Expr
	var leftRemainder, rightRemainder []Expr

	// Find all matching predicates and separate the remainder
	rightRemainder = rhs.Predicates
	for _, lp := range lhs.Predicates {
		rhs := rightRemainder
		rightRemainder = nil
		isCommon := false
		for _, rp := range rhs {
			if Equals.Expr(lp, rp) {
				commonPredicates = append(commonPredicates, lp)
				isCommon = true
			} else {
				rightRemainder = append(rightRemainder, rp)
			}
		}
		if !isCommon {
			leftRemainder = append(leftRemainder, lp)
		}
	}

	if len(commonPredicates) == 0 {
		return or, false
	}

	// Build the final AndExpr with common predicates and the OrExpr of remainders
	commonPred := AndExpressions(commonPredicates...)
	if len(leftRemainder) == 0 && len(rightRemainder) == 0 {
		if DebugRewrite {
			fmt.Println(prefix, "remove duplicate predicates across ANDs")
		}
		return commonPred, true
	}

	switch {
	case len(rightRemainder) == 0 || len(leftRemainder) == 0:
		if DebugRewrite {
			fmt.Println(prefix, "(A and B and A and D) or (A and D) => A and D")
		}
		return commonPred, true
	default:
		if DebugRewrite {
			fmt.Println(prefix, "(A and B and C) or (A and B and D) => (A and B) and (C or D)")
		}

		return AndExpressions(commonPred, createOrs(AndExpressions(rightRemainder...), AndExpressions(leftRemainder...))), true
	}
}

func simplifyOrWithAnAND(and *AndExpr, other Expr, left bool) (Expr, bool) {
	for _, lp := range and.Predicates {
		if Equals.Expr(other, lp) {
			// if we have the same predicate on both sides of the OR, we can simplify
			// (A AND B) OR A => A
			// because if A is true, the OR is true, not matter what B is,
			// and if A is false, the AND is false, and again we don't care about B
			if DebugRewrite {
				fmt.Println(prefix, "(A AND B) OR A => A")
			}
			return other, true
		}
	}

	// Distribution Law
	var distributedPredicates []Expr
	for _, lp := range and.Predicates {
		var or *OrExpr
		if left {
			or = &OrExpr{Left: lp, Right: other}
		} else {
			or = &OrExpr{Left: other, Right: lp}
		}
		distributedPredicates = append(distributedPredicates, or)
	}
	if DebugRewrite {
		fmt.Println(prefix, "(A and B) or C => (A or C) and (B or C)")
	}
	return AndExpressions(distributedPredicates...), true
}

func simplifyOr(or *OrExpr) (Expr, bool) {
	res, rewritten := distinctOr(or)
	if rewritten {
		if DebugRewrite {
			fmt.Println(prefix, "distinct or elements")
		}

		return res, true
	}

	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)

	switch {
	case lok && rok:
		return simplifyOredAnds(or, land, rand)
	case !lok && !rok:
		return simplifyOrToIn(or)
	default:
		// if we get here, one side is an AND
		var and *AndExpr
		var other Expr
		if lok {
			and = land
			other = or.Right
		} else {
			and = rand
			other = or.Left
		}

		return simplifyOrWithAnAND(and, other, lok)
	}
}

func simplifyOrToIn(or *OrExpr) (Expr, bool) {
	lftCmp, lok := or.Left.(*ComparisonExpr)
	rgtCmp, rok := or.Right.(*ComparisonExpr)
	if lok && rok {
		newExpr, rewritten := tryTurningOrIntoIn(lftCmp, rgtCmp)
		if rewritten {
			if DebugRewrite {
				fmt.Println(prefix, "turning OR into IN")
			}
			return newExpr, true
		}
	}

	return or, false
}

func simplifyXor(xor *XorExpr) (Expr, bool) {
	if DebugRewrite {
		fmt.Println(prefix, "a xor b => (a or b) and not(a and b)")
	}
	return AndExpressions(
		&OrExpr{Left: xor.Left, Right: xor.Right},
		&NotExpr{Expr: AndExpressions(xor.Left, xor.Right)},
	), true
}

func simplifyAnd(expr *AndExpr) (Expr, bool) {
	if len(expr.Predicates) == 1 {
		if DebugRewrite {
			fmt.Println(prefix, "single predicate in AND")
		}
		return expr.Predicates[0], true
	}

	res, rewritten := distinctAnd(expr)
	if rewritten {
		if DebugRewrite {
			fmt.Println(prefix, "distinct and elements")
		}

		return res, true
	}

	var simplifiedPredicates []Expr
	simplified := false

	// Loop over all predicates in the AndExpr
outer:
	for i, andPred := range expr.Predicates {
		if or, ok := andPred.(*OrExpr); ok {
			// Check if we can simplify by matching with another predicate in the AndExpr
			for j, otherPred := range expr.Predicates {
				if i == j {
					continue // Skip the same predicate
				}

				// Simplification: and(or(a,b), a) => a
				if Equals.Expr(or.Left, otherPred) || Equals.Expr(or.Right, otherPred) {
					// Found a match, keep the simpler expression (otherPred)
					simplifiedPredicates = append(simplifiedPredicates, otherPred)
					simplified = true
					continue outer
				}
			}
		}

		// No simplification possible, keep the original predicate
		simplifiedPredicates = append(simplifiedPredicates, andPred)
	}

	if simplified {
		if DebugRewrite {
			fmt.Println(prefix, "(a or b) and a => a")
		}

		// Return a new AndExpr with the simplified predicates
		return AndExpressions(simplifiedPredicates...), true
	}

	return expr, false
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

// andToSlice will return a slice of comparisons, containing all the comparison expressions in the AND expression
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

	for _, p := range andExpr.Predicates {
		if !handleAndSide(p) {
			return nil
		}
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
		for _, p := range curr.Predicates {
			if and, ok := p.(*AndExpr); ok {
				// we will flatten the ANDs
				changed = true
				todo = append(todo, and)
			} else {
				leaves = append(leaves, p)
			}
		}
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

	return AndExpressions(leaves...), true
}
