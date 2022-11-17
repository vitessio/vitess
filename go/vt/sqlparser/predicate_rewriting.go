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

import "vitess.io/vitess/go/vt/log"

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
// Note: In order to re-plan, we need to empty the accumulated metadata in the AST,
// so ColName.Metadata will be nil:ed out as part of this rewrite
func RewritePredicate(ast SQLNode) SQLNode {
	for {
		finishedRewrite := true
		log.Errorf(String(ast))
		ast = Rewrite(ast, nil, func(cursor *Cursor) bool {
			if e, isExpr := cursor.node.(Expr); isExpr {
				rewritten, didRewrite := simplifyExpression(e)
				if didRewrite {
					finishedRewrite = false
					cursor.Replace(rewritten)
				}
			}
			if col, isCol := cursor.node.(*ColName); isCol {
				col.Metadata = nil
			}
			return true
		})

		if finishedRewrite {
			return ast
		}
	}
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
		// NOT NOT A => A
		return child.Expr, true
	case *OrExpr:
		// DeMorgan Rewriter
		// NOT (A OR B) => NOT A AND NOT B
		return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
	case *AndExpr:
		// DeMorgan Rewriter
		// NOT (A AND B) => NOT A OR NOT B
		return &OrExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, true
	}
	return expr, false
}

func simplifyOr(expr *OrExpr) (Expr, bool) {
	or := expr

	// first we search for ANDs and see how they can be simplified
	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)
	switch {
	case lok && rok:
		var a, b, c Expr
		switch {
		// (A and B) or (A and C) => A AND (B OR C)
		case EqualsExpr(land.Left, rand.Left):
			a, b, c = land.Left, land.Right, rand.Right
		// (A and B) or (C and A) => A AND (B OR C)
		case EqualsExpr(land.Left, rand.Right):
			a, b, c = land.Left, land.Right, rand.Left
		// (B and A) or (A and C) => A AND (B OR C)
		case EqualsExpr(land.Right, rand.Left):
			a, b, c = land.Right, land.Left, rand.Right
		// (B and A) or (C and A) => A AND (B OR C)
		case EqualsExpr(land.Right, rand.Right):
			a, b, c = land.Right, land.Left, rand.Left
		default:
			return expr, false
		}
		return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, true
	case lok:
		// Simplification
		// (A AND B) OR A => A
		if EqualsExpr(or.Right, land.Left) || EqualsExpr(or.Right, land.Right) {
			return or.Right, true
		}
		// Distribution Law
		// (A AND B) OR C => (A OR C) AND (B OR C)
		return &AndExpr{Left: &OrExpr{Left: land.Left, Right: or.Right}, Right: &OrExpr{Left: land.Right, Right: or.Right}}, true
	case rok:
		// Simplification
		// A OR (A AND B) => A
		if EqualsExpr(or.Left, rand.Left) || EqualsExpr(or.Left, rand.Right) {
			return or.Left, true
		}
		// Distribution Law
		// C OR (A AND B) => (C OR A) AND (C OR B)
		return &AndExpr{Left: &OrExpr{Left: or.Left, Right: rand.Left}, Right: &OrExpr{Left: or.Left, Right: rand.Right}}, true
	}

	// next, we want to try to turn multiple ORs into an IN when possible
	lftCmp, lok := or.Left.(*ComparisonExpr)
	rgtCmp, rok := or.Right.(*ComparisonExpr)
	if lok && rok {
		newExpr, rewritten := tryTurningOrIntoIn(lftCmp, rgtCmp)
		if rewritten {
			return newExpr, true
		}
	}

	// Try to make distinct
	return distinctOr(expr)
}

func tryTurningOrIntoIn(l, r *ComparisonExpr) (Expr, bool) {
	// looks for A = X OR A = Y and turns them into A IN (X, Y)
	col, ok := l.Left.(*ColName)
	if !ok || !EqualsExpr(col, r.Left) {
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
			if EqualsExpr(expr, seen) {
				continue outer
			}
		}
		output = append(output, expr)
	}
	return
}

func simplifyXor(expr *XorExpr) (Expr, bool) {
	// DeMorgan Rewriter
	// (A XOR B) => (A OR B) AND NOT (A AND B)
	return &AndExpr{Left: &OrExpr{Left: expr.Left, Right: expr.Right}, Right: &NotExpr{Expr: &AndExpr{Left: expr.Left, Right: expr.Right}}}, true
}

func simplifyAnd(expr *AndExpr) (Expr, bool) {
	res, rewritten := distinctAnd(expr)
	if rewritten {
		return res, rewritten
	}
	and := expr
	if or, ok := and.Left.(*OrExpr); ok {
		// Simplification
		// (A OR B) AND A => A
		if EqualsExpr(or.Left, and.Right) || EqualsExpr(or.Right, and.Right) {
			return and.Right, true
		}
	}
	if or, ok := and.Right.(*OrExpr); ok {
		// Simplification
		// A OR (A AND B) => A
		if EqualsExpr(or.Left, and.Left) || EqualsExpr(or.Right, and.Left) {
			return or.Left, true
		}
	}

	return expr, false

}
