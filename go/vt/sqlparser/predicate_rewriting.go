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

const (
	Changed  RewriteState = true
	NoChange RewriteState = false
)

type RewriteState bool

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
// Note: In order to re-plan, we need to empty the accumulated metadata in the AST,
// so ColName.Metadata will be nil:ed out as part of this rewrite
func RewritePredicate(ast SQLNode) SQLNode {
	for {
		finishedRewrite := true
		ast = Rewrite(ast, nil, func(cursor *Cursor) bool {
			if e, isExpr := cursor.node.(Expr); isExpr {
				rewritten, state := simplifyExpression(e)
				if state == Changed {
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

func simplifyExpression(expr Expr) (Expr, RewriteState) {
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
	return expr, NoChange
}

func simplifyNot(expr *NotExpr) (Expr, RewriteState) {
	switch child := expr.Expr.(type) {
	case *NotExpr:
		// NOT NOT A => A
		return child.Expr, Changed
	case *OrExpr:
		// DeMorgan Rewriter
		// NOT (A OR B) => NOT A AND NOT B
		return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, Changed
	case *AndExpr:
		// DeMorgan Rewriter
		// NOT (A AND B) => NOT A OR NOT B
		return &OrExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, Changed
	}
	return expr, NoChange
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
			in, state := tryTurningOrIntoIn(l, r)
			if state == Changed {
				ins = append(ins, in)
			}
		}
	}

	return ins
}

func simplifyOr(expr *OrExpr) (Expr, RewriteState) {
	or := expr

	// first we search for ANDs and see how they can be simplified
	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)
	switch {
	case lok && rok:
		var a, b, c Expr
		switch {
		// (A and B) or (A and C) => A AND (B OR C)
		case Equals.Expr(land.Left, rand.Left):
			a, b, c = land.Left, land.Right, rand.Right
		// (A and B) or (C and A) => A AND (B OR C)
		case Equals.Expr(land.Left, rand.Right):
			a, b, c = land.Left, land.Right, rand.Left
		// (B and A) or (A and C) => A AND (B OR C)
		case Equals.Expr(land.Right, rand.Left):
			a, b, c = land.Right, land.Left, rand.Right
		// (B and A) or (C and A) => A AND (B OR C)
		case Equals.Expr(land.Right, rand.Right):
			a, b, c = land.Right, land.Left, rand.Left
		default:
			return expr, NoChange
		}
		return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, Changed
	case lok:
		// Simplification
		// (A AND B) OR A => A
		if Equals.Expr(or.Right, land.Left) || Equals.Expr(or.Right, land.Right) {
			return or.Right, Changed
		}
		// Distribution Law
		// (A AND B) OR C => (A OR C) AND (B OR C)
		return &AndExpr{Left: &OrExpr{Left: land.Left, Right: or.Right}, Right: &OrExpr{Left: land.Right, Right: or.Right}}, Changed
	case rok:
		// Simplification
		// A OR (A AND B) => A
		if Equals.Expr(or.Left, rand.Left) || Equals.Expr(or.Left, rand.Right) {
			return or.Left, Changed
		}
		// Distribution Law
		// C OR (A AND B) => (C OR A) AND (C OR B)
		return &AndExpr{Left: &OrExpr{Left: or.Left, Right: rand.Left}, Right: &OrExpr{Left: or.Left, Right: rand.Right}}, Changed
	}

	// next, we want to try to turn multiple ORs into an IN when possible
	lftCmp, lok := or.Left.(*ComparisonExpr)
	rgtCmp, rok := or.Right.(*ComparisonExpr)
	if lok && rok {
		newExpr, rewritten := tryTurningOrIntoIn(lftCmp, rgtCmp)
		if rewritten {
			return newExpr, Changed
		}
	}

	// Try to make distinct
	return distinctOr(expr)
}

func tryTurningOrIntoIn(l, r *ComparisonExpr) (Expr, RewriteState) {
	// looks for A = X OR A = Y and turns them into A IN (X, Y)
	col, ok := l.Left.(*ColName)
	if !ok || !Equals.Expr(col, r.Left) {
		return nil, NoChange
	}

	var tuple ValTuple

	switch l.Operator {
	case EqualOp:
		tuple = ValTuple{l.Right}
	case InOp:
		lft, ok := l.Right.(ValTuple)
		if !ok {
			return nil, NoChange
		}
		tuple = lft
	default:
		return nil, NoChange
	}

	switch r.Operator {
	case EqualOp:
		tuple = append(tuple, r.Right)
	case InOp:
		lft, ok := r.Right.(ValTuple)
		if !ok {
			return nil, NoChange
		}
		tuple = append(tuple, lft...)
	default:
		return nil, NoChange
	}

	return &ComparisonExpr{
		Operator: InOp,
		Left:     col,
		Right:    uniquefy(tuple),
	}, Changed
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

func simplifyXor(expr *XorExpr) (Expr, RewriteState) {
	// DeMorgan Rewriter
	// (A XOR B) => (A OR B) AND NOT (A AND B)
	return &AndExpr{Left: &OrExpr{Left: expr.Left, Right: expr.Right}, Right: &NotExpr{Expr: &AndExpr{Left: expr.Left, Right: expr.Right}}}, Changed
}

func simplifyAnd(expr *AndExpr) (Expr, RewriteState) {
	res, rewritten := distinctAnd(expr)
	if rewritten {
		return res, rewritten
	}
	and := expr
	if or, ok := and.Left.(*OrExpr); ok {
		// Simplification
		// (A OR B) AND A => A
		if Equals.Expr(or.Left, and.Right) || Equals.Expr(or.Right, and.Right) {
			return and.Right, Changed
		}
	}
	if or, ok := and.Right.(*OrExpr); ok {
		// Simplification
		// A OR (A AND B) => A
		if Equals.Expr(or.Left, and.Left) || Equals.Expr(or.Right, and.Left) {
			return or.Left, Changed
		}
	}

	return expr, NoChange
}

func distinctOr(in *OrExpr) (Expr, RewriteState) {
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
	original := len(leaves)
	var predicates []Expr

outer1:
	for len(leaves) > 0 {
		curr := leaves[0]
		leaves = leaves[1:]
		for _, alreadyIn := range predicates {
			if Equals.Expr(alreadyIn, curr) {
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if original == len(predicates) {
		return in, NoChange
	}
	var result Expr
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &OrExpr{Left: result, Right: curr}
	}
	return result, Changed
}

func distinctAnd(in *AndExpr) (Expr, RewriteState) {
	todo := []*AndExpr{in}
	var leaves []Expr
	for len(todo) > 0 {
		curr := todo[0]
		todo = todo[1:]
		addAnd := func(in Expr) {
			and, ok := in.(*AndExpr)
			if ok {
				todo = append(todo, and)
			} else {
				leaves = append(leaves, in)
			}
		}
		addAnd(curr.Left)
		addAnd(curr.Right)
	}
	original := len(leaves)
	var predicates []Expr

outer1:
	for len(leaves) > 0 {
		curr := leaves[0]
		leaves = leaves[1:]
		for _, alreadyIn := range predicates {
			if Equals.Expr(alreadyIn, curr) {
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if original == len(predicates) {
		return in, NoChange
	}
	var result Expr
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &AndExpr{Left: result, Right: curr}
	}
	return result, Changed
}
