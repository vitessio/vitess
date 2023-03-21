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
	"vitess.io/vitess/go/vt/log"
)

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
// Note: In order to re-plan, we need to empty the accumulated metadata in the AST,
// so ColName.Metadata will be nil:ed out as part of this rewrite
func RewritePredicate(ast SQLNode) SQLNode {
	for {
		printExpr(ast)
		exprChanged := false
		stopOnChange := func(SQLNode, SQLNode) bool {
			return !exprChanged
		}
		ast = SafeRewrite(ast, stopOnChange, func(cursor *Cursor) bool {
			e, isExpr := cursor.node.(Expr)
			if !isExpr {
				return true
			}

			rewritten, state := simplifyExpression(e)
			if ch, isChange := state.(changed); isChange {
				printRule(ch.rule, ch.exprMatched)
				exprChanged = true
				cursor.Replace(rewritten)
			}

			if col, isCol := cursor.node.(*ColName); isCol {
				col.Metadata = nil
			}
			return !exprChanged
		})

		if !exprChanged {
			return ast
		}
	}
}

func simplifyExpression(expr Expr) (Expr, rewriteState) {
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
	return expr, noChange{}
}

func simplifyNot(expr *NotExpr) (Expr, rewriteState) {
	switch child := expr.Expr.(type) {
	case *NotExpr:
		return child.Expr,
			newChange("NOT NOT A => A", f(expr))
	case *OrExpr:
		return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}},
			newChange("NOT (A OR B) => NOT A AND NOT B", f(expr))
	case *AndExpr:
		return &OrExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}},
			newChange("NOT (A AND B) => NOT A OR NOT B", f(expr))
	}
	return expr, noChange{}
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
			if state.changed() {
				ins = append(ins, in)
			}
		}
	}

	return uniquefy(ins)
}

func simplifyOr(expr *OrExpr) (Expr, rewriteState) {
	or := expr

	// first we search for ANDs and see how they can be simplified
	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)
	switch {
	case lok && rok:
		// (<> AND <>) OR (<> AND <>)
		var a, b, c Expr
		var change changed
		switch {
		case Equals.Expr(land.Left, rand.Left):
			change = newChange("(A and B) or (A and C) => A AND (B OR C)", f(expr))
			a, b, c = land.Left, land.Right, rand.Right
		case Equals.Expr(land.Left, rand.Right):
			change = newChange("(A and B) or (C and A) => A AND (B OR C)", f(expr))
			a, b, c = land.Left, land.Right, rand.Left
		case Equals.Expr(land.Right, rand.Left):
			change = newChange("(B and A) or (A and C) => A AND (B OR C)", f(expr))
			a, b, c = land.Right, land.Left, rand.Right
		case Equals.Expr(land.Right, rand.Right):
			change = newChange("(B and A) or (C and A) => A AND (B OR C)", f(expr))
			a, b, c = land.Right, land.Left, rand.Left
		default:
			return expr, noChange{}
		}
		return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, change
	case lok:
		// (<> AND <>) OR <>
		// Simplification
		if Equals.Expr(or.Right, land.Left) || Equals.Expr(or.Right, land.Right) {
			return or.Right, newChange("(A AND B) OR A => A", f(expr))
		}
		// Distribution Law
		return &AndExpr{Left: &OrExpr{Left: land.Left, Right: or.Right}, Right: &OrExpr{Left: land.Right, Right: or.Right}},
			newChange("(A AND B) OR C => (A OR C) AND (B OR C)", f(expr))
	case rok:
		// <> OR (<> AND <>)
		// Simplification
		if Equals.Expr(or.Left, rand.Left) || Equals.Expr(or.Left, rand.Right) {
			return or.Left, newChange("A OR (A AND B) => A", f(expr))
		}
		// Distribution Law
		return &AndExpr{
				Left:  &OrExpr{Left: or.Left, Right: rand.Left},
				Right: &OrExpr{Left: or.Left, Right: rand.Right},
			},
			newChange("C OR (A AND B) => (C OR A) AND (C OR B)", f(expr))
	}

	// next, we want to try to turn multiple ORs into an IN when possible
	lftCmp, lok := or.Left.(*ComparisonExpr)
	rgtCmp, rok := or.Right.(*ComparisonExpr)
	if lok && rok {
		newExpr, rewritten := tryTurningOrIntoIn(lftCmp, rgtCmp)
		if rewritten.changed() {
			return newExpr, rewritten
		}
	}

	// Try to make distinct
	return distinctOr(expr)
}

func tryTurningOrIntoIn(l, r *ComparisonExpr) (Expr, rewriteState) {
	// looks for A = X OR A = Y and turns them into A IN (X, Y)
	col, ok := l.Left.(*ColName)
	if !ok || !Equals.Expr(col, r.Left) {
		return nil, noChange{}
	}

	var tuple ValTuple
	var ruleStr string
	switch l.Operator {
	case EqualOp:
		tuple = ValTuple{l.Right}
		ruleStr = "A = <>"
	case InOp:
		lft, ok := l.Right.(ValTuple)
		if !ok {
			return nil, noChange{}
		}
		tuple = lft
		ruleStr = "A IN (<>, <>)"
	default:
		return nil, noChange{}
	}

	ruleStr += " OR "

	switch r.Operator {
	case EqualOp:
		tuple = append(tuple, r.Right)
		ruleStr += "A = <>"
	case InOp:
		lft, ok := r.Right.(ValTuple)
		if !ok {
			return nil, noChange{}
		}
		tuple = append(tuple, lft...)
		ruleStr += "A IN (<>, <>)"
	default:
		return nil, noChange{}
	}

	ruleStr += " => A IN (<>, <>)"

	return &ComparisonExpr{
		Operator: InOp,
		Left:     col,
		Right:    uniquefy(tuple),
	}, newChange(ruleStr, f(&OrExpr{Left: l, Right: r}))
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

func simplifyXor(expr *XorExpr) (Expr, rewriteState) {
	// DeMorgan Rewriter
	return &AndExpr{
		Left:  &OrExpr{Left: expr.Left, Right: expr.Right},
		Right: &NotExpr{Expr: &AndExpr{Left: expr.Left, Right: expr.Right}},
	}, newChange("(A XOR B) => (A OR B) AND NOT (A AND B)", f(expr))
}

func simplifyAnd(expr *AndExpr) (Expr, rewriteState) {
	res, rewritten := distinctAnd(expr)
	if rewritten.changed() {
		return res, rewritten
	}
	and := expr
	if or, ok := and.Left.(*OrExpr); ok {
		// Simplification

		if Equals.Expr(or.Left, and.Right) {
			return and.Right, newChange("(A OR B) AND A => A", f(expr))
		}
		if Equals.Expr(or.Right, and.Right) {
			return and.Right, newChange("(A OR B) AND B => B", f(expr))
		}
	}
	if or, ok := and.Right.(*OrExpr); ok {
		// Simplification
		if Equals.Expr(or.Left, and.Left) {
			return and.Left, newChange("A AND (A OR B) => A", f(expr))
		}
		if Equals.Expr(or.Right, and.Left) {
			return and.Left, newChange("A AND (B OR A) => A", f(expr))
		}
	}

	return expr, noChange{}
}

func distinctOr(in *OrExpr) (Expr, rewriteState) {
	var skipped []*OrExpr
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
				if log.V(0) {
					skipped = append(skipped, &OrExpr{Left: alreadyIn, Right: curr})
				}
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if original == len(predicates) {
		return in, noChange{}
	}
	var result Expr
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &OrExpr{Left: result, Right: curr}
	}

	return result, newChange("A OR A => A", func() Expr {
		var result Expr
		for _, orExpr := range skipped {
			if result == nil {
				result = orExpr
				continue
			}

			result = &OrExpr{
				Left:  result,
				Right: orExpr,
			}
		}
		return result
	})
}

func distinctAnd(in *AndExpr) (Expr, rewriteState) {
	var skipped []*AndExpr
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
	original := len(leaves)
	var predicates []Expr

outer1:
	for _, curr := range leaves {
		for _, alreadyIn := range predicates {
			if Equals.Expr(alreadyIn, curr) {
				if log.V(0) {
					skipped = append(skipped, &AndExpr{Left: alreadyIn, Right: curr})
				}
				continue outer1
			}
		}
		predicates = append(predicates, curr)
	}
	if original == len(predicates) {
		return in, noChange{}
	}
	var result Expr
	for i, curr := range predicates {
		if i == 0 {
			result = curr
			continue
		}
		result = &AndExpr{Left: result, Right: curr}
	}
	return AndExpressions(leaves...), newChange("A AND A => A", func() Expr {
		var result Expr
		for _, andExpr := range skipped {
			if result == nil {
				result = andExpr
				continue
			}

			result = &AndExpr{
				Left:  result,
				Right: andExpr,
			}
		}
		return result
	})
}

type (
	rewriteState interface {
		changed() bool
	}
	noChange struct{}

	// changed makes it possible to make sure we have a rule string for each change we do in the expression tree
	changed struct {
		rule string

		// ExprMatched is a function here so building of this expression can be paid only when we are debug logging
		exprMatched func() Expr
	}
)

func (noChange) changed() bool { return false }
func (changed) changed() bool  { return true }

// f returns a function that returns the expression. It's short by design, so it interferes minimally
// used for logging
func f(e Expr) func() Expr {
	return func() Expr { return e }
}

func printRule(rule string, expr func() Expr) {
	if log.V(10) {
		log.Infof("Rule: %s   ON   %s", rule, String(expr()))
	}
}

func printExpr(expr SQLNode) {
	if log.V(10) {
		log.Infof("Current: %s", String(expr))
	}
}

func newChange(rule string, exprMatched func() Expr) changed {
	return changed{
		rule:        rule,
		exprMatched: exprMatched,
	}
}
