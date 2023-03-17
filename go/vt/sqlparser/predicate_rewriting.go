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
	"log"
)

const (
	Changed  RewriteState = true
	NoChange RewriteState = false
)

type RewriteState bool

var logger *log.Logger

// Set☝️to something not nil to get debug output
// var logger *log.Logger = log.New(os.Stdout, "", 0)

func f(e Expr) func() Expr {
	return func() Expr {
		return e
	}
}

func printRule(rule string, expr func() Expr) {
	if logger == nil {
		return
	}
	logger.Printf("Rule: %s   ON   %s", rule, String(expr()))
}

func printExpr(expr SQLNode) {
	if logger == nil {
		return
	}
	logger.Printf("Current: %s", String(expr))
}

// RewritePredicate walks the input AST and rewrites any boolean logic into a simpler form
// This simpler form is CNF plus logic for extracting predicates from OR, plus logic for turning ORs into IN
// Note: In order to re-plan, we need to empty the accumulated metadata in the AST,
// so ColName.Metadata will be nil:ed out as part of this rewrite
func RewritePredicate(ast SQLNode) SQLNode {
	for {
		printExpr(ast)
		changed := false
		stopOnChange := func(SQLNode, SQLNode) bool {
			return !changed
		}
		ast = SafeRewrite(ast, stopOnChange, func(cursor *Cursor) bool {
			e, isExpr := cursor.node.(Expr)
			if !isExpr {
				return true
			}

			rewritten, state := simplifyExpression(e)
			if state == Changed {
				changed = true
				cursor.Replace(rewritten)
			}

			if col, isCol := cursor.node.(*ColName); isCol {
				col.Metadata = nil
			}
			return !changed
		})

		if !changed {
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
		printRule("NOT NOT A => A", f(expr))
		return child.Expr, Changed
	case *OrExpr:
		printRule("NOT (A OR B) => NOT A AND NOT B", f(expr))
		return &AndExpr{Right: &NotExpr{Expr: child.Right}, Left: &NotExpr{Expr: child.Left}}, Changed
	case *AndExpr:
		printRule("NOT (A AND B) => NOT A OR NOT B", f(expr))
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

	return uniquefy(ins)
}

func simplifyOr(expr *OrExpr) (Expr, RewriteState) {
	or := expr

	// first we search for ANDs and see how they can be simplified
	land, lok := or.Left.(*AndExpr)
	rand, rok := or.Right.(*AndExpr)
	switch {
	case lok && rok:
		// (<> AND <>) OR (<> AND <>)
		var a, b, c Expr
		switch {
		case Equals.Expr(land.Left, rand.Left):
			printRule("(A and B) or (A and C) => A AND (B OR C)", f(expr))
			a, b, c = land.Left, land.Right, rand.Right
		case Equals.Expr(land.Left, rand.Right):
			printRule("(A and B) or (C and A) => A AND (B OR C)", f(expr))
			a, b, c = land.Left, land.Right, rand.Left
		case Equals.Expr(land.Right, rand.Left):
			printRule("(B and A) or (A and C) => A AND (B OR C)", f(expr))
			a, b, c = land.Right, land.Left, rand.Right
		case Equals.Expr(land.Right, rand.Right):
			printRule("(B and A) or (C and A) => A AND (B OR C)", f(expr))
			a, b, c = land.Right, land.Left, rand.Left
		default:
			return expr, NoChange
		}
		return &AndExpr{Left: a, Right: &OrExpr{Left: b, Right: c}}, Changed
	case lok:
		// (<> AND <>) OR <>
		// Simplification
		if Equals.Expr(or.Right, land.Left) || Equals.Expr(or.Right, land.Right) {
			printRule("(A AND B) OR A => A", f(expr))
			return or.Right, Changed
		}
		// Distribution Law
		printRule("(A AND B) OR C => (A OR C) AND (B OR C)", f(expr))
		return &AndExpr{Left: &OrExpr{Left: land.Left, Right: or.Right}, Right: &OrExpr{Left: land.Right, Right: or.Right}}, Changed
	case rok:
		// <> OR (<> AND <>)
		// Simplification
		if Equals.Expr(or.Left, rand.Left) || Equals.Expr(or.Left, rand.Right) {
			printRule("A OR (A AND B) => A", f(expr))
			return or.Left, Changed
		}
		// Distribution Law
		printRule("C OR (A AND B) => (C OR A) AND (C OR B)", f(expr))
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
	var ruleStr string
	switch l.Operator {
	case EqualOp:
		tuple = ValTuple{l.Right}
		ruleStr = "A = <>"
	case InOp:
		lft, ok := l.Right.(ValTuple)
		if !ok {
			return nil, NoChange
		}
		tuple = lft
		ruleStr = "A IN (<>, <>)"
	default:
		return nil, NoChange
	}

	ruleStr += " OR "

	switch r.Operator {
	case EqualOp:
		tuple = append(tuple, r.Right)
		ruleStr += "A = <>"
	case InOp:
		lft, ok := r.Right.(ValTuple)
		if !ok {
			return nil, NoChange
		}
		tuple = append(tuple, lft...)
		ruleStr += "A IN (<>, <>)"
	default:
		return nil, NoChange
	}

	ruleStr += " => A IN (<>, <>)"

	printRule(ruleStr, f(&OrExpr{Left: l, Right: r}))
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
	printRule("(A XOR B) => (A OR B) AND NOT (A AND B)", f(expr))
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

		if Equals.Expr(or.Left, and.Right) {
			printRule("(A OR B) AND A => A", f(expr))
			return and.Right, Changed
		}
		if Equals.Expr(or.Right, and.Right) {
			printRule("(A OR B) AND B => B", f(expr))
			return and.Right, Changed
		}
	}
	if or, ok := and.Right.(*OrExpr); ok {
		// Simplification
		if Equals.Expr(or.Left, and.Left) {
			printRule("A AND (A OR B) => A", f(expr))
			return and.Left, Changed
		}
		if Equals.Expr(or.Right, and.Left) {
			printRule("A AND (B OR A) => A", f(expr))
			return and.Left, Changed
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
				printRule("A OR A => A", f(&AndExpr{Left: alreadyIn, Right: curr}))
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
				printRule("A AND A => A", f(&AndExpr{Left: alreadyIn, Right: curr}))
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
	return AndExpressions(leaves...), Changed
}
