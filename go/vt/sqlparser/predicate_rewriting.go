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

// RewriteToCNF walks the input AST and rewrites any boolean logic into CNF
// Note: In order to re-plan, we need to empty the accumulated metadata in the AST,
// so ColName.Metadata will be nil:ed out as part of this rewrite
func RewriteToCNF(ast SQLNode) SQLNode {
	for {
		finishedRewrite := true
		ast = Rewrite(ast, func(cursor *Cursor) bool {
			if e, isExpr := cursor.node.(Expr); isExpr {
				rewritten, didRewrite := rewriteToCNFExpr(e)
				if didRewrite {
					finishedRewrite = false
					cursor.Replace(rewritten)
				}
			}
			if col, isCol := cursor.node.(*ColName); isCol {
				col.Metadata = nil
			}
			return true
		}, nil)

		if finishedRewrite {
			return ast
		}
	}
}

func rewriteToCNFExpr(expr Expr) (Expr, bool) {
	switch expr := expr.(type) {
	case *NotExpr:
		simplifyNot(expr)
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

	// Try to make distinct
	return distinctOr(expr)
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

func TryRewriteOrToIn(expr Expr) Expr {
	rewrote := false
	newPred := Rewrite(CloneExpr(expr), func(cursor *Cursor) bool {
		_, ok := cursor.Node().(*OrExpr)
		return ok
	}, func(cursor *Cursor) bool {
		// we are looking for the pattern WHERE c = 1 or c = 2
		switch or := cursor.Node().(type) {
		case *OrExpr:
			lftCmp, ok := or.Left.(*ComparisonExpr)
			if !ok {
				return true
			}
			rgtCmp, ok := or.Right.(*ComparisonExpr)
			if !ok {
				return true
			}

			col, ok := lftCmp.Left.(*ColName)
			if !ok || !EqualsExpr(lftCmp.Left, rgtCmp.Left) {
				return true
			}

			var tuple ValTuple
			switch lftCmp.Operator {
			case EqualOp:
				tuple = ValTuple{lftCmp.Right}
			case InOp:
				lft, ok := lftCmp.Right.(ValTuple)
				if !ok {
					return true
				}
				tuple = lft
			default:
				return true
			}

			switch rgtCmp.Operator {
			case EqualOp:
				tuple = append(tuple, rgtCmp.Right)
			case InOp:
				lft, ok := rgtCmp.Right.(ValTuple)
				if !ok {
					return true
				}
				tuple = append(tuple, lft...)
			default:
				return true
			}

			rewrote = true
			cursor.Replace(&ComparisonExpr{
				Operator: InOp,
				Left:     col,
				Right:    tuple,
			})
		}
		return true
	})
	if rewrote {
		return newPred.(Expr)
	}
	return nil
}
