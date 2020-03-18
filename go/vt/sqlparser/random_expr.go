/*
Copyright 2020 The Vitess Authors.

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
	"math/rand"
)

// This file is used to generate random expressions to be used for testing

type generator struct {
	seed  int64
	r     *rand.Rand
	depth int
}

// enter should be called whenever we are producing an intermediate node. it should be followed by a `defer g.exit()`
func (g *generator) enter() {
	g.depth++
}

// exit should be called when exiting an intermediate node
func (g *generator) exit() {
	g.depth--
}

// atMaxDepth returns true if we have reached the maximum allowed depth or the expression tree
func (g *generator) atMaxDepth() bool {
	return g.depth >= 3
}

func (g *generator) expression() Expr {
	if g.randomBool() {
		return g.booleanExpr()
	}

	return g.intExpr()
}

func (g *generator) booleanExpr() Expr {
	if g.atMaxDepth() {
		return g.booleanLiteral()
	}

	options := []exprF{
		func() Expr { return g.andExpr() },
		func() Expr { return g.orExpr() },
		func() Expr { return g.booleanLiteral() },
		func() Expr { return g.comparison() },
		func() Expr { return g.inExpr() },
		func() Expr { return g.between() },
		func() Expr { return g.isExpr() },
	}

	return g.randomOf(options)
}

func (g *generator) intExpr() Expr {
	if g.atMaxDepth() {
		return g.intLiteral()
	}

	options := []exprF{
		func() Expr { return g.arithmetic() },
		func() Expr { return g.intLiteral() },
		func() Expr { return g.caseExpr() },
	}

	return g.randomOf(options)
}

func (g *generator) booleanLiteral() Expr {
	return BoolVal(g.randomBool())
}

func (g *generator) randomBool() bool {
	return g.r.Float32() < 0.5
}

func (g *generator) intLiteral() Expr {
	t := fmt.Sprintf("%d", g.r.Intn(1000)-g.r.Intn((1000)))

	return NewIntVal([]byte(t))
}

var comparisonOps = []string{"=", ">", "<", ">=", "<=", "<=>", "!="}

func (g *generator) comparison() Expr {
	g.enter()
	defer g.exit()

	v := g.r.Intn(len(comparisonOps))

	cmp := &ComparisonExpr{
		Operator: comparisonOps[v],
		Left:     g.intExpr(),
		Right:    g.intExpr(),
	}

	return cmp
}

func (g *generator) caseExpr() Expr {
	g.enter()
	defer g.exit()

	var exp Expr
	var elseExpr Expr
	if g.randomBool() {
		exp = g.intExpr()
	}
	if g.randomBool() {
		elseExpr = g.intExpr()
	}

	size := g.r.Intn(5) + 2
	var whens []*When
	for i := 0; i < size; i++ {
		var cond Expr
		if exp == nil {
			cond = g.booleanExpr()
		} else {
			cond = g.expression()
		}

		whens = append(whens, &When{
			Cond: cond,
			Val:  g.expression(),
		})
	}

	return &CaseExpr{
		Expr:  exp,
		Whens: whens,
		Else:  elseExpr,
	}
}

var arithmeticOps = []string{BitAndStr, BitOrStr, BitXorStr, PlusStr, MinusStr, MultStr, DivStr, IntDivStr, ModStr, ShiftRightStr, ShiftLeftStr}

func (g *generator) arithmetic() Expr {
	g.enter()
	defer g.exit()

	op := arithmeticOps[g.r.Intn(len(arithmeticOps))]

	return &BinaryExpr{
		Operator: op,
		Left:     g.intExpr(),
		Right:    g.intExpr(),
	}
}

type exprF func() Expr

func (g *generator) randomOf(options []exprF) Expr {
	return options[g.r.Intn(len(options))]()
}

func (g *generator) randomOfS(options []string) string {
	return options[g.r.Intn(len(options))]
}

func (g *generator) andExpr() Expr {
	g.enter()
	defer g.exit()
	return &AndExpr{
		Left:  g.booleanExpr(),
		Right: g.booleanExpr(),
	}
}

func (g *generator) orExpr() Expr {
	g.enter()
	defer g.exit()
	return &OrExpr{
		Left:  g.booleanExpr(),
		Right: g.booleanExpr(),
	}
}

func (g *generator) inExpr() Expr {
	g.enter()
	defer g.exit()

	expr := g.intExpr()
	size := g.r.Intn(5) + 2
	tuples := ValTuple{}
	for i := 0; i < size; i++ {
		tuples = append(tuples, g.intExpr())
	}
	op := InStr
	if g.randomBool() {
		op = NotInStr
	}

	return &ComparisonExpr{
		Operator: op,
		Left:     expr,
		Right:    tuples,
	}
}

func (g *generator) between() Expr {
	g.enter()
	defer g.exit()

	return &RangeCond{
		Operator: BetweenStr,
		Left:     g.intExpr(),
		From:     g.intExpr(),
		To:       g.intExpr(),
	}
}

func (g *generator) isExpr() Expr {
	g.enter()
	defer g.exit()

	ops := []string{IsNullStr, IsNotNullStr, IsTrueStr, IsNotTrueStr, IsFalseStr, IsNotFalseStr}

	return &IsExpr{
		Operator: g.randomOfS(ops),
		Expr:     g.booleanExpr(),
	}
}
