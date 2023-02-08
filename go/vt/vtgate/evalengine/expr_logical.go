/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	LogicalExpr struct {
		BinaryExpr
		op     func(left, right boolean) boolean
		opname string
	}

	NotExpr struct {
		UnaryExpr
	}

	boolean int8

	// IsExpr represents the IS expression in MySQL.
	// boolean_primary IS [NOT] {TRUE | FALSE | NULL}
	IsExpr struct {
		UnaryExpr
		Op    sqlparser.IsExprOperator
		Check func(eval) bool
	}

	WhenThen struct {
		when Expr
		then Expr
	}

	CaseExpr struct {
		cases []WhenThen
		Else  Expr
	}
)

var _ Expr = (*IsExpr)(nil)
var _ Expr = (*LogicalExpr)(nil)
var _ Expr = (*NotExpr)(nil)

const (
	boolFalse boolean = 0
	boolTrue  boolean = 1
	boolNULL  boolean = -1
)

func makeboolean(b bool) boolean {
	if b {
		return boolTrue
	}
	return boolFalse
}

func makeboolean2(b, isNull bool) boolean {
	if isNull {
		return boolNULL
	}
	return makeboolean(b)
}

func (left boolean) eval() eval {
	switch left {
	case boolTrue:
		return evalBoolTrue
	case boolFalse:
		return evalBoolFalse
	default:
		return nil
	}
}

func (left boolean) not() boolean {
	switch left {
	case boolFalse:
		return boolTrue
	case boolTrue:
		return boolFalse
	default:
		return left
	}
}

func (left boolean) and(right boolean) boolean {
	// Logical AND.
	// Evaluates to 1 if all operands are nonzero and not NULL, to 0 if one or more operands are 0, otherwise NULL is returned.
	switch {
	case left == boolTrue && right == boolTrue:
		return boolTrue
	case left == boolFalse || right == boolFalse:
		return boolFalse
	default:
		return boolNULL
	}
}

func (left boolean) or(right boolean) boolean {
	// Logical OR. When both operands are non-NULL, the result is 1 if any operand is nonzero, and 0 otherwise.
	// With a NULL operand, the result is 1 if the other operand is nonzero, and NULL otherwise.
	// If both operands are NULL, the result is NULL.
	switch {
	case left == boolNULL:
		if right == boolTrue {
			return boolTrue
		}
		return boolNULL

	case right == boolNULL:
		if left == boolTrue {
			return boolTrue
		}
		return boolNULL

	default:
		if left == boolTrue || right == boolTrue {
			return boolTrue
		}
		return boolFalse
	}
}

func (left boolean) xor(right boolean) boolean {
	// Logical XOR. Returns NULL if either operand is NULL.
	// For non-NULL operands, evaluates to 1 if an odd number of operands is nonzero, otherwise 0 is returned.
	switch {
	case left == boolNULL || right == boolNULL:
		return boolNULL
	default:
		if left != right {
			return boolTrue
		}
		return boolFalse
	}
}

func (n *NotExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := n.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	return evalIsTruthy(e).not().eval(), nil
}

func (n *NotExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, flags := n.Inner.typeof(env)
	return sqltypes.Uint64, flags
}

func (l *LogicalExpr) eval(env *ExpressionEnv) (eval, error) {
	left, right, err := l.arguments(env)
	if err != nil {
		return nil, err
	}
	return l.op(evalIsTruthy(left), evalIsTruthy(right)).eval(), nil
}

func (l *LogicalExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f1 := l.Left.typeof(env)
	_, f2 := l.Right.typeof(env)
	return sqltypes.Uint64, f1 | f2
}

func (i *IsExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := i.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	return newEvalBool(i.Check(e)), nil
}

func (i *IsExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, 0
}

func (c *CaseExpr) eval(env *ExpressionEnv) (eval, error) {
	var ca collationAggregation
	var local = collations.Local()
	var result eval
	var matched = false

	// From what we can tell, MySQL actually evaluates all the branches
	// of a CASE expression, even after a truthy match. I.e. the CASE
	// operator does _not_ short-circuit.

	for _, whenThen := range c.cases {
		when, err := whenThen.when.eval(env)
		if err != nil {
			return nil, err
		}
		truthy := evalIsTruthy(when) == boolTrue

		then, err := whenThen.then.eval(env)
		if err != nil {
			return nil, err
		}
		if err := ca.add(local, evalCollation(then)); err != nil {
			return nil, err
		}

		if !matched && truthy {
			result = then
			matched = true
		}
	}
	if c.Else != nil {
		e, err := c.Else.eval(env)
		if err != nil {
			return nil, err
		}
		if err := ca.add(local, evalCollation(e)); err != nil {
			return nil, err
		}
		if !matched {
			result = e
			matched = true
		}
	}

	if !matched {
		return nil, nil
	}
	t, _ := c.typeof(env)
	return evalCoerce(result, t, ca.result().Collation)
}

func (c *CaseExpr) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	var ta typeAggregation
	var resultFlag typeFlag

	for _, whenthen := range c.cases {
		t, f := whenthen.then.typeof(env)
		ta.add(t, f)
		resultFlag = resultFlag | f
	}
	if c.Else != nil {
		t, f := c.Else.typeof(env)
		ta.add(t, f)
		resultFlag = f
	}
	return ta.result(), resultFlag
}

func (c *CaseExpr) format(buf *formatter, depth int) {
	buf.WriteString("CASE")
	for _, cs := range c.cases {
		buf.WriteString(" WHEN ")
		cs.when.format(buf, depth)
		buf.WriteString(" THEN ")
		cs.then.format(buf, depth)
	}
	if c.Else != nil {
		buf.WriteString(" ELSE ")
		c.Else.format(buf, depth)
	}
}

func (c *CaseExpr) constant() bool {
	// TODO we should be able to simplify more cases than constant/simplify allows us to today
	// example: case when true then col end
	if c.Else != nil {
		if !c.Else.constant() {
			return false
		}
	}

	for _, then := range c.cases {
		if !then.when.constant() || !then.then.constant() {
			return false
		}
	}

	return true
}

func (c *CaseExpr) simplify(env *ExpressionEnv) error {
	var err error
	for i := range c.cases {
		whenThen := &c.cases[i]
		whenThen.when, err = simplifyExpr(env, whenThen.when)
		if err != nil {
			return err
		}
		whenThen.then, err = simplifyExpr(env, whenThen.then)
		if err != nil {
			return err
		}
	}
	if c.Else != nil {
		c.Else, err = simplifyExpr(env, c.Else)
	}
	return err
}

var _ Expr = (*CaseExpr)(nil)
