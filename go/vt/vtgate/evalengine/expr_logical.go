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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	opLogical interface {
		String() string
		eval(left, right IR, env *ExpressionEnv) (boolean, error)
		compileLeft(c *compiler) *jump
		compileRight(c *compiler)
	}

	opLogicalAnd struct{}
	opLogicalOr  struct{}
	opLogicalXor struct{}

	LogicalExpr struct {
		BinaryExpr
		op opLogical
	}

	NotExpr struct {
		UnaryExpr
	}

	boolean int8

	IntervalExpr struct {
		CallExpr
	}

	// IsExpr represents the IS expression in MySQL.
	// boolean_primary IS [NOT] {TRUE | FALSE | NULL}
	IsExpr struct {
		UnaryExpr
		Op    sqlparser.IsExprOperator
		Check func(eval) bool
	}

	WhenThen struct {
		when IR
		then IR
	}

	CaseExpr struct {
		cases []WhenThen
		Else  IR
	}
)

var _ IR = (*IntervalExpr)(nil)
var _ IR = (*IsExpr)(nil)
var _ IR = (*LogicalExpr)(nil)
var _ IR = (*NotExpr)(nil)

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

func (opLogicalAnd) String() string {
	return "and"
}

func (opLogicalAnd) eval(le, re IR, env *ExpressionEnv) (boolean, error) {
	// Logical AND.
	// Evaluates to 1 if all operands are nonzero and not NULL, to 0 if one or more operands are 0, otherwise NULL is returned.
	l, err := le.eval(env)
	if err != nil {
		return boolNULL, err
	}

	left := evalIsTruthy(l)
	if left == boolFalse {
		return boolFalse, nil
	}

	r, err := re.eval(env)
	if err != nil {
		return boolNULL, err
	}
	right := evalIsTruthy(r)

	switch {
	case left == boolTrue && right == boolTrue:
		return boolTrue, nil
	case right == boolFalse:
		return boolFalse, nil
	default:
		return boolNULL, nil
	}
}

func (op opLogicalAnd) compileLeft(c *compiler) *jump {
	j := c.asm.jumpFrom()
	c.asm.emit(func(env *ExpressionEnv) int {
		left, ok := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if ok && left.i == 0 {
			return j.offset()
		}
		return 1
	}, "AND CHECK INT64(SP-1)")
	return j
}

func (op opLogicalAnd) compileRight(c *compiler) {
	c.asm.adjustStack(-1)
	c.asm.emit(func(env *ExpressionEnv) int {
		left, lok := env.vm.stack[env.vm.sp-2].(*evalInt64)
		right, rok := env.vm.stack[env.vm.sp-1].(*evalInt64)

		isLeft := lok && left.i != 0
		isRight := rok && right.i != 0

		if isLeft && isRight {
			left.i = 1
		} else if rok && !isRight {
			env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalBool(false)
		} else {
			env.vm.stack[env.vm.sp-2] = nil
		}
		env.vm.sp--
		return 1
	}, "AND INT64(SP-2), INT64(SP-1)")
}

func (opLogicalOr) String() string {
	return "or"
}

func (opLogicalOr) eval(le, re IR, env *ExpressionEnv) (boolean, error) {
	// Logical OR. When both operands are non-NULL, the result is 1 if any operand is nonzero, and 0 otherwise.
	// With a NULL operand, the result is 1 if the other operand is nonzero, and NULL otherwise.
	// If both operands are NULL, the result is NULL.
	l, err := le.eval(env)
	if err != nil {
		return boolNULL, err
	}

	left := evalIsTruthy(l)
	if left == boolTrue {
		return boolTrue, nil
	}

	r, err := re.eval(env)
	if err != nil {
		return boolNULL, err
	}
	right := evalIsTruthy(r)

	switch {
	case left == boolNULL:
		if right == boolTrue {
			return boolTrue, nil
		}
		return boolNULL, nil

	case right == boolNULL:
		return boolNULL, nil

	default:
		if right == boolTrue {
			return boolTrue, nil
		}
		return boolFalse, nil
	}
}

func (opLogicalOr) compileLeft(c *compiler) *jump {
	j := c.asm.jumpFrom()
	c.asm.emit(func(env *ExpressionEnv) int {
		left, ok := env.vm.stack[env.vm.sp-1].(*evalInt64)
		if ok && left.i != 0 {
			left.i = 1
			return j.offset()
		}
		return 1
	}, "OR CHECK INT64(SP-1)")
	return j
}

func (opLogicalOr) compileRight(c *compiler) {
	c.asm.adjustStack(-1)
	c.asm.emit(func(env *ExpressionEnv) int {
		left, lok := env.vm.stack[env.vm.sp-2].(*evalInt64)
		right, rok := env.vm.stack[env.vm.sp-1].(*evalInt64)

		isLeft := lok && left.i != 0
		isRight := rok && right.i != 0

		switch {
		case !lok:
			if isRight {
				env.vm.stack[env.vm.sp-2] = env.vm.arena.newEvalBool(true)
			}
		case !rok:
			env.vm.stack[env.vm.sp-2] = nil
		default:
			if isLeft || isRight {
				left.i = 1
			} else {
				left.i = 0
			}
		}
		env.vm.sp--
		return 1
	}, "OR INT64(SP-2), INT64(SP-1)")
}

func (opLogicalXor) String() string {
	return "xor"
}

func (opLogicalXor) eval(le, re IR, env *ExpressionEnv) (boolean, error) {
	// Logical XOR. Returns NULL if either operand is NULL.
	// For non-NULL operands, evaluates to 1 if an odd number of operands is nonzero, otherwise 0 is returned.
	l, err := le.eval(env)
	if err != nil {
		return boolNULL, err
	}

	left := evalIsTruthy(l)
	if left == boolNULL {
		return boolNULL, nil
	}

	r, err := re.eval(env)
	if err != nil {
		return boolNULL, err
	}
	right := evalIsTruthy(r)

	switch {
	case left == boolNULL || right == boolNULL:
		return boolNULL, nil
	default:
		if left != right {
			return boolTrue, nil
		}
		return boolFalse, nil
	}
}

func (opLogicalXor) compileLeft(c *compiler) *jump {
	j := c.asm.jumpFrom()
	c.asm.emit(func(env *ExpressionEnv) int {
		if env.vm.stack[env.vm.sp-1] == nil {
			return j.offset()
		}
		return 1
	}, "XOR CHECK INT64(SP-1)")
	return j
}

func (opLogicalXor) compileRight(c *compiler) {
	c.asm.adjustStack(-1)
	c.asm.emit(func(env *ExpressionEnv) int {
		left := env.vm.stack[env.vm.sp-2].(*evalInt64)
		right, rok := env.vm.stack[env.vm.sp-1].(*evalInt64)

		isLeft := left.i != 0
		isRight := rok && right.i != 0

		switch {
		case !rok:
			env.vm.stack[env.vm.sp-2] = nil
		default:
			if isLeft != isRight {
				left.i = 1
			} else {
				left.i = 0
			}
		}
		env.vm.sp--
		return 1
	}, "XOR INT64(SP-2), INT64(SP-1)")
}

func (n *NotExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := n.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	return evalIsTruthy(e).not().eval(), nil
}

func (expr *NotExpr) compile(c *compiler) (ctype, error) {
	arg, err := expr.Inner.compile(c)
	if err != nil {
		return ctype{}, nil
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Null:
		// No-op.
	case sqltypes.Int64:
		c.asm.Not_i()
	case sqltypes.Uint64:
		c.asm.Not_u()
	case sqltypes.Float64:
		c.asm.Not_f()
	case sqltypes.Decimal:
		c.asm.Not_d()
	case sqltypes.VarChar, sqltypes.VarBinary:
		if arg.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Not_u()
		} else {
			c.asm.Convert_bB(1)
			c.asm.Not_i()
		}
	case sqltypes.TypeJSON:
		c.asm.Convert_jB(1)
		c.asm.Not_i()
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp:
		c.asm.Convert_TB(1)
		c.asm.Not_i()
	default:
		c.asm.Convert_bB(1)
		c.asm.Not_i()
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(arg.Flag) | flagIsBoolean, Col: collationNumeric}, nil
}

func (l *LogicalExpr) eval(env *ExpressionEnv) (eval, error) {
	res, err := l.op.eval(l.Left, l.Right, env)
	return res.eval(), err
}

func (expr *LogicalExpr) compile(c *compiler) (ctype, error) {
	lt, err := expr.Left.compile(c)
	if err != nil {
		return ctype{}, err
	}

	switch lt.Type {
	case sqltypes.Null, sqltypes.Int64:
		// No-op.
	case sqltypes.Uint64:
		c.asm.Convert_uB(1)
	case sqltypes.Float64:
		c.asm.Convert_fB(1)
	case sqltypes.Decimal:
		c.asm.Convert_dB(1)
	case sqltypes.VarChar, sqltypes.VarBinary:
		if lt.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Convert_uB(1)
		} else {
			c.asm.Convert_bB(1)
		}
	case sqltypes.TypeJSON:
		c.asm.Convert_jB(1)
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp:
		c.asm.Convert_TB(1)
	default:
		c.asm.Convert_bB(1)
	}

	jump := expr.op.compileLeft(c)

	rt, err := expr.Right.compile(c)
	if err != nil {
		return ctype{}, err
	}

	switch rt.Type {
	case sqltypes.Null, sqltypes.Int64:
		// No-op.
	case sqltypes.Uint64:
		c.asm.Convert_uB(1)
	case sqltypes.Float64:
		c.asm.Convert_fB(1)
	case sqltypes.Decimal:
		c.asm.Convert_dB(1)
	case sqltypes.VarChar, sqltypes.VarBinary:
		if rt.isHexOrBitLiteral() {
			c.asm.Convert_xu(1)
			c.asm.Convert_uB(1)
		} else {
			c.asm.Convert_bB(1)
		}
	case sqltypes.TypeJSON:
		c.asm.Convert_jB(1)
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp:
		c.asm.Convert_TB(1)
	default:
		c.asm.Convert_bB(1)
	}

	expr.op.compileRight(c)
	c.asm.jumpDestination(jump)
	return ctype{Type: sqltypes.Int64, Flag: ((lt.Flag | rt.Flag) & flagNullable) | flagIsBoolean, Col: collationNumeric}, nil
}

func intervalCompare(n, val eval) (int, bool, error) {
	if val == nil {
		return 1, true, nil
	}

	val = evalToNumeric(val, false)
	cmp, err := compareNumeric(n, val)
	return cmp, false, err
}

func findInterval(args []eval) (int64, error) {
	n := args[0]
	start := int64(1)
	end := int64(len(args) - 1)
	for {
		if start > end {
			return end, nil
		}

		val := args[start]
		cmp, _, err := intervalCompare(n, val)
		if err != nil {
			return 0, err
		}

		if cmp < 0 {
			return start - 1, nil
		}

		pos := start + (end-start)/2

		val = args[pos]
		cmp, null, err := intervalCompare(n, val)
		if err != nil {
			return 0, err
		}

		prevPos := pos
		for null {
			prevPos--
			if prevPos < start {
				break
			}
			prevVal := args[prevPos]
			cmp, null, err = intervalCompare(n, prevVal)
			if err != nil {
				return 0, err
			}
		}

		if cmp < 0 {
			end = pos - 1
		} else {
			start = pos + 1
		}
	}
}

func (i *IntervalExpr) eval(env *ExpressionEnv) (eval, error) {
	args := make([]eval, 0, len(i.Arguments))
	for _, arg := range i.Arguments {
		val, err := arg.eval(env)
		if err != nil {
			return nil, err
		}
		args = append(args, val)
	}

	if args[0] == nil {
		return newEvalInt64(-1), nil
	}

	args[0] = evalToNumeric(args[0], false)

	idx, err := findInterval(args)
	if err != nil {
		return nil, err
	}
	return newEvalInt64(idx), err
}

func (i *IntervalExpr) compile(c *compiler) (ctype, error) {
	n, err := i.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	switch n.Type {
	case sqltypes.Int64, sqltypes.Uint64, sqltypes.Float64, sqltypes.Decimal:
	default:
		s := c.compileNullCheck1(n)
		c.asm.Convert_xf(1)
		c.asm.jumpDestination(s)
	}

	for j := 1; j < len(i.Arguments); j++ {
		argType, err := i.Arguments[j].compile(c)
		if err != nil {
			return ctype{}, err
		}
		switch argType.Type {
		case sqltypes.Int64, sqltypes.Uint64, sqltypes.Float64, sqltypes.Decimal:
		default:
			s := c.compileNullCheck1(argType)
			c.asm.Convert_xf(1)
			c.asm.jumpDestination(s)
		}
	}

	c.asm.Interval(len(i.Arguments) - 1)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric}, nil
}

func (i *IsExpr) eval(env *ExpressionEnv) (eval, error) {
	e, err := i.Inner.eval(env)
	if err != nil {
		return nil, err
	}
	return newEvalBool(i.Check(e)), nil
}

func (is *IsExpr) compile(c *compiler) (ctype, error) {
	_, err := is.Inner.compile(c)
	if err != nil {
		return ctype{}, err
	}
	c.asm.Is(is.Check)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: flagIsBoolean}, nil
}

func (c *CaseExpr) eval(env *ExpressionEnv) (eval, error) {
	var ta typeAggregation
	var ca collationAggregation
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
		ta.addEval(then)
		if err := ca.add(evalCollation(then), env.collationEnv); err != nil {
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
		ta.addEval(e)
		if err := ca.add(evalCollation(e), env.collationEnv); err != nil {
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
	return evalCoerce(result, ta.result(), ca.result().Collation, env.now, env.sqlmode.AllowZeroDate())
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

func (cs *CaseExpr) compile(c *compiler) (ctype, error) {
	var ca collationAggregation
	var ta typeAggregation

	for _, wt := range cs.cases {
		when, err := wt.when.compile(c)
		if err != nil {
			return ctype{}, err
		}

		if err := c.compileCheckTrue(when, 1); err != nil {
			return ctype{}, err
		}

		then, err := wt.then.compile(c)
		if err != nil {
			return ctype{}, err
		}

		ta.add(then.Type, then.Flag)
		if err := ca.add(then.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
	}

	if cs.Else != nil {
		els, err := cs.Else.compile(c)
		if err != nil {
			return ctype{}, err
		}

		ta.add(els.Type, els.Flag)
		if err := ca.add(els.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
	}

	var f typeFlag
	if ta.nullable {
		f |= flagNullable
	}
	ct := ctype{Type: ta.result(), Flag: f, Col: ca.result()}
	c.asm.CmpCase(len(cs.cases), cs.Else != nil, ct.Type, ct.Col, c.sqlmode.AllowZeroDate())
	return ct, nil
}

var _ IR = (*CaseExpr)(nil)
