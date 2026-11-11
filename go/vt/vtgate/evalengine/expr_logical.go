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
	"vitess.io/vitess/go/vt/vtenv"
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
		cases   []WhenThen
		Else    IR
		collate collations.ID
	}
)

var (
	_ IR = (*IntervalExpr)(nil)
	_ IR = (*IsExpr)(nil)
	_ IR = (*LogicalExpr)(nil)
	_ IR = (*NotExpr)(nil)
)

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
	// Like MySQL, evaluate the WHEN conditions in order up to the first true one,
	// then only the THEN of that condition, or the ELSE if no condition is true.
	// The other conditions and branches are not evaluated: their errors and side
	// effects do not happen.
	taken := len(c.cases)
	for i, whenThen := range c.cases {
		when, err := whenThen.when.eval(env)
		if err != nil {
			return nil, err
		}
		if evalIsTruthy(when) == boolTrue {
			taken = i
			break
		}
	}

	var result eval
	var err error
	switch {
	case taken < len(c.cases):
		result, err = c.cases[taken].then.eval(env)
	case c.Else != nil:
		result, err = c.Else.eval(env)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// The type of the result is the aggregate of the types of all the branches,
	// whichever one is taken. The taken branch contributes the type of its value,
	// the others their declared type. A branch whose type cannot be known without
	// evaluating it contributes nothing.
	var ta typeAggregation
	var ca collationAggregation
	var declared []caseBranchType
	branches := c.branches()
	if branches > 1 {
		declared = c.declaredTypes(env)
	}
	for i := range branches {
		if i == taken {
			ta.addEval(result)
			if err := ca.add(evalCollation(result), env.collationEnv); err != nil {
				return nil, err
			}
			continue
		}
		d := declared[i]
		if !d.ok {
			continue
		}
		f := d.ct.Flag
		if d.ct.Type == sqltypes.Null {
			// A NULL makes the result nullable, as when it is evaluated.
			f |= flagNullable
		}
		ta.add(d.ct.Type, f, d.ct.Size, d.ct.Scale)
		if err := ca.add(d.ct.Col, env.collationEnv); err != nil {
			return nil, err
		}
	}

	tt := ta.result()
	return evalCoerce(result, tt, ta.size, ta.scale, ca.resultFor(tt, c.collate).Collation, env.now, env.sqlmode.AllowZeroDate())
}

// caseBranchType is the declared type of a branch of a CASE, if it is known.
type caseBranchType struct {
	ct ctype
	ok bool
}

// branches returns the number of THEN and ELSE branches.
func (c *CaseExpr) branches() int {
	if c.Else != nil {
		return len(c.cases) + 1
	}
	return len(c.cases)
}

// declaredTypes returns the declared type of every THEN branch, then of the ELSE,
// for the types of the current row and bind variables, without evaluating them.
// Inside an UntypedExpr they are computed once per set of input types.
func (c *CaseExpr) declaredTypes(env *ExpressionEnv) []caseBranchType {
	if u := env.untyped; u != nil {
		if typed, err := u.loadTypedExpression(env); err == nil {
			if types, ok := typed.caseTypes.Load(c); ok {
				return types.([]caseBranchType)
			}
			types := c.compileTypes(env, u.env)
			typed.caseTypes.Store(c, types)
			return types
		}
	}
	if env.vc == nil {
		return make([]caseBranchType, c.branches())
	}
	return c.compileTypes(env, env.vc.Environment())
}

func (c *CaseExpr) compileTypes(env *ExpressionEnv, venv *vtenv.Environment) []caseBranchType {
	types := make([]caseBranchType, 0, c.branches())
	add := func(expr IR) {
		ct, err := declaredType(env, venv, c.collate, expr)
		types = append(types, caseBranchType{ct: ct, ok: err == nil})
	}
	for _, whenThen := range c.cases {
		add(whenThen.then)
	}
	if c.Else != nil {
		add(c.Else)
	}
	return types
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
	// A condition or branch is only evaluated when it is needed, so one that fails
	// to fold is kept as it is: it fails when it is evaluated, as in MySQL.
	simplify := func(e IR) IR {
		if simplified, err := simplifyExpr(env, e); err == nil {
			return simplified
		}
		return e
	}
	for i := range c.cases {
		whenThen := &c.cases[i]
		whenThen.when = simplify(whenThen.when)
		whenThen.then = simplify(whenThen.then)
	}
	if c.Else != nil {
		c.Else = simplify(c.Else)
	}
	return nil
}

func (cs *CaseExpr) compile(c *compiler) (ctype, error) {
	var ca collationAggregation
	var ta typeAggregation

	// Like MySQL, only run the code of the conditions up to the first true one, and
	// of the branch that is taken. The type the taken branch converts its value to
	// is filled in once all the branches have been compiled.
	res := &caseResult{allowZeroDate: c.sqlmode.AllowZeroDate()}
	var end []*jump

	for _, wt := range cs.cases {
		when, err := wt.when.compile(c)
		if err != nil {
			return ctype{}, err
		}

		if err := c.compileCheckTrue(when, 1); err != nil {
			return ctype{}, err
		}
		next := c.asm.jumpFrom()
		c.asm.CaseWhen(next)

		then, err := wt.then.compile(c)
		if err != nil {
			return ctype{}, err
		}

		ta.add(then.Type, then.Flag, then.Size, then.Scale)
		if err := ca.add(then.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}

		j := c.asm.jumpFrom()
		c.asm.CaseResult(res, j)
		end = append(end, j)

		// When the condition is not true, the code continues here, without the
		// value of the THEN on the stack.
		c.asm.jumpDestination(next)
		c.asm.adjustStack(-1)
	}

	if cs.Else != nil {
		els, err := cs.Else.compile(c)
		if err != nil {
			return ctype{}, err
		}

		ta.add(els.Type, els.Flag, els.Size, els.Scale)
		if err := ca.add(els.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
		c.asm.CaseResult(res, nil)
	} else {
		c.asm.PushNull()
	}
	c.asm.jumpDestination(end...)

	var f typeFlag
	// Without an ELSE branch, a CASE that matches no WHEN returns NULL.
	if ta.nullable || cs.Else == nil {
		f |= flagNullable
	}
	tt := ta.result()
	ct := ctype{Type: tt, Flag: f, Col: ca.resultFor(tt, cs.collate), Scale: ta.scale, Size: ta.size}
	res.tt, res.size, res.scale, res.col = ct.Type, ct.Size, ct.Scale, ct.Col.Collation
	return ct, nil
}

var _ IR = (*CaseExpr)(nil)
