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
	"errors"
	"slices"
	"sync"

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
		cases []WhenThen
		Else  IR
		// typing types the expression when it is interpreted; nil if the
		// expression was not built by translation.
		typing *caseTyping
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
	// MySQL evaluates the WHEN conditions in order and only the branch it
	// returns, so a branch that would fail (e.g. EXP(710)) must not fail the
	// whole expression unless it is chosen. The type of the result still
	// depends on every branch, so it comes from their static types.
	ct, err := c.typeof(env)
	if err != nil {
		return c.evalAllBranches(env)
	}

	result := c.Else
	for _, whenThen := range c.cases {
		when, err := whenThen.when.eval(env)
		if err != nil {
			return nil, err
		}
		if evalIsTruthy(when) == boolTrue {
			result = whenThen.then
			break
		}
	}
	if result == nil {
		return nil, nil
	}
	e, err := result.eval(env)
	if err != nil {
		return nil, err
	}
	return evalCoerce(e, ct.Type, ct.Size, ct.Scale, ct.Col.Collation, env.now, env.sqlmode.AllowZeroDate())
}

// typeof returns the type that the compiler gives the CASE expression, typing
// the columns and bind variables that were not typed at translation from env.
func (c *CaseExpr) typeof(env *ExpressionEnv) (ctype, error) {
	if c.typing == nil {
		return ctype{}, errCaseUntyped
	}
	return c.typing.typeof(env, c)
}

var errCaseUntyped = errors.New("CASE expression without typing information")

// caseTyping caches the type of an interpreted CASE expression for each set
// of types of its untyped columns and bind variables, like UntypedExpr does
// for compiled programs.
type caseTyping struct {
	env       *vtenv.Environment
	collation collations.ID
	untyped   []typedIR

	mu    sync.Mutex
	types []caseType
}

type caseType struct {
	dynamic []ctype
	ct      ctype
	err     error
}

func (ast *astCompiler) caseTyping(untypedFrom int) *caseTyping {
	return &caseTyping{
		env:       ast.cfg.Environment,
		collation: ast.cfg.Collation,
		untyped:   slices.Clone(ast.untyped[untypedFrom:]),
	}
}

func (t *caseTyping) typeof(env *ExpressionEnv, c *CaseExpr) (ctype, error) {
	var dynamic []ctype
	if len(t.untyped) > 0 {
		dynamic = make([]ctype, len(t.untyped))
		for i, expr := range t.untyped {
			var err error
			if dynamic[i], err = expr.typeof(env); err != nil {
				return ctype{}, err
			}
		}
	}

	// ponytail: one lock per evaluation, as UntypedExpr.Compile; make it
	// lock-free if it shows up under contention
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, typ := range t.types {
		if slices.EqualFunc(typ.dynamic, dynamic, func(a, b ctype) bool { return a.equal(b) }) {
			return typ.ct, typ.err
		}
	}
	comp := compiler{
		env:       t.env,
		collation: t.collation,
		sqlmode:   env.sqlmode,
		typeEnv:   env,
	}
	ct, err := c.compile(&comp)
	t.types = append(t.types, caseType{dynamic: dynamic, ct: ct, err: err})
	return ct, err
}

// evalAllBranches evaluates every branch and aggregates the result type from
// the values. It is the fallback for expressions the compiler cannot type.
func (c *CaseExpr) evalAllBranches(env *ExpressionEnv) (eval, error) {
	var ta typeAggregation
	var ca collationAggregation
	var result eval
	matched := false

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
	return evalCoerce(result, ta.result(), ta.size, ta.scale, ca.result().Collation, env.now, env.sqlmode.AllowZeroDate())
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
	for i := range c.cases {
		whenThen := &c.cases[i]
		whenThen.when = simplifyBranch(env, whenThen.when)
		whenThen.then = simplifyBranch(env, whenThen.then)
	}
	if c.Else != nil {
		c.Else = simplifyBranch(env, c.Else)
	}
	return nil
}

func (cs *CaseExpr) compile(c *compiler) (ctype, error) {
	var ca collationAggregation
	var ta typeAggregation
	var done []*jump

	// Only the chosen branch runs: a false or NULL WHEN jumps over its THEN,
	// and every THEN jumps to the end, where the result is coerced to the
	// type of the whole expression.
	for _, wt := range cs.cases {
		when, err := wt.when.compile(c)
		if err != nil {
			return ctype{}, err
		}

		if err := c.compileCheckTrue(when, 1); err != nil {
			return ctype{}, err
		}
		next := c.asm.jumpFrom()
		c.asm.JumpFalse(next)

		then, err := wt.then.compile(c)
		if err != nil {
			return ctype{}, err
		}
		then = c.constantBranchType(wt.then, then)

		ta.add(then.Type, then.Flag, then.Size, then.Scale)
		if err := ca.add(then.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}

		j := c.asm.jumpFrom()
		c.asm.Jump(j)
		done = append(done, j)
		// the next WHEN starts without this THEN's result on the stack
		c.asm.adjustStack(-1)
		c.asm.jumpDestination(next)
	}

	if cs.Else != nil {
		els, err := cs.Else.compile(c)
		if err != nil {
			return ctype{}, err
		}
		els = c.constantBranchType(cs.Else, els)

		ta.add(els.Type, els.Flag, els.Size, els.Scale)
		if err := ca.add(els.Col, c.env.CollationEnv()); err != nil {
			return ctype{}, err
		}
	} else {
		c.asm.PushNull()
	}
	c.asm.jumpDestination(done...)

	var f typeFlag
	if ta.nullable {
		f |= flagNullable
	}
	ct := ctype{Type: ta.result(), Flag: f, Col: ca.result(), Scale: ta.scale, Size: ta.size}
	c.asm.Coerce(ct.Type, ct.Size, ct.Scale, ct.Col, c.sqlmode.AllowZeroDate())
	return ct, nil
}

// constantBranchType types a constant branch by its value, as constant folding
// would. The compiler types some constants optimistically and corrects them
// at run time (e.g. -9223372036854775809 is compiled as a BIGINT negation that
// deoptimizes into a DECIMAL), which never happens for a branch that is not
// taken. A branch that fails to evaluate keeps its compiled type.
func (c *compiler) constantBranchType(expr IR, ct ctype) ctype {
	if !expr.constant() {
		return ct
	}
	v, err := expr.eval(EmptyExpressionEnv(c.env))
	if err != nil || v == nil || v.SQLType() == ct.Type {
		return ct
	}
	ct.Type, ct.Size, ct.Scale = v.SQLType(), v.Size(), v.Scale()
	return ct
}

// simplifyBranch folds a branch that is only evaluated when it is chosen. A
// branch that fails to fold is kept as is, so that it fails only if chosen.
func simplifyBranch(env *ExpressionEnv, e IR) IR {
	simplified, err := simplifyExpr(env, e)
	if err != nil {
		return e
	}
	return simplified
}

var _ IR = (*CaseExpr)(nil)
