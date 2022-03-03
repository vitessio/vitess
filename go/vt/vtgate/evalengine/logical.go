/*
Copyright 2021 The Vitess Authors.

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
	LogicalOp interface {
		eval(left, right EvalResult) (boolean, error)
		String()
	}

	LogicalExpr struct {
		BinaryExpr
		op     func(left, right boolean) boolean
		opname string
	}
	NotExpr struct {
		UnaryExpr
	}

	OpLogicalAnd struct{}

	boolean int8
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

func (n *NotExpr) eval(env *ExpressionEnv, out *EvalResult) {
	var inner EvalResult
	inner.init(env, n.Inner)
	out.setBoolean(inner.isTruthy().not())
}

func (n *NotExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, flags := n.Inner.typeof(env)
	return sqltypes.Uint64, flags
}

func (l *LogicalExpr) eval(env *ExpressionEnv, out *EvalResult) {
	var left, right EvalResult
	left.init(env, l.Left)
	right.init(env, l.Right)
	if left.typeof() == sqltypes.Tuple || right.typeof() == sqltypes.Tuple {
		panic("did not typecheck tuples")
	}
	out.setBoolean(l.op(left.isTruthy(), right.isTruthy()))
}

func (l *LogicalExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f1 := l.Left.typeof(env)
	_, f2 := l.Right.typeof(env)
	return sqltypes.Uint64, f1 | f2
}

// IsExpr represents the IS expression in MySQL.
// boolean_primary IS [NOT] {TRUE | FALSE | NULL}
type IsExpr struct {
	UnaryExpr
	Op    sqlparser.IsExprOperator
	Check func(*EvalResult) bool
}

func (i *IsExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var in EvalResult
	in.init(env, i.Inner)
	result.setBool(i.Check(&in))
}

func (i *IsExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	return sqltypes.Int64, 0
}
