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
	"vitess.io/vitess/go/mysql/collations"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

func (b boolean) not() boolean {
	switch b {
	case boolFalse:
		return boolTrue
	case boolTrue:
		return boolFalse
	default:
		return b
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

func (b boolean) evalResult() EvalResult {
	if b == boolNULL {
		return resultNull
	}
	return evalResultBool(b == boolTrue)
}

func (n *NotExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	res, err := n.Inner.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	return res.nonzero().not().evalResult(), nil
}

func (n *NotExpr) typeof(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_UINT64, nil
}

func (n *NotExpr) collation() collations.TypedCollation {
	return collationNumeric
}

func (l *LogicalExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	lVal, err := l.Left.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := l.Right.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	if lVal.typ == querypb.Type_TUPLE || rVal.typ == querypb.Type_TUPLE {
		panic("did not typecheck tuples")
		// return EvalResult{}, cardinalityError(1)
	}
	return l.op(lVal.nonzero(), rVal.nonzero()).evalResult(), nil
}

func (l *LogicalExpr) typeof(env *ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_UINT64, nil
}

func (n *LogicalExpr) collation() collations.TypedCollation {
	return collationNumeric
}
