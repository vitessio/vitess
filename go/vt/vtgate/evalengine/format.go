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
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func precedenceFor(in IR) sqlparser.Precendence {
	switch node := in.(type) {
	case *LogicalExpr:
		switch node.op.(type) {
		case opLogicalOr:
			return sqlparser.P16
		case opLogicalXor:
			return sqlparser.P15
		case opLogicalAnd:
			return sqlparser.P14
		}
	case *NotExpr:
		return sqlparser.P13
	case *ComparisonExpr:
		return sqlparser.P11
	case *IsExpr:
		return sqlparser.P11
	case *BitwiseExpr:
		switch node.Op.(type) {
		case opBitOr:
			return sqlparser.P10
		case opBitAnd:
			return sqlparser.P9
		case opBitShift:
			return sqlparser.P8
		case opBitXor:
			return sqlparser.P5
		}
	case *ArithmeticExpr:
		switch node.Op.(type) {
		case *opArithAdd, *opArithSub:
			return sqlparser.P7
		case *opArithDiv, *opArithIntDiv, *opArithMul, *opArithMod:
			return sqlparser.P6
		}
	case *NegateExpr:
		return sqlparser.P4
	}
	return sqlparser.Syntactic
}

func needParens(op, val IR, left bool) bool {
	// Values are atomic and never need parens
	switch val.(type) {
	case *Literal:
		return false
	case *IsExpr:
		if _, ok := op.(*IsExpr); ok {
			return true
		}
	}

	opBinding := precedenceFor(op)
	valBinding := precedenceFor(val)

	if opBinding == sqlparser.Syntactic || valBinding == sqlparser.Syntactic {
		return false
	}

	if left {
		// for left associative operators, if the value is to the left of the operator,
		// we only need parens if the order is higher for the value expression
		return valBinding > opBinding
	}

	return valBinding >= opBinding
}

func formatExpr(buf *sqlparser.TrackedBuffer, currentExpr, expr IR, left bool) {
	needp := needParens(currentExpr, expr, left)
	if needp {
		buf.WriteByte('(')
	}
	expr.format(buf)
	if needp {
		buf.WriteByte(')')
	}
}

func formatBinary(buf *sqlparser.TrackedBuffer, self, left IR, op string, right IR) {
	formatExpr(buf, self, left, true)
	buf.WriteString(" ")
	buf.WriteLiteral(op)
	buf.WriteString(" ")
	formatExpr(buf, self, right, false)
}

func (l *Literal) Format(buf *sqlparser.TrackedBuffer) {
	l.format(buf)
}

func (l *Literal) FormatFast(buf *sqlparser.TrackedBuffer) {
	l.format(buf)
}

func (l *Literal) format(buf *sqlparser.TrackedBuffer) {
	switch inner := l.inner.(type) {
	case *evalTuple:
		buf.WriteByte('(')
		for i, val := range inner.t {
			if i > 0 {
				buf.WriteString(", ")
			}
			evalToSQLValue(val).EncodeSQLStringBuilder(buf.Builder)
		}
		buf.WriteByte(')')

	default:
		evalToSQLValue(l.inner).EncodeSQLStringBuilder(buf.Builder)
	}
}

func (bv *BindVariable) Format(buf *sqlparser.TrackedBuffer) {
	bv.format(buf)
}

func (bv *BindVariable) FormatFast(buf *sqlparser.TrackedBuffer) {
	bv.format(buf)
}

func (bv *BindVariable) format(buf *sqlparser.TrackedBuffer) {
	if bv.Type == sqltypes.Tuple {
		buf.WriteArg("::", bv.Key)
	} else {
		buf.WriteArg(":", bv.Key)
	}
}

func (bv *TupleBindVariable) Format(buf *sqlparser.TrackedBuffer) {
	bv.format(buf)
}

func (bv *TupleBindVariable) FormatFast(buf *sqlparser.TrackedBuffer) {
	bv.format(buf)
}

func (bv *TupleBindVariable) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteString(fmt.Sprintf("%s:%d", bv.Key, bv.Index))
}

func (c *Column) Format(buf *sqlparser.TrackedBuffer) {
	c.format(buf)
}

func (c *Column) FormatFast(buf *sqlparser.TrackedBuffer) {
	c.format(buf)
}

func (c *Column) format(buf *sqlparser.TrackedBuffer) {
	if c.Original != nil {
		c.Original.FormatFast(buf)
	} else {
		_, _ = fmt.Fprintf(buf, "_vt_column_%d", c.Offset)
	}
}

func (b *ArithmeticExpr) format(buf *sqlparser.TrackedBuffer) {
	formatBinary(buf, b, b.Left, b.Op.String(), b.Right)
}

func (c *ComparisonExpr) format(buf *sqlparser.TrackedBuffer) {
	formatBinary(buf, c, c.Left, c.Op.String(), c.Right)
}

func (c *LikeExpr) format(buf *sqlparser.TrackedBuffer) {
	op := "like"
	if c.Negate {
		op = "not like"
	}
	formatBinary(buf, c, c.Left, op, c.Right)
}

func (c *InExpr) format(buf *sqlparser.TrackedBuffer) {
	op := "in"
	if c.Negate {
		op = "not in"
	}
	formatBinary(buf, c, c.Left, op, c.Right)
}

func (tuple TupleExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteByte('(')
	for i, expr := range tuple {
		if i > 0 {
			buf.WriteString(", ")
		}
		formatExpr(buf, tuple, expr, true)
	}
	buf.WriteByte(')')
}

func (c *CollateExpr) format(buf *sqlparser.TrackedBuffer) {
	formatExpr(buf, c, c.Inner, true)
	buf.WriteLiteral(" COLLATE ")
	buf.WriteString(c.CollationEnv.LookupName(c.TypedCollation.Collation))
}

func (i *IntroducerExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteString("_")
	buf.WriteString(i.CollationEnv.LookupName(i.TypedCollation.Collation))
	formatExpr(buf, i, i.Inner, true)
}

func (n *NotExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteLiteral("not ")
	formatExpr(buf, n, n.Inner, true)
}

func (b *LogicalExpr) format(buf *sqlparser.TrackedBuffer) {
	formatBinary(buf, b, b.Left, b.op.String(), b.Right)
}

func (i *IsExpr) format(buf *sqlparser.TrackedBuffer) {
	formatExpr(buf, i, i.Inner, true)
	switch i.Op {
	case sqlparser.IsNullOp:
		buf.WriteLiteral(" is null")
	case sqlparser.IsNotNullOp:
		buf.WriteLiteral(" is not null")
	case sqlparser.IsTrueOp:
		buf.WriteLiteral(" is true")
	case sqlparser.IsNotTrueOp:
		buf.WriteLiteral(" is not true")
	case sqlparser.IsFalseOp:
		buf.WriteLiteral(" is false")
	case sqlparser.IsNotFalseOp:
		buf.WriteLiteral(" is not false")
	}
}

func (c *CallExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteLiteral(c.Method)
	buf.WriteByte('(')
	for i, expr := range c.Arguments {
		if i > 0 {
			buf.WriteString(", ")
		}
		formatExpr(buf, c, expr, true)
	}
	buf.WriteByte(')')
}

func (c *builtinWeightString) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteLiteral("weight_string(")
	formatExpr(buf, c, c.Arguments[0], true)

	if c.Cast != "" {
		buf.WriteLiteral(" as ")
		buf.WriteLiteral(c.Cast)
		_, _ = fmt.Fprintf(buf, "(%d)", *c.Len)
	}
	buf.WriteByte(')')
}

func (n *NegateExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteByte('-')
	formatExpr(buf, n, n.Inner, true)
}

func (bit *BitwiseExpr) format(buf *sqlparser.TrackedBuffer) {
	formatBinary(buf, bit, bit.Left, bit.Op.BitwiseOp(), bit.Right)
}

func (b *BitwiseNotExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteByte('~')
	formatExpr(buf, b, b.Inner, true)
}

func (c *ConvertExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteLiteral("convert(")
	formatExpr(buf, c, c.Inner, true)

	switch {
	case c.Length != nil && c.Scale != nil:
		_, _ = fmt.Fprintf(buf, ", %s(%d,%d)", c.Type, *c.Length, *c.Scale)
	case c.Length != nil:
		_, _ = fmt.Fprintf(buf, ", %s(%d)", c.Type, *c.Length)
	default:
		_, _ = fmt.Fprintf(buf, ", %s", c.Type)
	}
	if c.Collation != collations.Unknown {
		buf.WriteLiteral(" character set ")
		buf.WriteString(c.CollationEnv.LookupName(c.Collation))
	}
	buf.WriteByte(')')
}

func (c *ConvertUsingExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteLiteral("convert(")
	formatExpr(buf, c, c.Inner, true)
	buf.WriteLiteral(" using ")
	buf.WriteString(c.CollationEnv.LookupName(c.Collation))
	buf.WriteByte(')')
}

func (c *CaseExpr) format(buf *sqlparser.TrackedBuffer) {
	buf.WriteLiteral("case")
	for _, cs := range c.cases {
		buf.WriteLiteral(" when ")
		formatExpr(buf, c, cs.when, true)
		buf.WriteLiteral(" then ")
		formatExpr(buf, c, cs.then, true)
	}
	if c.Else != nil {
		buf.WriteLiteral(" else ")
		formatExpr(buf, c, c.Else, true)
	}
}
