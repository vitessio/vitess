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
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
)

func FormatExpr(expr Expr) string {
	var f formatter
	expr.format(&f, 0)
	return f.String()
}

type formatter struct {
	strings.Builder
}

func (f *formatter) formatBinary(left Expr, op string, right Expr, depth int) {
	if depth > 0 {
		f.WriteByte('(')
	}

	left.format(f, depth+1)
	f.WriteString(" ")
	f.WriteString(op)
	f.WriteString(" ")
	right.format(f, depth+1)

	if depth > 0 {
		f.WriteByte(')')
	}
}

func (l *Literal) format(w *formatter, depth int) {
	switch inner := l.inner.(type) {
	case *evalTuple:
		w.WriteByte('(')
		for i, val := range inner.t {
			if i > 0 {
				w.WriteString(", ")
			}
			w.WriteString(evalToSQLValue(val).String())
		}
		w.WriteByte(')')

	default:
		w.WriteString(evalToSQLValue(l.inner).String())
	}
}

func (bv *BindVariable) format(w *formatter, depth int) {
	w.WriteByte(':')
	if bv.tuple {
		w.WriteByte(':')
	}
	w.WriteString(bv.Key)
}

func (c *Column) format(w *formatter, depth int) {
	fmt.Fprintf(w, "[COLUMN %d]", c.Offset)
}

func (b *ArithmeticExpr) format(w *formatter, depth int) {
	w.formatBinary(b.Left, b.Op.String(), b.Right, depth)
}

func (c *ComparisonExpr) format(w *formatter, depth int) {
	w.formatBinary(c.Left, c.Op.String(), c.Right, depth)
}

func (c *LikeExpr) format(w *formatter, depth int) {
	op := "LIKE"
	if c.Negate {
		op = "NOT LIKE"
	}
	w.formatBinary(c.Left, op, c.Right, depth)
}

func (c *InExpr) format(w *formatter, depth int) {
	op := "IN"
	if c.Negate {
		op = "NOT IN"
	}
	w.formatBinary(c.Left, op, c.Right, depth)
}

func (t TupleExpr) format(w *formatter, depth int) {
	w.WriteByte('(')
	for i, expr := range t {
		if i > 0 {
			w.WriteString(", ")
		}
		expr.format(w, depth+1)
	}
	w.WriteByte(')')
}

func (c *CollateExpr) format(w *formatter, depth int) {
	c.Inner.format(w, depth)
	coll := collations.Local().LookupByID(c.TypedCollation.Collation)
	w.WriteString(" COLLATE ")
	w.WriteString(coll.Name())
}

func (n *NotExpr) format(w *formatter, depth int) {
	w.WriteString("NOT ")
	n.Inner.format(w, depth)
}

func (b *LogicalExpr) format(w *formatter, depth int) {
	w.formatBinary(b.Left, b.opname, b.Right, depth)
}

func (i *IsExpr) format(w *formatter, depth int) {
	i.Inner.format(w, depth)
	switch i.Op {
	case sqlparser.IsNullOp:
		w.WriteString(" IS NULL")
	case sqlparser.IsNotNullOp:
		w.WriteString(" IS NOT NULL")
	case sqlparser.IsTrueOp:
		w.WriteString(" IS TRUE")
	case sqlparser.IsNotTrueOp:
		w.WriteString(" IS NOT TRUE")
	case sqlparser.IsFalseOp:
		w.WriteString(" IS FALSE")
	case sqlparser.IsNotFalseOp:
		w.WriteString(" IS NOT FALSE")
	}
}

func (c *CallExpr) format(w *formatter, depth int) {
	w.WriteString(strings.ToUpper(c.Method))
	w.WriteByte('(')
	for i, expr := range c.Arguments {
		if i > 0 {
			w.WriteString(", ")
		}
		expr.format(w, depth+1)
	}
	w.WriteByte(')')
}

func (c *builtinWeightString) format(w *formatter, depth int) {
	w.WriteString("WEIGHT_STRING(")
	c.String.format(w, depth)

	if c.Cast != "" {
		fmt.Fprintf(w, " AS %s(%d)", strings.ToUpper(c.Cast), c.Len)
	}
	w.WriteByte(')')
}

func (n *NegateExpr) format(w *formatter, depth int) {
	w.WriteByte('-')
	n.Inner.format(w, depth)
}

func (bit *BitwiseExpr) format(buf *formatter, depth int) {
	buf.formatBinary(bit.Left, bit.Op.BitwiseOp(), bit.Right, depth)
}

func (b *BitwiseNotExpr) format(buf *formatter, depth int) {
	buf.WriteByte('~')
	b.Inner.format(buf, depth)
}

func (c *ConvertExpr) format(buf *formatter, depth int) {
	buf.WriteString("CONVERT(")
	c.Inner.format(buf, depth)

	switch {
	case c.HasLength && c.HasScale:
		fmt.Fprintf(buf, ", %s(%d,%d)", c.Type, c.Length, c.Scale)
	case c.HasLength:
		fmt.Fprintf(buf, ", %s(%d)", c.Type, c.Length)
	default:
		fmt.Fprintf(buf, ", %s", c.Type)
	}
	if c.Collation != collations.Unknown {
		buf.WriteString(" CHARACTER SET ")
		buf.WriteString(collations.Local().LookupByID(c.Collation).Name())
	}
	buf.WriteByte(')')
}

func (c *ConvertUsingExpr) format(buf *formatter, depth int) {
	buf.WriteString("CONVERT(")
	c.Inner.format(buf, depth)
	buf.WriteString(" USING ")
	buf.WriteString(collations.Local().LookupByID(c.Collation).Name())
	buf.WriteByte(')')
}
