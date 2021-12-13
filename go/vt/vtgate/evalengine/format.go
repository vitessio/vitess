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
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
)

func FormatExpr(expr Expr) string {
	var f formatter
	expr.format(&f, 0)
	return f.String()
}

func PrettyPrint(expr Expr) string {
	var f formatter
	f.indent = "    "
	expr.format(&f, 0)
	return f.String()
}

type formatter struct {
	strings.Builder
	indent string
}

func (f *formatter) Indent(depth int) {
	if depth > 0 && f.indent != "" {
		f.WriteByte('\n')
		for i := 0; i < depth; i++ {
			f.WriteString(f.indent)
		}
	}
}

func (l *Literal) format(w *formatter, depth int) {
	w.Indent(depth)
	w.WriteString(l.Val.Value().String())
}

func (bv *BindVariable) format(w *formatter, depth int) {
	w.Indent(depth)
	w.WriteByte(':')
	w.WriteString(bv.Key)
}

func (c *Column) format(w *formatter, depth int) {
	w.Indent(depth)
	fmt.Fprintf(w, "[COLUMN %d]", c.Offset)
}

func (b *BinaryExpr) format(w *formatter, depth int) {
	w.Indent(depth)
	if depth > 0 {
		w.WriteByte('(')
	}

	b.Left.format(w, depth+1)
	w.WriteString(" ")
	w.WriteString(b.Op.String())
	w.WriteString(" ")
	b.Right.format(w, depth+1)

	if depth > 0 {
		w.WriteByte(')')
	}
}

func (c *ComparisonExpr) format(w *formatter, depth int) {
	w.Indent(depth)
	if depth > 0 {
		w.WriteByte('(')
	}

	c.Left.format(w, depth+1)
	w.WriteString(" ")
	w.WriteString(c.Op.String())
	w.WriteString(" ")
	c.Right.format(w, depth+1)

	if depth > 0 {
		w.WriteByte(')')
	}
}

func (t TupleExpr) format(w *formatter, depth int) {
	w.Indent(depth)
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
	w.Indent(depth)
	c.Inner.format(w, depth)
	coll := collations.Local().LookupByID(c.TypedCollation.Collation)
	w.WriteString(" COLLATE ")
	w.WriteString(coll.Name())
}

func (n *NotExpr) format(w *formatter, depth int) {
	w.Indent(depth)
	w.WriteString("NOT ")
	n.Inner.format(w, depth)
}
