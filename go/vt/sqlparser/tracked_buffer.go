/*
Copyright 2019 The Vitess Authors.

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
	"strings"
)

// NodeFormatter defines the signature of a custom node formatter
// function that can be given to TrackedBuffer for code generation.
type NodeFormatter func(buf *TrackedBuffer, node SQLNode)

// TrackedBuffer is used to rebuild a query from the ast.
// bindLocations keeps track of locations in the buffer that
// use bind variables for efficient future substitutions.
// nodeFormatter is the formatting function the buffer will
// use to format a node. By default(nil), it's FormatNode.
// But you can supply a different formatting function if you
// want to generate a query that's different from the default.
type TrackedBuffer struct {
	*strings.Builder
	bindLocations []bindLocation
	nodeFormatter NodeFormatter
	literal       func(string) (int, error)
	fast          bool

	escape escapeType
}

type escapeType int

const (
	escapeKeywords escapeType = iota
	escapeAllIdentifiers
	escapeNoIdentifiers
)

// NewTrackedBuffer creates a new TrackedBuffer.
func NewTrackedBuffer(nodeFormatter NodeFormatter) *TrackedBuffer {
	buf := &TrackedBuffer{
		Builder:       new(strings.Builder),
		nodeFormatter: nodeFormatter,
	}
	buf.literal = buf.WriteString
	buf.fast = nodeFormatter == nil
	return buf
}

func (buf *TrackedBuffer) writeStringUpperCase(lit string) (int, error) {
	// Upcasing is performed for ASCII only, following MySQL's behavior
	buf.Grow(len(lit))
	for i := 0; i < len(lit); i++ {
		c := lit[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf.WriteByte(c)
	}
	return len(lit), nil
}

// SetUpperCase sets whether all SQL statements formatted by this TrackedBuffer will be normalized into
// uppercase. By default, formatted statements are normalized into lowercase.
// Enabling this option will prevent the optimized fastFormat routines from running.
func (buf *TrackedBuffer) SetUpperCase(enable bool) {
	buf.fast = false
	if enable {
		buf.literal = buf.writeStringUpperCase
	} else {
		buf.literal = buf.WriteString
	}
}

// SetEscapeAllIdentifiers sets whether ALL identifiers in the serialized SQL query should be quoted
// and escaped. By default, identifiers are only escaped if they match the name of a SQL keyword or they
// contain characters that must be escaped.
// Enabling this option will prevent the optimized fastFormat routines from running.
func (buf *TrackedBuffer) SetEscapeAllIdentifiers() {
	buf.fast = false
	buf.escape = escapeAllIdentifiers
}

// SetEscapeNoIdentifier sets whether NO identifiers in the serialized SQL query should be quoted and escaped.
// Warning: this can lead to query output that is not valid SQL
// Enabling this option will prevent the optimized fastFormat routines from running.
func (buf *TrackedBuffer) SetEscapeNoIdentifier() {
	buf.fast = false
	buf.escape = escapeNoIdentifiers
}

// WriteNode function, initiates the writing of a single SQLNode tree by passing
// through to Myprintf with a default format string
func (buf *TrackedBuffer) WriteNode(node SQLNode) *TrackedBuffer {
	buf.Myprintf("%v", node)
	return buf
}

// Myprintf mimics fmt.Fprintf(buf, ...), but limited to Node(%v),
// Node.Value(%s) and string(%s). It also allows a %a for a value argument, in
// which case it adds tracking info for future substitutions.
// It adds parens as needed to follow precedence rules when printing expressions.
// To handle parens correctly for left associative binary operators,
// use %l and %r to tell the TrackedBuffer which value is on the LHS and RHS
//
// The name must be something other than the usual Printf() to avoid "go vet"
// warnings due to our custom format specifiers.
// *** THIS METHOD SHOULD NOT BE USED FROM ast.go. USE astPrintf INSTEAD ***
func (buf *TrackedBuffer) Myprintf(format string, values ...any) {
	buf.astPrintf(nil, format, values...)
}

func (buf *TrackedBuffer) printExpr(currentExpr Expr, expr Expr, left bool) {
	if precedenceFor(currentExpr) == Syntactic {
		expr.formatFast(buf)
	} else {
		needParens := needParens(currentExpr, expr, left)
		if needParens {
			buf.WriteByte('(')
		}
		expr.formatFast(buf)
		if needParens {
			buf.WriteByte(')')
		}
	}
}

// astPrintf is for internal use by the ast structs
func (buf *TrackedBuffer) astPrintf(currentNode SQLNode, format string, values ...any) {
	currentExpr, checkParens := currentNode.(Expr)
	if checkParens {
		// expressions that have Precedence Syntactic will never need parens
		checkParens = precedenceFor(currentExpr) != Syntactic
	}

	end := len(format)
	fieldnum := 0
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' {
			i++
		}
		if i > lasti {
			_, _ = buf.literal(format[lasti:i])
		}
		if i >= end {
			break
		}
		i++ // '%'

		caseSensitive := false
		if format[i] == '#' {
			caseSensitive = true
			i++
		}

		switch format[i] {
		case 'c':
			switch v := values[fieldnum].(type) {
			case byte:
				buf.WriteByte(v)
			case rune:
				buf.WriteRune(v)
			default:
				panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
			}
		case 's':
			switch v := values[fieldnum].(type) {
			case string:
				if caseSensitive {
					buf.WriteString(v)
				} else {
					_, _ = buf.literal(v)
				}
			default:
				panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
			}
		case 'l', 'r', 'v':
			left := format[i] != 'r'
			value := values[fieldnum]
			expr := getExpressionForParensEval(checkParens, value)

			if expr == nil {
				buf.formatter(value.(SQLNode))
			} else {
				needParens := needParens(currentExpr, expr, left)
				if needParens {
					buf.WriteByte('(')
				}
				buf.formatter(expr)
				if needParens {
					buf.WriteByte(')')
				}
			}
		case 'd':
			buf.WriteString(fmt.Sprintf("%d", values[fieldnum]))
		case 'a':
			buf.WriteArg("", values[fieldnum].(string))
		default:
			panic("unexpected")
		}
		fieldnum++
		i++
	}
}

func getExpressionForParensEval(checkParens bool, value any) Expr {
	if checkParens {
		expr, isExpr := value.(Expr)
		if isExpr {
			return expr
		}
	}
	return nil
}

func (buf *TrackedBuffer) formatter(node SQLNode) {
	switch {
	case buf.fast:
		node.formatFast(buf)
	case buf.nodeFormatter != nil:
		buf.nodeFormatter(buf, node)
	default:
		node.Format(buf)
	}
}

// needParens says if we need a parenthesis
// op is the operator we are printing
// val is the value we are checking if we need parens around or not
// left let's us know if the value is on the lhs or rhs of the operator
func needParens(op, val Expr, left bool) bool {
	// Values are atomic and never need parens
	if IsValue(val) {
		return false
	}

	if areBothISExpr(op, val) {
		return true
	}

	opBinding := precedenceFor(op)
	valBinding := precedenceFor(val)

	if opBinding == Syntactic || valBinding == Syntactic {
		return false
	}

	if left {
		// for left associative operators, if the value is to the left of the operator,
		// we only need parens if the order is higher for the value expression
		return valBinding > opBinding
	}

	return valBinding >= opBinding
}

func areBothISExpr(op Expr, val Expr) bool {
	_, isOpIS := op.(*IsExpr)
	if isOpIS {
		_, isValIS := val.(*IsExpr)
		if isValIS {
			// when using IS on an IS op, we need special handling
			return true
		}
	}
	return false
}

// WriteArg writes a value argument into the buffer along with
// tracking information for future substitutions.
func (buf *TrackedBuffer) WriteArg(prefix, arg string) {
	buf.bindLocations = append(buf.bindLocations, bindLocation{
		offset: buf.Len(),
		length: len(prefix) + len(arg),
	})
	buf.WriteString(prefix)
	buf.WriteString(arg)
}

// ParsedQuery returns a ParsedQuery that contains bind
// locations for easy substitution.
func (buf *TrackedBuffer) ParsedQuery() *ParsedQuery {
	return &ParsedQuery{Query: buf.String(), bindLocations: buf.bindLocations}
}

// HasBindVars returns true if the parsed query uses bind vars.
func (buf *TrackedBuffer) HasBindVars() bool {
	return len(buf.bindLocations) != 0
}

// BuildParsedQuery builds a ParsedQuery from the input.
func BuildParsedQuery(in string, vars ...any) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf(in, vars...)
	return buf.ParsedQuery()
}

// String returns a string representation of an SQLNode.
func String(node SQLNode) string {
	if node == nil {
		return "<nil>"
	}

	buf := NewTrackedBuffer(nil)
	node.formatFast(buf)
	return buf.String()
}

// UnescapedString will return a string where no identifiers have been escaped.
func UnescapedString(node SQLNode) string {
	if node == nil {
		return "" // do not return '<nil>', which is Go syntax.
	}

	buf := NewTrackedBuffer(nil)
	buf.SetEscapeNoIdentifier()
	node.Format(buf)
	return buf.String()

}

// CanonicalString returns a canonical string representation of an SQLNode where all identifiers
// are always escaped and all SQL syntax is in uppercase. This matches the canonical output from MySQL.
func CanonicalString(node SQLNode) string {
	if node == nil {
		return "" // do not return '<nil>', which is Go syntax.
	}

	buf := NewTrackedBuffer(nil)
	buf.SetUpperCase(true)
	buf.SetEscapeAllIdentifiers()
	node.Format(buf)
	return buf.String()
}
