/*
Copyright 2026 The Vitess Authors.

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
	"strings"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/eightbit"
)

// This file implements the rules MySQL uses to name the result-set column of
// a select expression. See doc/design-docs/MySQLCompatibleColumnNames.md for
// the rules and where they come from in the MySQL source.

// columnNameKind says how MySQL names an unaliased select expression.
type columnNameKind uint8

const (
	// nameRaw names the column after the expression's query text.
	nameRaw columnNameKind = iota
	// nameColumn names the column after the column identifier, as typed.
	nameColumn
	// nameText names the column after the value of the first fragment of a
	// text literal.
	nameText
	// nameNull names the column "NULL".
	nameNull
	// nameParam names the column "?".
	nameParam
	// nameInt names the column after an integer token that fits in a signed
	// 64-bit integer. The name is never truncated.
	nameInt
	// nameUint names the column after an integer token that only fits in an
	// unsigned 64-bit integer. The name is truncated to 256 bytes.
	nameUint
	// nameDecimal names the column after a decimal token, or an integer token
	// too large for 64 bits. The name is never truncated.
	nameDecimal
	// nameFloat names the column after a floating-point token. The name is
	// truncated to 256 bytes.
	nameFloat
)

// cppEditKind is a change that MySQL's lexer makes to the query text while it
// copies it into its pre-processed buffer, which is where column names are
// taken from.
type cppEditKind uint8

const (
	// cppOpen is the opening "/*!" or "/*!NNNNN" of an executed versioned
	// comment. It is removed.
	cppOpen cppEditKind = iota
	// cppClose is the closing "*/" of an executed versioned comment. It is
	// removed, and a space may be inserted in its place.
	cppClose
	// cppSkip is a versioned comment that is not executed. It is removed.
	cppSkip
)

// cppEdit is a cppEditKind at the byte range [start, end) of the query.
type cppEdit struct {
	start, end int
	kind       cppEditKind
}

// columnNameInput is what the parser records about a select expression so
// that the column can later be named the way MySQL names it.
type columnNameInput struct {
	// text is the query text of the expression, from the first byte of its
	// first token to the last byte of its last token.
	text string
	// token is the identifier of a column reference, or the token of a number.
	token string
	// edits are the versioned-comment edits inside text, with offsets
	// relative to text.
	edits []cppEdit
	kind  columnNameKind
	// parsed is set for every select expression the parser creates.
	parsed bool
	// aliased is set when the query gave the expression an alias. The alias
	// can be empty (AS ''), which is different from having no alias.
	aliased bool
}

// ColumnNameEnv holds the session settings that MySQL's column names depend on.
type ColumnNameEnv struct {
	// ClientCharset is the value of character_set_client. Empty means utf8mb4.
	ClientCharset string
	// ConnectionCharset is the charset of collation_connection. Empty means
	// utf8mb4.
	ConnectionCharset string
}

// setColumnNameInput records the naming input of a select expression. start and
// end are the byte offsets of the expression in the query, and aliased says
// whether the query gave the expression an alias.
func (ae *AliasedExpr) setColumnNameInput(tkn *Tokenizer, start, end int, aliased bool) {
	in := &ae._name
	in.parsed = true
	in.aliased = aliased
	if aliased {
		return
	}
	if start < 0 || end > len(tkn.buf) || start > end {
		return
	}
	in.text = tkn.buf[start:end]
	for _, e := range tkn.cppEdits {
		if e.start >= start && e.end <= end {
			in.edits = append(in.edits, cppEdit{start: e.start - start, end: e.end - start, kind: e.kind})
		}
	}

	switch expr := ae.Expr.(type) {
	case *ColName:
		in.kind = nameColumn
		in.token = expr.Name.String()
	case *NullVal:
		in.kind = nameNull
	case *Argument:
		if unwrapParensAndPlus(in.text) == "?" {
			in.kind = nameParam
		}
	case *Literal:
		in.kind, in.token = literalNameKind(expr)
	case *UnaryExpr:
		if lit, ok := expr.Expr.(*Literal); ok && expr.Operator == NStringOp && lit.Type == StrVal {
			in.kind = nameText
		}
	case *IntroducerExpr:
		if lit, ok := expr.Expr.(*Literal); ok && lit.Type == StrVal {
			in.kind = nameText
		}
	}
}

// literalNameKind returns how MySQL names a literal, and the token the name is
// taken from, if any.
func literalNameKind(lit *Literal) (columnNameKind, string) {
	switch lit.Type {
	case StrVal:
		return nameText, ""
	case IntVal:
		return integerNameKind(lit.Val), lit.Val
	case DecimalVal:
		return nameDecimal, lit.Val
	case FloatVal:
		return nameFloat, lit.Val
	}
	// Hexadecimal, bit and temporal literals are named after their text.
	return nameRaw, ""
}

// integerNameKind classifies an integer token the way MySQL's lexer does,
// which decides whether its name is truncated.
func integerNameKind(token string) columnNameKind {
	digits := strings.TrimLeft(token, "0")
	const maxInt64 = "9223372036854775807"
	const maxUint64 = "18446744073709551615"
	switch {
	case len(digits) < len(maxInt64) || (len(digits) == len(maxInt64) && digits <= maxInt64):
		return nameInt
	case len(digits) < len(maxUint64) || (len(digits) == len(maxUint64) && digits <= maxUint64):
		return nameUint
	}
	return nameDecimal
}

// unwrapParensAndPlus strips the parentheses and unary plus signs around an
// expression's text, which MySQL ignores when it names the expression.
func unwrapParensAndPlus(text string) string {
	for {
		trimmed := strings.TrimSpace(text)
		switch {
		case strings.HasPrefix(trimmed, "+"):
			text = trimmed[1:]
		case len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')':
			text = trimmed[1 : len(trimmed)-1]
		default:
			return trimmed
		}
	}
}

// MySQLColumnName returns the name MySQL gives the result-set column of this
// select expression.
//
// Expressions that the parser did not create, for example ones that the planner
// adds, are named by ColumnName.
func (ae *AliasedExpr) MySQLColumnName(env ColumnNameEnv) string {
	in := &ae._name
	if !in.parsed {
		return ae.ColumnName()
	}
	if in.aliased {
		return aliasName(ae.As.String())
	}
	switch in.kind {
	case nameColumn, nameInt, nameDecimal:
		return cutAtNUL(in.token)
	case nameNull:
		return "NULL"
	case nameParam:
		return "?"
	case nameUint, nameFloat:
		return truncateUTF8(in.token, maxAliasName)
	case nameText:
		value, cs := firstTextFragment(in.text)
		if cs == "utf8mb3" {
			// N'...' is converted to the national charset, utf8mb3.
			value = toUTF8MB3(value, len(value))
		} else if cs == "" {
			cs = env.ConnectionCharset
		}
		return copyName(value, cs)
	}
	return copyName(applyCppEdits(in.text, in.edits), env.ClientCharset)
}

// maxAliasName is MAX_ALIAS_NAME in MySQL: the longest a column name can be,
// in bytes.
const maxAliasName = 256

// aliasName applies MySQL's rules to an explicit alias: characters outside the
// Basic Multilingual Plane become '?' when the alias is converted to utf8mb3,
// leading non-graphic characters are removed, and the name is truncated to
// 256 bytes.
func aliasName(alias string) string {
	return copyName(toUTF8MB3(alias, len(alias)), "utf8mb3")
}

// copyName is MySQL's Name_string::copy, followed by the NUL cut that happens
// when the name is sent to the client. The name is always returned as valid
// UTF-8.
func copyName(name string, cs string) string {
	name = stripLeadingNonGraphic(name, cs)
	switch cs {
	case "utf8mb3", "utf8":
		// MySQL copies at most 256 bytes as they are. It can cut a character in
		// half, which clients that receive utf8mb4 never see: the conversion
		// drops it.
		name = truncateUTF8(name, maxAliasName)
	case "latin1":
		name = latin1ToUTF8MB3(name, maxAliasName-1)
	case "binary":
		if len(name) > maxAliasName-1 {
			name = name[:maxAliasName-1]
		}
		name = strings.ToValidUTF8(name, "?")
	default:
		name = toUTF8MB3(name, maxAliasName-1)
	}
	return cutAtNUL(name)
}

// stripLeadingNonGraphic removes the leading bytes that are not graphic in the
// given charset, the way Name_string::copy does.
func stripLeadingNonGraphic(name string, cs string) string {
	for i := 0; i < len(name); i++ {
		b := name[i]
		graphic := b > 0x20 && b != 0x7f
		switch cs {
		case "binary":
			graphic = graphic && b < 0x80
		case "latin1":
			switch b {
			case 0x81, 0x8d, 0x8f, 0x90, 0x9d, 0xa0:
				graphic = false
			}
		}
		if graphic {
			return name[i:]
		}
	}
	return ""
}

// toUTF8MB3 converts UTF-8 text to utf8mb3 the way MySQL does: characters
// outside the Basic Multilingual Plane and invalid bytes become '?', and an
// incomplete character at the end ends the text. It stops before the result
// would exceed limit bytes.
func toUTF8MB3(s string, limit int) string {
	clean := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			clean = false
			break
		}
	}
	if clean {
		if len(s) > limit {
			return s[:limit]
		}
		return s
	}

	var b strings.Builder
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size <= 1 && !utf8.FullRuneInString(s) {
			break
		}
		width := size
		if r == utf8.RuneError && size <= 1 || r > 0xFFFF {
			r, width = '?', 1
		}
		if b.Len()+width > limit {
			break
		}
		if r == '?' && width == 1 {
			b.WriteByte('?')
		} else {
			b.WriteString(s[:size])
		}
		s = s[size:]
	}
	return b.String()
}

// latin1ToUTF8MB3 converts latin1 bytes to UTF-8, stopping before the result
// would exceed limit bytes.
func latin1ToUTF8MB3(s string, limit int) string {
	var cs eightbit.Charset_latin1
	var b strings.Builder
	var buf [utf8.UTFMax]byte
	for i := 0; i < len(s); i++ {
		r, _, _ := cs.DecodeRune([]byte{s[i]})
		n := utf8.EncodeRune(buf[:], r)
		if b.Len()+n > limit {
			break
		}
		b.Write(buf[:n])
	}
	return b.String()
}

// truncateUTF8 truncates s to at most limit bytes, without cutting a character
// in half.
func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}

// cutAtNUL returns s up to its first NUL byte. MySQL sends column names as
// NUL-terminated strings, so a name ends at its first NUL.
func cutAtNUL(s string) string {
	if i := strings.IndexByte(s, 0); i >= 0 {
		return s[:i]
	}
	return s
}

// applyCppEdits applies the versioned-comment edits to an expression's text,
// giving the text as it is in MySQL's pre-processed buffer.
func applyCppEdits(text string, edits []cppEdit) string {
	if len(edits) == 0 {
		return text
	}
	var b strings.Builder
	pos := 0
	for _, e := range edits {
		b.WriteString(text[pos:e.start])
		pos = e.end
		if e.kind != cppClose || pos >= len(text) || b.Len() == 0 {
			continue
		}
		// MySQL inserts a space after an executed versioned comment when
		// neither the text before nor the text after it is whitespace.
		out := b.String()
		if !isCppSpace(text[pos]) && !isCppSpace(out[len(out)-1]) {
			b.WriteByte(' ')
		}
	}
	b.WriteString(text[pos:])
	return b.String()
}

func isCppSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// firstTextFragment returns the value of the first string of a text literal,
// such as a for 'a' 'b', N'a' or _utf8mb4'a', and the charset that an
// introducer or N gives it, if any. MySQL names a text literal after its first
// string only.
func firstTextFragment(text string) (value, charset string) {
	tkn := &Tokenizer{buf: text, parser: &Parser{}}
	for {
		typ, val := tkn.Scan()
		switch typ {
		case STRING:
			return val, charset
		case NCHAR_STRING:
			return val, "utf8mb3"
		case 0, LEX_ERROR:
			return "", charset
		default:
			if strings.HasPrefix(val, "_") {
				charset = strings.ToLower(val[1:])
			}
		}
	}
}
