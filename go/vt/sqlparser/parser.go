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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// parserPool is a pool for parser objects.
var parserPool = sync.Pool{
	New: func() any {
		return &yyParserImpl{}
	},
}

// zeroParser is a zero-initialized parser to help reinitialize the parser for pooling.
var zeroParser yyParserImpl

// yyParsePooled is a wrapper around yyParse that pools the parser objects. There isn't a
// particularly good reason to use yyParse directly, since it immediately discards its parser.
//
// N.B: Parser pooling means that you CANNOT take references directly to parse stack variables (e.g.
// $$ = &$4) in sql.y rules. You must instead add an intermediate reference like so:
//
//	showCollationFilterOpt := $4
//	$$ = &Show{Type: string($2), ShowCollationFilterOpt: &showCollationFilterOpt}
func yyParsePooled(yylex yyLexer) int {
	parser := parserPool.Get().(*yyParserImpl)
	defer func() {
		*parser = zeroParser
		parserPool.Put(parser)
	}()
	return parser.Parse(yylex)
}

// Instructions for creating new types: If a type
// needs to satisfy an interface, declare that function
// along with that interface. This will help users
// identify the list of types to which they can assert
// those interfaces.
// If the member of a type has a string with a predefined
// list of values, declare those values as const following
// the type.
// For interfaces that define dummy functions to consolidate
// a set of types, define the function as iTypeName.
// This will help avoid name collisions.

// Parse2 parses the SQL in full and returns a Statement, which
// is the AST representation of the query, and a set of BindVars, which are all the
// bind variables that were found in the original SQL query. If a DDL statement
// is partially parsed but still contains a syntax error, the
// error is ignored and the DDL is returned anyway.
func (p *Parser) Parse2(sql string) (Statement, BindVars, error) {
	tokenizer := p.NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 || tokenizer.LastError != nil {
		if tokenizer.partialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return nil, nil, fmt.Errorf("extra characters encountered after end of DDL: '%s'", val)
			}
			log.Warn(fmt.Sprintf("ignoring error parsing DDL '%s': %v", sql, tokenizer.LastError))
			switch x := tokenizer.partialDDL.(type) {
			case DBDDLStatement:
				x.SetFullyParsed(false)
			case DDLStatement:
				x.SetFullyParsed(false)
			}
			tokenizer.ParseTrees = []Statement{tokenizer.partialDDL}
			return tokenizer.ParseTrees[0], tokenizer.BindVars, nil
		}
		return nil, nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, tokenizer.LastError.Error())
	}
	err := checkParseTreesError(tokenizer)
	if err != nil {
		return nil, nil, err
	}
	return tokenizer.ParseTrees[0], tokenizer.BindVars, nil
}

// ParseMultiple parses the SQL in full and returns a list of Statements, which
// are the AST representation of the query. This command is meant to parse more than
// one SQL statement at a time.
func (p *Parser) ParseMultiple(sql string) ([]Statement, error) {
	tokenizer := p.NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		return nil, tokenizer.LastError
	}
	return tokenizer.ParseTrees, nil
}

// ParseMultipleIgnoreEmpty parses multiple statements, but ignores empty statements.
func (p *Parser) ParseMultipleIgnoreEmpty(sql string) ([]Statement, error) {
	stmts, err := p.ParseMultiple(sql)
	if err != nil {
		return nil, err
	}
	newStmts := make([]Statement, 0)
	for _, stmt := range stmts {
		// Only keep non-empty non comment only statements.
		if _, isCommentOnly := stmt.(*CommentOnly); stmt != nil && !isCommentOnly {
			newStmts = append(newStmts, stmt)
		}
	}
	return newStmts, nil
}

// parse parses the SQL in full and returns a list of Statements, which
// are the AST representation of the query. This command is meant to parse more than
// one SQL statement at a time.
func parse(tokenizer *Tokenizer) ([]Statement, error) {
	if yyParsePooled(tokenizer) != 0 {
		return nil, tokenizer.LastError
	}
	return tokenizer.ParseTrees, nil
}

// checkParseTreesError checks for errors that need to be sent based on the parseTrees generated.
func checkParseTreesError(tokenizer *Tokenizer) error {
	if len(tokenizer.ParseTrees) > 1 {
		return ErrMultipleStatements
	}
	if len(tokenizer.ParseTrees) == 0 || tokenizer.ParseTrees[0] == nil {
		return ErrEmpty
	}
	return nil
}

// ConvertMySQLVersionToCommentVersion converts the MySQL version into comment version format.
func ConvertMySQLVersionToCommentVersion(version string) (string, error) {
	res := make([]int, 3)
	idx := 0
	val := ""
	for _, c := range version {
		if c <= '9' && c >= '0' {
			val += string(c)
		} else if c == '.' {
			v, err := strconv.Atoi(val)
			if err != nil {
				return "", err
			}
			val = ""
			res[idx] = v
			idx++
			if idx == 3 {
				break
			}
		} else {
			break
		}
	}
	if val != "" {
		v, err := strconv.Atoi(val)
		if err != nil {
			return "", err
		}
		res[idx] = v
		idx++
	}
	if idx == 0 {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "MySQL version not correctly setup - %s.", version)
	}

	return fmt.Sprintf("%01d%02d%02d", res[0], res[1], res[2]), nil
}

// ParseExpr parses an expression and transforms it to an AST
func (p *Parser) ParseExpr(sql string) (Expr, error) {
	stmt, err := p.Parse("select " + sql)
	if err != nil {
		return nil, err
	}
	aliasedExpr := stmt.(*Select).SelectExprs.Exprs[0].(*AliasedExpr)
	return aliasedExpr.Expr, err
}

// Parse behaves like Parse2 but does not return a set of bind variables
func (p *Parser) Parse(sql string) (Statement, error) {
	stmt, _, err := p.Parse2(sql)
	return stmt, err
}

// ParseStrictDDL is the same as Parse except it errors on
// partially parsed DDL statements.
func (p *Parser) ParseStrictDDL(sql string) (Statement, error) {
	tokenizer := p.NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		return nil, tokenizer.LastError
	}
	err := checkParseTreesError(tokenizer)
	if err != nil {
		return nil, err
	}
	return tokenizer.ParseTrees[0], nil
}

// ErrEmpty is a sentinel error returned when parsing empty statements.
var ErrEmpty = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.EmptyQuery, "Query was empty")

// ErrMultipleStatements is a sentinel error returned when we parsed multiple statements when we were expecting one.
var ErrMultipleStatements = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "Expected a single statement")

// ParseNext parses the first statement of sql the way MySQL's multi-statement
// dispatcher does: the grammar consumes one complete statement and stops at
// the top-level ';' that follows it, so a ';' inside a compound statement
// (a CREATE PROCEDURE body, say) never ends the statement. stmt is nil for an
// empty statement (nothing, or whitespace only; comments alone give a
// *CommentOnly). text is the statement's own text without the ';', leading
// whitespace and comments included. rest is everything after the ';', or
// empty when the input ended with the statement.
//
// A DDL statement that is only partially parsed is accepted the way Parse
// accepts it, marked as not fully parsed and cut at the next top-level ';'.
func (p *Parser) ParseNext(sql string) (stmt Statement, text string, rest string, err error) {
	stmt, text, rest, err = p.parseNext(sql)
	if err != nil {
		return nil, "", "", vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	return stmt, text, rest, nil
}

// parseNext is ParseNext, except that on a syntax error text and rest are
// still returned, cut at the next top-level ';' the tokenizer can find (or
// the end of the input), and err is the tokenizer's own error, without the
// stack trace and formatting of a vterrors error most callers discard.
// SplitStatementToPieces relies on the cut to pass statements the grammar
// does not know on to MySQL unchanged.
func (p *Parser) parseNext(sql string) (stmt Statement, text string, rest string, err error) {
	tokenizer := p.NewStringTokenizer(sql)
	tokenizer.stopAfterFirstStatement = true
	if yyParsePooled(tokenizer) != 0 || tokenizer.LastError != nil {
		if tokenizer.partialDDL != nil && !tokenizer.resyncLexError {
			switch x := tokenizer.partialDDL.(type) {
			case DBDDLStatement:
				x.SetFullyParsed(false)
			case DDLStatement:
				x.SetFullyParsed(false)
			}
			stmt = tokenizer.partialDDL
		} else {
			err = tokenizer.LastError
			tokenizer.resync()
		}
	} else {
		stmt = tokenizer.ParseTrees[0]
	}
	if tokenizer.stmtEnd < 0 {
		return stmt, sql, "", err
	}
	return stmt, sql[:tokenizer.stmtEnd], sql[tokenizer.stmtEnd+1:], err
}

// ForEachStatement hands the statements of sql to fn one at a time, the way
// MySQL's multi-statement dispatcher does: a statement is parsed only when its
// turn comes, after fn has returned for every statement before it, so an
// error from fn ends the batch before anything after it is even parsed. text
// is the statement's own text without its ';', whitespace and comments
// around it included; rest is everything after the ';', empty for the last
// statement. A single statement (at most with a trailing ';') is handed over
// as is, without being parsed.
//
// The parse here only finds the statement's end: a statement the grammar
// rejects is handed to fn as well, cut at the next top-level ';', for fn's
// own parse to report (the way it would have reported it for that statement
// alone). As MySQL does before parsing a COM_QUERY, trailing ';' and
// whitespace are dropped from the whole text first, so "select 1;;" is one
// statement. An empty statement followed by more input is a syntax error, as
// in MySQL. Comments alone are a statement like any other. An input with no
// statement at all is ErrEmpty.
func (p *Parser) ForEachStatement(sql string, fn func(text, rest string) error) error {
	return ForEachStatementWith(sql, func() *Parser { return p }, fn)
}

// ForEachStatementWith is ForEachStatement with the parser chosen afresh before
// each statement's end is looked for, for callers whose parse mode can change
// between the statements of a batch: a SET sql_mode in it applies to the
// statements after it, and where they end depends on the mode.
func ForEachStatementWith(sql string, parserFor func() *Parser, fn func(text, rest string) error) error {
	if strings.Trim(sql, blankChars) == "" {
		return ErrEmpty
	}
	// fast path: a single statement needs no split.
	if end, ok := parserFor().singleStatement(sql); ok {
		if strings.Trim(sql[:end], blankChars) == "" {
			return ErrEmpty
		}
		return fn(sql[:end], "")
	}

	sql = strings.TrimRight(sql, blankChars+";")
	if strings.Trim(sql, blankChars) == "" {
		return ErrEmpty
	}
	offset := 0 // start of the current statement in sql
	for {
		stmt, text, rest, err := parserFor().parseNext(sql[offset:])
		if err == nil && stmt == nil {
			return NewParseErrorNear(sql, offset)
		}
		if err := fn(text, rest); err != nil {
			return err
		}
		if rest == "" {
			return nil
		}
		offset = len(sql) - len(rest)
	}
}

// IsBlankOrComments reports whether sql holds no statement at all: nothing but
// blanks and comments, the way a comment after a statement's terminating ';'
// does not start a second statement. The text is read as text, not lexed: an
// executable comment left after the terminator is a comment here whatever it
// holds, as it is for MySQL's prepare, where a ';' inside one would be a
// syntax error in a statement's own text.
func (p *Parser) IsBlankOrComments(sql string) bool {
	for {
		sql = strings.TrimLeft(sql, blankChars)
		switch {
		case sql == "":
			return true
		case strings.HasPrefix(sql, "/*"):
			end := strings.Index(sql[2:], "*/")
			if end < 0 {
				return false
			}
			sql = sql[2+end+2:]
		case strings.HasPrefix(sql, "#"), strings.HasPrefix(sql, "-- "), strings.HasPrefix(sql, "--\t"), sql == "--", strings.HasPrefix(sql, "--\n"), strings.HasPrefix(sql, "--\r"):
			end := strings.IndexByte(sql, '\n')
			if end < 0 {
				return true
			}
			sql = sql[end+1:]
		default:
			return false
		}
	}
}

// NewParseErrorNear returns MySQL's ER_PARSE_ERROR (1064) for the input that
// starts at offset in sql.
func NewParseErrorNear(sql string, offset int) error {
	const maxNear = 80 // MySQL formats the message with %-.80s
	near := strings.TrimLeft(sql[offset:], blankChars)
	line := 1 + strings.Count(sql[:len(sql)-len(near)], "\n")
	if len(near) > maxNear {
		near = near[:maxNear]
	}
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.ParseError, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line %d", near, line)
}

// singleStatement reports, without the grammar, whether sql is a single
// statement: no ';' token at all, or one that only blanks follow — the
// lexer's blanks, so that a character it does not read as blank is handed
// to the grammar rather than dropped. end is where the statement's text
// ends (len(sql), or the offset of that ';').
// A ';' inside a string, a quoted identifier or a comment is not a token,
// for MySQL's lexer no more than for ours, so such a statement is handed
// over as is instead of being parsed just to find its end. A lexical error
// leaves the question to the grammar.
func (p *Parser) singleStatement(sql string) (end int, ok bool) {
	if strings.IndexByte(sql, ';') == -1 {
		return len(sql), true
	}
	tokenizer := Tokenizer{buf: sql, parser: p, stmtEnd: -1}
	for {
		switch typ, _ := tokenizer.Scan(); typ {
		case ';':
			end = tokenizer.Pos - 1
			return end, strings.Trim(sql[tokenizer.Pos:], blankChars) == ""
		case 0:
			return len(sql), true
		case LEX_ERROR:
			return 0, false
		}
	}
}

// SplitStatement returns the first sql statement up to either a ';' or EOF
// and the remainder from the given buffer. The boundary is the one ParseNext
// finds; a statement the grammar rejects is cut at the next top-level ';'.
// A single statement is not parsed.
func (p *Parser) SplitStatement(blob string) (string, string, error) {
	if end, ok := p.singleStatement(blob); ok {
		if end == len(blob) {
			return blob, "", nil
		}
		return blob[:end], blob[end+1:], nil
	}
	_, sql, rem, _ := p.parseNext(blob)
	return sql, rem, nil
}

// SplitStatementToPieces splits raw sql statement that may have multi sql pieces to sql pieces
// returns the sql pieces blob contains. Statement boundaries come from
// ParseNext; a piece the grammar rejects is cut at the next top-level ';'
// instead of failing, so that callers can pass statements Vitess does not
// parse (CHANGE REPLICATION SOURCE TO, CREATE FUNCTION, ...) on to MySQL.
// Empty and comment-only pieces are dropped.
func (p *Parser) SplitStatementToPieces(blob string) (pieces []string, err error) {
	// fast path: the vast majority of SQL statements do not have semicolons in them
	if blob == "" {
		return nil, nil
	}
	switch strings.IndexByte(blob, ';') {
	case -1: // if there is no semicolon, return blob as a whole
		return []string{blob}, nil
	case len(blob) - 1: // if there's a single semicolon, and it's the last character, return blob without it
		return []string{blob[:len(blob)-1]}, nil
	}

	pieces = make([]string, 0, 16)
	for blob != "" {
		stmt, text, rest, err := p.parseNext(blob)
		if err != nil || !isEmptyStatement(stmt) {
			pieces = append(pieces, text)
		}
		blob = rest
	}
	return pieces, nil
}

// isEmptyStatement is true for the statements ParseNext returns when there
// is nothing to execute: nothing at all, or comments only.
func isEmptyStatement(stmt Statement) bool {
	if stmt == nil {
		return true
	}
	_, isCommentOnly := stmt.(*CommentOnly)
	return isCommentOnly
}

// IsStatementIncomplete returns true if the statement is incomplete: it does
// not parse, and the syntax error is at its very end.
//
// Deprecated: statement boundaries come from ParseNext now, and nothing in
// Vitess calls this anymore. It is kept for downstream users of this package
// and will be removed in a later release.
func (p *Parser) IsStatementIncomplete(stmt string) bool {
	tokenizer := p.NewStringTokenizer(stmt)
	yyParsePooled(tokenizer)
	var pe PositionedErr
	return errors.As(tokenizer.LastError, &pe) && pe.Pos == len(stmt)+1
}

func (p *Parser) IsMySQL80AndAbove() bool {
	return p.version >= "80000"
}

func (p *Parser) SetTruncateErrLen(l int) {
	p.truncateErrLen = l
}

// SQLMode holds the sql_mode flags that change how statements are parsed.
// Only the modes the parser or planner depend on are represented; other
// execution-only modes are ignored. The lexer flags follow MySQL's lexer
// behavior: parsing is mode-dependent, but the resulting AST always formats
// back to mode-independent SQL (e.g. || lowers to concat(), quoted
// identifiers format with backticks), like MySQL's own normalization of
// stored views.
type SQLMode uint32

const (
	// SQLModeANSIQuotes treats "..." as a quoted identifier instead of a
	// string literal.
	SQLModeANSIQuotes SQLMode = 1 << iota
	// SQLModePipesAsConcat treats || as the string concatenation operator
	// instead of logical OR.
	SQLModePipesAsConcat
	// SQLModeIgnoreSpace permits whitespace between a function-name
	// keyword and the opening parenthesis of its call.
	SQLModeIgnoreSpace
	// SQLModeNoBackslashEscapes treats backslash as an ordinary character
	// in string literals instead of an escape character.
	SQLModeNoBackslashEscapes
	// SQLModeHighNotPrecedence gives NOT the precedence of the unary
	// operators, so that NOT a BETWEEN b AND c parses as
	// (NOT a) BETWEEN b AND c.
	SQLModeHighNotPrecedence
	// SQLModeRealAsFloat makes the REAL type a synonym for FLOAT instead
	// of DOUBLE. The lexer ignores it — parsing is identical either way —
	// but planning depends on it (evalengine types CAST(x AS REAL) by it),
	// so it is represented here to key plan caches on it.
	SQLModeRealAsFloat
)

// ParseSQLMode extracts the parse-relevant flags from a MySQL sql_mode
// value. Mode names are matched as whole words, case-insensitively, so
// quoting or expression noise around the names is tolerated. The ANSI
// combination mode expands to the parse-relevant modes it includes.
func ParseSQLMode(sqlMode string) SQLMode {
	var mode SQLMode
	for _, word := range strings.FieldsFunc(sqlMode, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
	}) {
		switch {
		case strings.EqualFold(word, "ANSI_QUOTES"):
			mode |= SQLModeANSIQuotes
		case strings.EqualFold(word, "PIPES_AS_CONCAT"):
			mode |= SQLModePipesAsConcat
		case strings.EqualFold(word, "IGNORE_SPACE"):
			mode |= SQLModeIgnoreSpace
		case strings.EqualFold(word, "NO_BACKSLASH_ESCAPES"):
			mode |= SQLModeNoBackslashEscapes
		case strings.EqualFold(word, "HIGH_NOT_PRECEDENCE"):
			mode |= SQLModeHighNotPrecedence
		case strings.EqualFold(word, "REAL_AS_FLOAT"):
			mode |= SQLModeRealAsFloat
		case strings.EqualFold(word, "ANSI"):
			mode |= SQLModeRealAsFloat | SQLModeANSIQuotes | SQLModePipesAsConcat | SQLModeIgnoreSpace
		}
	}
	return mode
}

// StripUnforwardableModes returns the given comma-separated sql_mode list
// with NO_BACKSLASH_ESCAPES removed — the one mode no serialization can be
// inert under: string literals must escape somehow, and the two backslash
// regimes read any one escaping differently, so a consumer lexing under the
// other regime would read the text differently than it was written. Every
// other mode is forwarded, the ANSI combination, its members, and
// HIGH_NOT_PRECEDENCE included: their lexer aspects are inert on the
// mode-independent SQL Vitess serializes (NOT operands that would bind
// differently under HIGH_NOT_PRECEDENCE are parenthesized), while their
// resolution- and execution-time semantics (e.g. the ANSI aggregate rule,
// ONLY_FULL_GROUP_BY) are the consumer's to enforce. The result is
// deduplicated, preserving first occurrences.
func StripUnforwardableModes(sqlMode string) string {
	var kept []string
	seen := make(map[string]bool)
	for part := range strings.SplitSeq(sqlMode, ",") {
		word := strings.TrimSpace(part)
		switch {
		case word == "":
		case strings.EqualFold(word, "NO_BACKSLASH_ESCAPES"):
		default:
			upper := strings.ToUpper(word)
			if !seen[upper] {
				seen[upper] = true
				kept = append(kept, word)
			}
		}
	}
	return strings.Join(kept, ",")
}

// sqlModeCanonicalOrder lists the sql_mode member names in MySQL's
// canonical (bit) order, which governs how a stored sql_mode value reads
// back. Matches sql_mode_names in MySQL 8.0's sys_vars.cc, with the unused
// placeholder entries omitted.
var sqlModeCanonicalOrder = []string{
	"REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE",
	"ONLY_FULL_GROUP_BY", "NO_UNSIGNED_SUBTRACTION", "NO_DIR_IN_CREATE",
	"ANSI", "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES",
	"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE",
	"NO_ZERO_DATE", "ALLOW_INVALID_DATES", "ERROR_FOR_DIVISION_BY_ZERO",
	"TRADITIONAL", "HIGH_NOT_PRECEDENCE", "NO_ENGINE_SUBSTITUTION",
	"PAD_CHAR_TO_FULL_LENGTH", "TIME_TRUNCATE_FRACTIONAL",
}

// sqlModeCombinations maps the combination modes to the member modes they
// turn on; like in MySQL, the combination's own name stays in the value.
var sqlModeCombinations = map[string][]string{
	"ANSI": {
		"REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES",
		"IGNORE_SPACE", "ONLY_FULL_GROUP_BY",
	},
	"TRADITIONAL": {
		"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES",
		"NO_ZERO_IN_DATE", "NO_ZERO_DATE", "ERROR_FOR_DIVISION_BY_ZERO",
		"NO_ENGINE_SUBSTITUTION",
	},
}

var knownSQLModes = func() map[string]bool {
	known := make(map[string]bool, len(sqlModeCanonicalOrder))
	for _, name := range sqlModeCanonicalOrder {
		known[name] = true
	}
	return known
}()

// KnownSQLMode reports whether the given name is a valid sql_mode member or
// combination name, matched case-insensitively.
func KnownSQLMode(name string) bool {
	return knownSQLModes[strings.ToUpper(strings.TrimSpace(name))]
}

// SQLModeValueList unwraps an optionally quoted sql_mode value and reports
// whether it is a plain comma-separated mode list.
func SQLModeValueList(value string) (string, bool) {
	inner := value
	if len(inner) >= 2 && inner[0] == '\'' && inner[len(inner)-1] == '\'' {
		inner = inner[1 : len(inner)-1]
	}
	for _, r := range inner {
		isWordChar := r == '_' || r == ',' || r == ' ' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !isWordChar {
			return "", false
		}
	}
	return inner, true
}

// CanonicalizeSQLModeValue rewrites a (possibly quoted) sql_mode value the
// way MySQL stores it: names uppercased and deduplicated, combination modes
// expanded to their members while keeping their own name, and the result
// ordered canonically, so that reading @@sql_mode back matches MySQL.
// Values that are not a plain mode list, or that contain unknown mode
// names, are returned unchanged.
func CanonicalizeSQLModeValue(value string) string {
	inner, ok := SQLModeValueList(value)
	if !ok {
		return value
	}
	quoted := inner != value && len(value) >= 2
	set := make(map[string]bool)
	for part := range strings.SplitSeq(inner, ",") {
		word := strings.ToUpper(strings.TrimSpace(part))
		if word == "" {
			continue
		}
		if !knownSQLModes[word] {
			return value
		}
		set[word] = true
		for _, member := range sqlModeCombinations[word] {
			set[member] = true
		}
	}
	var out []string
	for _, name := range sqlModeCanonicalOrder {
		if set[name] {
			out = append(out, name)
		}
	}
	joined := strings.Join(out, ",")
	if quoted {
		return "'" + joined + "'"
	}
	return joined
}

// StripUnforwardableModesValue applies StripUnforwardableModes to a stored
// sql_mode value that may carry surrounding single quotes. Values that are
// not a plain (possibly quoted) mode list — e.g. expressions — are returned
// unchanged.
func StripUnforwardableModesValue(value string) string {
	if !IsSQLModeList(value) {
		return value
	}
	inner, quoted := unquoteSQLModeValue(value)
	stripped := StripUnforwardableModes(inner)
	if quoted {
		return "'" + stripped + "'"
	}
	return stripped
}

// IsSQLModeList reports whether a stored sql_mode value is a plain list of mode
// names — comma-separated, optionally single-quoted — as opposed to an expression
// (REPLACE(@@sql_mode, ...), a user variable, ...) whose modes cannot be read off
// its text. Only a list can be parsed for the modes it enables.
func IsSQLModeList(value string) bool {
	inner, _ := unquoteSQLModeValue(value)
	for _, r := range inner {
		isWordChar := r == '_' || r == ',' || r == ' ' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !isWordChar {
			return false
		}
	}
	return true
}

// unquoteSQLModeValue strips the single quotes of a stored sql_mode value, reporting
// whether there were any.
func unquoteSQLModeValue(value string) (inner string, quoted bool) {
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1], true
	}
	return value, false
}

type Options struct {
	MySQLServerVersion string
	TruncateUILen      int
	TruncateErrLen     int
	SQLMode            SQLMode
}

type Parser struct {
	version        string
	truncateUILen  int
	truncateErrLen int
	sqlMode        SQLMode
}

// WithSQLMode returns a copy of the parser that parses statements according
// to the given parse-relevant sql_mode flags.
func (p *Parser) WithSQLMode(mode SQLMode) *Parser {
	clone := *p
	clone.sqlMode = mode
	return &clone
}

func New(opts Options) (*Parser, error) {
	if opts.MySQLServerVersion == "" {
		opts.MySQLServerVersion = config.DefaultMySQLVersion
	}
	convVersion, err := ConvertMySQLVersionToCommentVersion(opts.MySQLServerVersion)
	if err != nil {
		return nil, err
	}
	return &Parser{
		version:        convVersion,
		truncateUILen:  opts.TruncateUILen,
		truncateErrLen: opts.TruncateErrLen,
		sqlMode:        opts.SQLMode,
	}, nil
}

func NewTestParser() *Parser {
	convVersion, err := ConvertMySQLVersionToCommentVersion(config.DefaultMySQLVersion)
	if err != nil {
		panic(err)
	}
	return &Parser{
		version:        convVersion,
		truncateUILen:  512,
		truncateErrLen: 0,
	}
}
