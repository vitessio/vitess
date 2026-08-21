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

// SplitStatement returns the first sql statement up to either a ';' or EOF
// and the remainder from the given buffer
func (p *Parser) SplitStatement(blob string) (string, string, error) {
	tokenizer := p.NewStringTokenizer(blob)
	tkn := 0
	for {
		tkn, _ = tokenizer.Scan()
		if tkn == 0 || tkn == ';' || tkn == eofChar {
			break
		}
	}
	if tokenizer.LastError != nil {
		return "", "", tokenizer.LastError
	}
	if tkn == ';' {
		return blob[:tokenizer.Pos-1], blob[tokenizer.Pos:], nil
	}
	return blob, "", nil
}

var validCreatePrefixes = [][]int{
	// These are the tokens (in order) for valid "create procedure" forms.
	{CREATE, PROCEDURE},
	{CREATE, DEFINER, '=', CURRENT_USER, PROCEDURE},
	{CREATE, DEFINER, '=', CURRENT_USER, '(', ')', PROCEDURE},
	{CREATE, DEFINER, '=', STRING, PROCEDURE},
	{CREATE, DEFINER, '=', STRING, AT_ID, PROCEDURE},
	{CREATE, DEFINER, '=', ID, PROCEDURE},
	{CREATE, DEFINER, '=', ID, AT_ID, PROCEDURE},
}

// matchesCreateProcedurePrefix checks if the given token sequence
// is a create procedure statement or not.
func matchesCreateProcedurePrefix(tokens []int) bool {
	// Check each candidate sequence.
	for _, pattern := range validCreatePrefixes {
		if len(tokens) >= len(pattern) {
			match := true
			for i, tok := range pattern {
				if tokens[i] != tok {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

// SplitStatementToPieces splits raw sql statement that may have multi sql pieces to sql pieces
// returns the sql pieces blob contains; or error if sql cannot be parsed.
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
	tokenizer := p.NewStringTokenizer(blob)

	tkn := 0
	var stmt string
	stmtBegin := 0
	emptyStatement := true
	var startTokens []int // holds the first tokens of the current statement

loop:
	for {
		tkn, _ = tokenizer.Scan()
		switch tkn {
		case ';':
			// Potential end of the statement.
			stmt = blob[stmtBegin : tokenizer.Pos-1]
			// If it's a create procedure statement and is incomplete, skip appending.
			if matchesCreateProcedurePrefix(startTokens) && p.IsStatementIncomplete(stmt) {
				continue
			}
			if !emptyStatement {
				pieces = append(pieces, stmt)
				// We can now reset the variables for the next statement.
				// It starts off as an empty statement.
				emptyStatement = true
				startTokens = startTokens[:0] // clear token slice
			}
			stmtBegin = tokenizer.Pos
		case 0, eofChar:
			blobTail := tokenizer.Pos - 1
			if stmtBegin < blobTail {
				stmt = blob[stmtBegin : blobTail+1]
				if !emptyStatement {
					pieces = append(pieces, stmt)
				}
			}
			break loop
		case COMMENT:
			// Skip comments entirely without altering the token list.
			continue
		default:
			// If we're at the very start of a statement, or we haven't filled out enough tokens
			// for our valid prefix match (assuming our longest valid sequence is 10 tokens),
			// accumulate the token.
			if len(startTokens) < 10 {
				startTokens = append(startTokens, tkn)
			}
			emptyStatement = false
		}
	}

	err = tokenizer.LastError
	return
}

// IsStatementIncomplete returns true if the statement is incomplete.
func (p *Parser) IsStatementIncomplete(stmt string) bool {
	tkn := p.NewStringTokenizer(stmt)
	yyParsePooled(tkn)
	if tkn.LastError != nil {
		var pe PositionedErr
		isPe := errors.As(tkn.LastError, &pe)
		if isPe && pe.Pos == len(stmt)+1 {
			// The error is at the end of the statement, which means it is incomplete.
			return true
		}
	}
	return false
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
// with NO_BACKSLASH_ESCAPES and HIGH_NOT_PRECEDENCE removed — the two modes
// whose meaning is fully absorbed by the parse-and-reserialize round trip:
// serialized SQL escapes string literals with backslashes and prints NOT
// without defensive parentheses, so a consumer lexing under either mode
// would read that text differently than it was written. Every other mode is
// forwarded, the ANSI combination and its members included: their lexer
// aspects are inert on the mode-independent SQL Vitess serializes, while
// their resolution- and execution-time semantics (e.g. the ANSI aggregate
// rule, ONLY_FULL_GROUP_BY) are the consumer's to enforce. The result is
// deduplicated, preserving first occurrences.
func StripUnforwardableModes(sqlMode string) string {
	var kept []string
	seen := make(map[string]bool)
	for part := range strings.SplitSeq(sqlMode, ",") {
		word := strings.TrimSpace(part)
		switch {
		case word == "":
		case strings.EqualFold(word, "NO_BACKSLASH_ESCAPES"),
			strings.EqualFold(word, "HIGH_NOT_PRECEDENCE"):
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
	inner := value
	quoted := len(inner) >= 2 && inner[0] == '\'' && inner[len(inner)-1] == '\''
	if quoted {
		inner = inner[1 : len(inner)-1]
	}
	for _, r := range inner {
		isWordChar := r == '_' || r == ',' || r == ' ' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !isWordChar {
			return value
		}
	}
	stripped := StripUnforwardableModes(inner)
	if quoted {
		return "'" + stripped + "'"
	}
	return stripped
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
