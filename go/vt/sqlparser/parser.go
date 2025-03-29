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
			log.Warningf("ignoring error parsing DDL '%s': %v", sql, tokenizer.LastError)
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
	var res = make([]int, 3)
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
	var prevToken int
	var isCreateProcedureStatement bool
loop:
	for {
		tkn, _ = tokenizer.Scan()
		switch tkn {
		case ';':
			stmt = blob[stmtBegin : tokenizer.Pos-1]
			// We now try to parse the statement to see if its complete.
			// If it is a create procedure, then it might not be complete, and we
			// would need to scan to the next ;
			if isCreateProcedureStatement && p.IsStatementIncomplete(stmt) {
				continue
			}
			if !emptyStatement {
				pieces = append(pieces, stmt)
				// We can now reset the variables for the next statement.
				// It starts off as an empty statement and we don't know if it is
				// a create procedure statement yet.
				emptyStatement = true
				isCreateProcedureStatement = false
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
			// We want to ignore comments and not store them in the prevToken for knowing
			// if the current statement is a create procedure statement.
			continue
		case PROCEDURE:
			if prevToken == CREATE {
				isCreateProcedureStatement = true
			}
			fallthrough
		default:
			prevToken = tkn
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

type Options struct {
	MySQLServerVersion string
	TruncateUILen      int
	TruncateErrLen     int
}

type Parser struct {
	version        string
	truncateUILen  int
	truncateErrLen int
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
