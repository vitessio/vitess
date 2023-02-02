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
	"io"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var versionFlagSync sync.Once

// parserPool is a pool for parser objects.
var parserPool = sync.Pool{
	New: func() any {
		return &yyParserImpl{}
	},
}

// zeroParser is a zero-initialized parser to help reinitialize the parser for pooling.
var zeroParser yyParserImpl

// mySQLParserVersion is the version of MySQL that the parser would emulate
var mySQLParserVersion string

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
func Parse2(sql string) (Statement, BindVars, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		if tokenizer.partialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return nil, nil, fmt.Errorf("extra characters encountered after end of DDL: '%s'", string(val))
			}
			log.Warningf("ignoring error parsing DDL '%s': %v", sql, tokenizer.LastError)
			switch x := tokenizer.partialDDL.(type) {
			case DBDDLStatement:
				x.SetFullyParsed(false)
			case DDLStatement:
				x.SetFullyParsed(false)
			}
			tokenizer.ParseTree = tokenizer.partialDDL
			return tokenizer.ParseTree, tokenizer.BindVars, nil
		}
		return nil, nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, tokenizer.LastError.Error())
	}
	if tokenizer.ParseTree == nil {
		return nil, nil, ErrEmpty
	}
	return tokenizer.ParseTree, tokenizer.BindVars, nil
}

func checkParserVersionFlag() {
	if flag.Parsed() {
		versionFlagSync.Do(func() {
			convVersion, err := convertMySQLVersionToCommentVersion(servenv.MySQLServerVersion())
			if err != nil {
				log.Fatalf("unable to parse mysql version: %v", err)
			}
			mySQLParserVersion = convVersion
		})
	}
}

// SetParserVersion sets the mysql parser version
func SetParserVersion(version string) {
	mySQLParserVersion = version
}

// GetParserVersion returns the version of the mysql parser
func GetParserVersion() string {
	return mySQLParserVersion
}

// convertMySQLVersionToCommentVersion converts the MySQL version into comment version format.
func convertMySQLVersionToCommentVersion(version string) (string, error) {
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
func ParseExpr(sql string) (Expr, error) {
	stmt, err := Parse("select " + sql)
	if err != nil {
		return nil, err
	}
	aliasedExpr := stmt.(*Select).SelectExprs[0].(*AliasedExpr)
	return aliasedExpr.Expr, err
}

// Parse behaves like Parse2 but does not return a set of bind variables
func Parse(sql string) (Statement, error) {
	stmt, _, err := Parse2(sql)
	return stmt, err
}

// ParseStrictDDL is the same as Parse except it errors on
// partially parsed DDL statements.
func ParseStrictDDL(sql string) (Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		return nil, tokenizer.LastError
	}
	if tokenizer.ParseTree == nil {
		return nil, ErrEmpty
	}
	return tokenizer.ParseTree, nil
}

// ParseTokenizer is a raw interface to parse from the given tokenizer.
// This does not used pooled parsers, and should not be used in general.
func ParseTokenizer(tokenizer *Tokenizer) int {
	return yyParse(tokenizer)
}

// ParseNext parses a single SQL statement from the tokenizer
// returning a Statement which is the AST representation of the query.
// The tokenizer will always read up to the end of the statement, allowing for
// the next call to ParseNext to parse any subsequent SQL statements. When
// there are no more statements to parse, a error of io.EOF is returned.
func ParseNext(tokenizer *Tokenizer) (Statement, error) {
	return parseNext(tokenizer, false)
}

// ParseNextStrictDDL is the same as ParseNext except it errors on
// partially parsed DDL statements.
func ParseNextStrictDDL(tokenizer *Tokenizer) (Statement, error) {
	return parseNext(tokenizer, true)
}

func parseNext(tokenizer *Tokenizer, strict bool) (Statement, error) {
	if tokenizer.cur() == ';' {
		tokenizer.skip(1)
		tokenizer.skipBlank()
	}
	if tokenizer.cur() == eofChar {
		return nil, io.EOF
	}

	tokenizer.reset()
	tokenizer.multi = true
	if yyParsePooled(tokenizer) != 0 {
		if tokenizer.partialDDL != nil && !strict {
			tokenizer.ParseTree = tokenizer.partialDDL
			return tokenizer.ParseTree, nil
		}
		return nil, tokenizer.LastError
	}
	_, isCommentOnly := tokenizer.ParseTree.(*CommentOnly)
	if tokenizer.ParseTree == nil || isCommentOnly {
		return ParseNext(tokenizer)
	}
	return tokenizer.ParseTree, nil
}

// ErrEmpty is a sentinel error returned when parsing empty statements.
var ErrEmpty = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.EmptyQuery, "Query was empty")

// SplitStatement returns the first sql statement up to either a ; or EOF
// and the remainder from the given buffer
func SplitStatement(blob string) (string, string, error) {
	tokenizer := NewStringTokenizer(blob)
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

// SplitStatementToPieces split raw sql statement that may have multi sql pieces to sql pieces
// returns the sql pieces blob contains; or error if sql cannot be parsed
func SplitStatementToPieces(blob string) (pieces []string, err error) {
	// fast path: the vast majority of SQL statements do not have semicolons in them
	if blob == "" {
		return nil, nil
	}
	switch strings.IndexByte(blob, ';') {
	case -1: // if there is no semicolon, return blob as a whole
		return []string{blob}, nil
	case len(blob) - 1: // if there's a single semicolon and it's the last character, return blob without it
		return []string{blob[:len(blob)-1]}, nil
	}

	pieces = make([]string, 0, 16)
	tokenizer := NewStringTokenizer(blob)

	tkn := 0
	var stmt string
	stmtBegin := 0
	emptyStatement := true
loop:
	for {
		tkn, _ = tokenizer.Scan()
		switch tkn {
		case ';':
			stmt = blob[stmtBegin : tokenizer.Pos-1]
			if !emptyStatement {
				pieces = append(pieces, stmt)
				emptyStatement = true
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
		default:
			emptyStatement = false
		}
	}

	err = tokenizer.LastError
	return
}

func IsMySQL80AndAbove() bool {
	return mySQLParserVersion >= "80000"
}
