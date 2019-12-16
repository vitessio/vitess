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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// parserPool is a pool for parser objects.
var parserPool = sync.Pool{}

// zeroParser is a zero-initialized parser to help reinitialize the parser for pooling.
var zeroParser = *(yyNewParser().(*yyParserImpl))

// yyParsePooled is a wrapper around yyParse that pools the parser objects. There isn't a
// particularly good reason to use yyParse directly, since it immediately discards its parser.  What
// would be ideal down the line is to actually pool the stacks themselves rather than the parser
// objects, as per https://github.com/cznic/goyacc/blob/master/main.go. However, absent an upstream
// change to goyacc, this is the next best option.
//
// N.B: Parser pooling means that you CANNOT take references directly to parse stack variables (e.g.
// $$ = &$4) in sql.y rules. You must instead add an intermediate reference like so:
//    showCollationFilterOpt := $4
//    $$ = &Show{Type: string($2), ShowCollationFilterOpt: &showCollationFilterOpt}
func yyParsePooled(yylex yyLexer) int {
	// Being very particular about using the base type and not an interface type b/c we depend on
	// the implementation to know how to reinitialize the parser.
	var parser *yyParserImpl

	i := parserPool.Get()
	if i != nil {
		parser = i.(*yyParserImpl)
	} else {
		parser = yyNewParser().(*yyParserImpl)
	}

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

// Parse parses the SQL in full and returns a Statement, which
// is the AST representation of the query. If a DDL statement
// is partially parsed but still contains a syntax error, the
// error is ignored and the DDL is returned anyway.
func Parse(sql string) (Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		if tokenizer.partialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return nil, fmt.Errorf("extra characters encountered after end of DDL: '%s'", string(val))
			}
			log.Warningf("ignoring error parsing DDL '%s': %v", sql, tokenizer.LastError)
			tokenizer.ParseTree = tokenizer.partialDDL
			return tokenizer.ParseTree, nil
		}
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, tokenizer.LastError.Error())
	}
	if tokenizer.ParseTree == nil {
		return nil, ErrEmpty
	}
	return tokenizer.ParseTree, nil
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
	if tokenizer.lastChar == ';' {
		tokenizer.next()
		tokenizer.skipBlank()
	}
	if tokenizer.lastChar == eofChar {
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
	if tokenizer.ParseTree == nil {
		return ParseNext(tokenizer)
	}
	return tokenizer.ParseTree, nil
}

// ErrEmpty is a sentinel error returned when parsing empty statements.
var ErrEmpty = errors.New("empty statement")

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
		return blob[:tokenizer.Position-2], blob[tokenizer.Position-1:], nil
	}
	return blob, "", nil
}

// SplitStatementToPieces split raw sql statement that may have multi sql pieces to sql pieces
// returns the sql pieces blob contains; or error if sql cannot be parsed
func SplitStatementToPieces(blob string) (pieces []string, err error) {
	pieces = make([]string, 0, 16)
	tokenizer := NewStringTokenizer(blob)

	tkn := 0
	var stmt string
	stmtBegin := 0
	for {
		tkn, _ = tokenizer.Scan()
		if tkn == ';' {
			stmt = blob[stmtBegin : tokenizer.Position-2]
			pieces = append(pieces, stmt)
			stmtBegin = tokenizer.Position - 1

		} else if tkn == 0 || tkn == eofChar {
			blobTail := tokenizer.Position - 2

			if stmtBegin < blobTail {
				stmt = blob[stmtBegin : blobTail+1]
				pieces = append(pieces, stmt)
			}
			break
		}
	}

	err = tokenizer.LastError
	return
}

// SQLNode defines the interface for all nodes
// generated by the parser.
type SQLNode interface {
	Format(buf *TrackedBuffer)
	// walkSubtree calls visit on all underlying nodes
	// of the subtree, but not the current one. Walking
	// must be interrupted if visit returns an error.
	walkSubtree(visit Visit) error
}

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node SQLNode) (kontinue bool, err error)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...SQLNode) error {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		kontinue, err := visit(node)
		if err != nil {
			return err
		}
		if kontinue {
			err = node.walkSubtree(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// String returns a string representation of an SQLNode.
func String(node SQLNode) string {
	if node == nil {
		return "<nil>"
	}

	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%v", node)
	return buf.String()
}

// Append appends the SQLNode to the buffer.
func Append(buf *strings.Builder, node SQLNode) {
	tbuf := &TrackedBuffer{
		Builder: buf,
	}
	node.Format(tbuf)
}

// Statements
type (
	// Statement represents a statement.
	Statement interface {
		iStatement()
		SQLNode
	}

	// SelectStatement any SELECT statement.
	SelectStatement interface {
		iSelectStatement()
		iStatement()
		iInsertRows()
		AddOrder(*Order)
		SetLimit(*Limit)
		SQLNode
	}

	// Select represents a SELECT statement.
	Select struct {
		Cache       string
		Comments    Comments
		Distinct    string
		Hints       string
		SelectExprs SelectExprs
		From        TableExprs
		Where       *Where
		GroupBy     GroupBy
		Having      *Where
		OrderBy     OrderBy
		Limit       *Limit
		Lock        string
	}

	// Union represents a UNION statement.
	Union struct {
		Type        string
		Left, Right SelectStatement
		OrderBy     OrderBy
		Limit       *Limit
		Lock        string
	}

	// Stream represents a SELECT statement.
	Stream struct {
		Comments   Comments
		SelectExpr SelectExpr
		Table      TableName
	}

	// Insert represents an INSERT or REPLACE statement.
	// Per the MySQL docs, http://dev.mysql.com/doc/refman/5.7/en/replace.html
	// Replace is the counterpart to `INSERT IGNORE`, and works exactly like a
	// normal INSERT except if the row exists. In that case it first deletes
	// the row and re-inserts with new values. For that reason we keep it as an Insert struct.
	// Replaces are currently disallowed in sharded schemas because
	// of the implications the deletion part may have on vindexes.
	// If you add fields here, consider adding them to calls to validateUnshardedRoute.
	Insert struct {
		Action     string
		Comments   Comments
		Ignore     string
		Table      TableName
		Partitions Partitions
		Columns    Columns
		Rows       InsertRows
		OnDup      OnDup
	}

	// Update represents an UPDATE statement.
	// If you add fields here, consider adding them to calls to validateUnshardedRoute.
	Update struct {
		Comments   Comments
		Ignore     string
		TableExprs TableExprs
		Exprs      UpdateExprs
		Where      *Where
		OrderBy    OrderBy
		Limit      *Limit
	}

	// Delete represents a DELETE statement.
	// If you add fields here, consider adding them to calls to validateUnshardedRoute.
	Delete struct {
		Comments   Comments
		Targets    TableNames
		TableExprs TableExprs
		Partitions Partitions
		Where      *Where
		OrderBy    OrderBy
		Limit      *Limit
	}

	// Set represents a SET statement.
	Set struct {
		Comments Comments
		Exprs    SetExprs
		Scope    string
	}

	// DBDDL represents a CREATE, DROP database statement.
	DBDDL struct {
		Action   string
		DBName   string
		IfExists bool
		Collate  string
		Charset  string
	}

	// DDL represents a CREATE, ALTER, DROP, RENAME, TRUNCATE or ANALYZE statement.
	DDL struct {
		Action string

		// FromTables is set if Action is RenameStr or DropStr.
		FromTables TableNames

		// ToTables is set if Action is RenameStr.
		ToTables TableNames

		// Table is set if Action is other than RenameStr or DropStr.
		Table TableName

		// The following fields are set if a DDL was fully analyzed.
		IfExists      bool
		TableSpec     *TableSpec
		OptLike       *OptLike
		PartitionSpec *PartitionSpec

		// VindexSpec is set for CreateVindexStr, DropVindexStr, AddColVindexStr, DropColVindexStr.
		VindexSpec *VindexSpec

		// VindexCols is set for AddColVindexStr.
		VindexCols []ColIdent

		// AutoIncSpec is set for AddAutoIncStr.
		AutoIncSpec *AutoIncSpec
	}

	// ParenSelect is a parenthesized SELECT statement.
	ParenSelect struct {
		Select SelectStatement
	}

	// Show represents a show statement.
	Show struct {
		Type                   string
		OnTable                TableName
		Table                  TableName
		ShowTablesOpt          *ShowTablesOpt
		Scope                  string
		ShowCollationFilterOpt *Expr
	}

	// Use represents a use statement.
	Use struct {
		DBName TableIdent
	}

	// Begin represents a Begin statement.
	Begin struct{}

	// Commit represents a Commit statement.
	Commit struct{}

	// Rollback represents a Rollback statement.
	Rollback struct{}

	// OtherRead represents a DESCRIBE, or EXPLAIN statement.
	// It should be used only as an indicator. It does not contain
	// the full AST for the statement.
	OtherRead struct{}

	// OtherAdmin represents a misc statement that relies on ADMIN privileges,
	// such as REPAIR, OPTIMIZE, or TRUNCATE statement.
	// It should be used only as an indicator. It does not contain
	// the full AST for the statement.
	OtherAdmin struct{}
)

func (*Union) iStatement()             {}
func (*Select) iStatement()            {}
func (*Stream) iStatement()            {}
func (*Insert) iStatement()            {}
func (*Update) iStatement()            {}
func (*Delete) iStatement()            {}
func (*Set) iStatement()               {}
func (*DBDDL) iStatement()             {}
func (*DDL) iStatement()               {}
func (*Show) iStatement()              {}
func (*Use) iStatement()               {}
func (*Begin) iStatement()             {}
func (*Commit) iStatement()            {}
func (*Rollback) iStatement()          {}
func (*OtherRead) iStatement()         {}
func (*OtherAdmin) iStatement()        {}
func (*Select) iSelectStatement()      {}
func (*Union) iSelectStatement()       {}
func (*ParenSelect) iSelectStatement() {}

// ParenSelect can actually not be a top level statement,
// but we have to allow it because it's a requirement
// of SelectStatement.
func (*ParenSelect) iStatement() {}

// Select.Distinct
const (
	DistinctStr      = "distinct "
	StraightJoinHint = "straight_join "
)

// Select.Lock
const (
	ForUpdateStr = " for update"
	ShareModeStr = " lock in share mode"
)

// Select.Cache
const (
	SQLCacheStr   = "sql_cache "
	SQLNoCacheStr = "sql_no_cache "
)

// AddOrder adds an order by element
func (node *Select) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetLimit sets the limit clause
func (node *Select) SetLimit(limit *Limit) {
	node.Limit = limit
}

func (node *Select) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.SelectExprs,
		node.From,
		node.Where,
		node.GroupBy,
		node.Having,
		node.OrderBy,
		node.Limit,
	)
}

// AddWhere adds the boolean expression to the
// WHERE clause as an AND condition. If the expression
// is an OR clause, it parenthesizes it. Currently,
// the OR operator is the only one that's lower precedence
// than AND.
func (node *Select) AddWhere(expr Expr) {
	if _, ok := expr.(*OrExpr); ok {
		expr = &ParenExpr{Expr: expr}
	}
	if node.Where == nil {
		node.Where = &Where{
			Type: WhereStr,
			Expr: expr,
		}
		return
	}
	node.Where.Expr = &AndExpr{
		Left:  node.Where.Expr,
		Right: expr,
	}
}

// AddHaving adds the boolean expression to the
// HAVING clause as an AND condition. If the expression
// is an OR clause, it parenthesizes it. Currently,
// the OR operator is the only one that's lower precedence
// than AND.
func (node *Select) AddHaving(expr Expr) {
	if _, ok := expr.(*OrExpr); ok {
		expr = &ParenExpr{Expr: expr}
	}
	if node.Having == nil {
		node.Having = &Where{
			Type: HavingStr,
			Expr: expr,
		}
		return
	}
	node.Having.Expr = &AndExpr{
		Left:  node.Having.Expr,
		Right: expr,
	}
}

// AddOrder adds an order by element
func (node *ParenSelect) AddOrder(order *Order) {
	panic("unreachable")
}

// SetLimit sets the limit clause
func (node *ParenSelect) SetLimit(limit *Limit) {
	panic("unreachable")
}

func (node *ParenSelect) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Select,
	)
}

// Union.Type
const (
	UnionStr         = "union"
	UnionAllStr      = "union all"
	UnionDistinctStr = "union distinct"
)

// AddOrder adds an order by element
func (node *Union) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetLimit sets the limit clause
func (node *Union) SetLimit(limit *Limit) {
	node.Limit = limit
}

func (node *Union) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
	)
}

func (node *Stream) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.SelectExpr,
		node.Table,
	)
}

// DDL strings.
const (
	InsertStr  = "insert"
	ReplaceStr = "replace"
)

func (node *Insert) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.Table,
		node.Columns,
		node.Rows,
		node.OnDup,
	)
}

// InsertRows represents the rows for an INSERT statement.
type InsertRows interface {
	iInsertRows()
	SQLNode
}

func (*Select) iInsertRows()      {}
func (*Union) iInsertRows()       {}
func (Values) iInsertRows()       {}
func (*ParenSelect) iInsertRows() {}

func (node *Update) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.TableExprs,
		node.Exprs,
		node.Where,
		node.OrderBy,
		node.Limit,
	)
}

func (node *Delete) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.Targets,
		node.TableExprs,
		node.Where,
		node.OrderBy,
		node.Limit,
	)
}

// Set.Scope or Show.Scope
const (
	SessionStr        = "session"
	GlobalStr         = "global"
	VitessMetadataStr = "vitess_metadata"
	ImplicitStr       = ""
)

func (node *Set) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.Exprs,
	)
}

// walkSubtree walks the nodes of the subtree.
func (node *DBDDL) walkSubtree(visit Visit) error {
	return nil
}

// DDL strings.
const (
	CreateStr           = "create"
	AlterStr            = "alter"
	DropStr             = "drop"
	RenameStr           = "rename"
	TruncateStr         = "truncate"
	FlushStr            = "flush"
	CreateVindexStr     = "create vindex"
	DropVindexStr       = "drop vindex"
	AddVschemaTableStr  = "add vschema table"
	DropVschemaTableStr = "drop vschema table"
	AddColVindexStr     = "on table add vindex"
	DropColVindexStr    = "on table drop vindex"
	AddSequenceStr      = "add sequence"
	AddAutoIncStr       = "add auto_increment"

	// Vindex DDL param to specify the owner of a vindex
	VindexOwnerStr = "owner"
)

func (node *DDL) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	for _, t := range node.AffectedTables() {
		if err := Walk(visit, t); err != nil {
			return err
		}
	}
	return nil
}

// AffectedTables returns the list table names affected by the DDL.
func (node *DDL) AffectedTables() TableNames {
	if node.Action == RenameStr || node.Action == DropStr {
		list := make(TableNames, 0, len(node.FromTables)+len(node.ToTables))
		list = append(list, node.FromTables...)
		list = append(list, node.ToTables...)
		return list
	}
	return TableNames{node.Table}
}

// Partition strings
const (
	ReorganizeStr = "reorganize partition"
)

// OptLike works for create table xxx like xxx
type OptLike struct {
	LikeTable TableName
}

func (node *OptLike) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.LikeTable)
}

// PartitionSpec describe partition actions (for alter and create)
type PartitionSpec struct {
	Action      string
	Name        ColIdent
	Definitions []*PartitionDefinition
}

func (node *PartitionSpec) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	if err := Walk(visit, node.Name); err != nil {
		return err
	}
	for _, def := range node.Definitions {
		if err := Walk(visit, def); err != nil {
			return err
		}
	}
	return nil
}

// PartitionDefinition describes a very minimal partition definition
type PartitionDefinition struct {
	Name     ColIdent
	Limit    Expr
	Maxvalue bool
}

func (node *PartitionDefinition) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Name,
		node.Limit,
	)
}

// TableSpec describes the structure of a table from a CREATE TABLE statement
type TableSpec struct {
	Columns     []*ColumnDefinition
	Indexes     []*IndexDefinition
	Constraints []*ConstraintDefinition
	Options     string
}

// AddColumn appends the given column to the list in the spec
func (ts *TableSpec) AddColumn(cd *ColumnDefinition) {
	ts.Columns = append(ts.Columns, cd)
}

// AddIndex appends the given index to the list in the spec
func (ts *TableSpec) AddIndex(id *IndexDefinition) {
	ts.Indexes = append(ts.Indexes, id)
}

// AddConstraint appends the given index to the list in the spec
func (ts *TableSpec) AddConstraint(cd *ConstraintDefinition) {
	ts.Constraints = append(ts.Constraints, cd)
}

func (ts *TableSpec) walkSubtree(visit Visit) error {
	if ts == nil {
		return nil
	}

	for _, n := range ts.Columns {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	for _, n := range ts.Indexes {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	for _, n := range ts.Constraints {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// ColumnDefinition describes a column in a CREATE TABLE statement
type ColumnDefinition struct {
	Name ColIdent
	Type ColumnType
}

func (col *ColumnDefinition) walkSubtree(visit Visit) error {
	if col == nil {
		return nil
	}
	return Walk(
		visit,
		col.Name,
		&col.Type,
	)
}

// ColumnType represents a sql type in a CREATE TABLE statement
// All optional fields are nil if not specified
type ColumnType struct {
	// The base type string
	Type string

	// Generic field options.
	NotNull       BoolVal
	Autoincrement BoolVal
	Default       Expr
	OnUpdate      Expr
	Comment       *SQLVal

	// Numeric field options
	Length   *SQLVal
	Unsigned BoolVal
	Zerofill BoolVal
	Scale    *SQLVal

	// Text field options
	Charset string
	Collate string

	// Enum values
	EnumValues []string

	// Key specification
	KeyOpt ColumnKeyOption
}

// DescribeType returns the abbreviated type information as required for
// describe table
func (ct *ColumnType) DescribeType() string {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%s", ct.Type)
	if ct.Length != nil && ct.Scale != nil {
		buf.Myprintf("(%v,%v)", ct.Length, ct.Scale)
	} else if ct.Length != nil {
		buf.Myprintf("(%v)", ct.Length)
	}

	opts := make([]string, 0, 16)
	if ct.Unsigned {
		opts = append(opts, keywordStrings[UNSIGNED])
	}
	if ct.Zerofill {
		opts = append(opts, keywordStrings[ZEROFILL])
	}
	if len(opts) != 0 {
		buf.Myprintf(" %s", strings.Join(opts, " "))
	}
	return buf.String()
}

// SQLType returns the sqltypes type code for the given column
func (ct *ColumnType) SQLType() querypb.Type {
	switch ct.Type {
	case keywordStrings[TINYINT]:
		if ct.Unsigned {
			return sqltypes.Uint8
		}
		return sqltypes.Int8
	case keywordStrings[SMALLINT]:
		if ct.Unsigned {
			return sqltypes.Uint16
		}
		return sqltypes.Int16
	case keywordStrings[MEDIUMINT]:
		if ct.Unsigned {
			return sqltypes.Uint24
		}
		return sqltypes.Int24
	case keywordStrings[INT]:
		fallthrough
	case keywordStrings[INTEGER]:
		if ct.Unsigned {
			return sqltypes.Uint32
		}
		return sqltypes.Int32
	case keywordStrings[BIGINT]:
		if ct.Unsigned {
			return sqltypes.Uint64
		}
		return sqltypes.Int64
	case keywordStrings[BOOL], keywordStrings[BOOLEAN]:
		return sqltypes.Uint8
	case keywordStrings[TEXT]:
		return sqltypes.Text
	case keywordStrings[TINYTEXT]:
		return sqltypes.Text
	case keywordStrings[MEDIUMTEXT]:
		return sqltypes.Text
	case keywordStrings[LONGTEXT]:
		return sqltypes.Text
	case keywordStrings[BLOB]:
		return sqltypes.Blob
	case keywordStrings[TINYBLOB]:
		return sqltypes.Blob
	case keywordStrings[MEDIUMBLOB]:
		return sqltypes.Blob
	case keywordStrings[LONGBLOB]:
		return sqltypes.Blob
	case keywordStrings[CHAR]:
		return sqltypes.Char
	case keywordStrings[VARCHAR]:
		return sqltypes.VarChar
	case keywordStrings[BINARY]:
		return sqltypes.Binary
	case keywordStrings[VARBINARY]:
		return sqltypes.VarBinary
	case keywordStrings[DATE]:
		return sqltypes.Date
	case keywordStrings[TIME]:
		return sqltypes.Time
	case keywordStrings[DATETIME]:
		return sqltypes.Datetime
	case keywordStrings[TIMESTAMP]:
		return sqltypes.Timestamp
	case keywordStrings[YEAR]:
		return sqltypes.Year
	case keywordStrings[FLOAT_TYPE]:
		return sqltypes.Float32
	case keywordStrings[DOUBLE]:
		return sqltypes.Float64
	case keywordStrings[DECIMAL]:
		return sqltypes.Decimal
	case keywordStrings[BIT]:
		return sqltypes.Bit
	case keywordStrings[ENUM]:
		return sqltypes.Enum
	case keywordStrings[SET]:
		return sqltypes.Set
	case keywordStrings[JSON]:
		return sqltypes.TypeJSON
	case keywordStrings[GEOMETRY]:
		return sqltypes.Geometry
	case keywordStrings[POINT]:
		return sqltypes.Geometry
	case keywordStrings[LINESTRING]:
		return sqltypes.Geometry
	case keywordStrings[POLYGON]:
		return sqltypes.Geometry
	case keywordStrings[GEOMETRYCOLLECTION]:
		return sqltypes.Geometry
	case keywordStrings[MULTIPOINT]:
		return sqltypes.Geometry
	case keywordStrings[MULTILINESTRING]:
		return sqltypes.Geometry
	case keywordStrings[MULTIPOLYGON]:
		return sqltypes.Geometry
	}
	panic("unimplemented type " + ct.Type)
}

func (ct *ColumnType) walkSubtree(visit Visit) error {
	return nil
}

// IndexDefinition describes an index in a CREATE TABLE statement
type IndexDefinition struct {
	Info    *IndexInfo
	Columns []*IndexColumn
	Options []*IndexOption
}

func (idx *IndexDefinition) walkSubtree(visit Visit) error {
	if idx == nil {
		return nil
	}

	for _, n := range idx.Columns {
		if err := Walk(visit, n.Column); err != nil {
			return err
		}
	}

	return nil
}

// IndexInfo describes the name and type of an index in a CREATE TABLE statement
type IndexInfo struct {
	Type    string
	Name    ColIdent
	Primary bool
	Spatial bool
	Unique  bool
}

func (ii *IndexInfo) walkSubtree(visit Visit) error {
	return Walk(visit, ii.Name)
}

// IndexColumn describes a column in an index definition with optional length
type IndexColumn struct {
	Column ColIdent
	Length *SQLVal
}

// LengthScaleOption is used for types that have an optional length
// and scale
type LengthScaleOption struct {
	Length *SQLVal
	Scale  *SQLVal
}

// IndexOption is used for trailing options for indexes: COMMENT, KEY_BLOCK_SIZE, USING
type IndexOption struct {
	Name  string
	Value *SQLVal
	Using string
}

// ColumnKeyOption indicates whether or not the given column is defined as an
// index element and contains the type of the option
type ColumnKeyOption int

const (
	colKeyNone ColumnKeyOption = iota
	colKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
)

// VindexSpec defines a vindex for a CREATE VINDEX or DROP VINDEX statement
type VindexSpec struct {
	Name   ColIdent
	Type   ColIdent
	Params []VindexParam
}

// AutoIncSpec defines and autoincrement value for a ADD AUTO_INCREMENT statement
type AutoIncSpec struct {
	Column   ColIdent
	Sequence TableName
}

func (node *AutoIncSpec) walkSubtree(visit Visit) error {
	err := Walk(visit, node.Sequence, node.Column)
	return err
}

// ParseParams parses the vindex parameter list, pulling out the special-case
// "owner" parameter
func (node *VindexSpec) ParseParams() (string, map[string]string) {
	var owner string
	params := map[string]string{}
	for _, p := range node.Params {
		if p.Key.Lowered() == VindexOwnerStr {
			owner = p.Val
		} else {
			params[p.Key.String()] = p.Val
		}
	}
	return owner, params
}

func (node *VindexSpec) walkSubtree(visit Visit) error {
	err := Walk(visit,
		node.Name,
	)

	if err != nil {
		return err
	}

	for _, p := range node.Params {
		err := Walk(visit, p)

		if err != nil {
			return err
		}
	}
	return nil
}

// VindexParam defines a key/value parameter for a CREATE VINDEX statement
type VindexParam struct {
	Key ColIdent
	Val string
}

func (node VindexParam) walkSubtree(visit Visit) error {
	return Walk(visit,
		node.Key,
	)
}

// ConstraintDefinition describes a constraint in a CREATE TABLE statement
type ConstraintDefinition struct {
	Name    string
	Details ConstraintInfo
}

// ConstraintInfo details a constraint in a CREATE TABLE statement
type ConstraintInfo interface {
	SQLNode
	constraintInfo()
}

func (c *ConstraintDefinition) walkSubtree(visit Visit) error {
	return Walk(visit, c.Details)
}

// ReferenceAction indicates the action takes by a referential constraint e.g.
// the `CASCADE` in a `FOREIGN KEY .. ON DELETE CASCADE` table definition.
type ReferenceAction int

// These map to the SQL-defined reference actions.
// See https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html#foreign-keys-referential-actions
const (
	// DefaultAction indicates no action was explicitly specified.
	DefaultAction ReferenceAction = iota
	Restrict
	Cascade
	NoAction
	SetNull
	SetDefault
)

func (a ReferenceAction) walkSubtree(visit Visit) error { return nil }

// ForeignKeyDefinition describes a foreign key in a CREATE TABLE statement
type ForeignKeyDefinition struct {
	Source            Columns
	ReferencedTable   TableName
	ReferencedColumns Columns
	OnDelete          ReferenceAction
	OnUpdate          ReferenceAction
}

var _ ConstraintInfo = &ForeignKeyDefinition{}

func (f *ForeignKeyDefinition) constraintInfo() {}

func (f *ForeignKeyDefinition) walkSubtree(visit Visit) error {
	if err := Walk(visit, f.Source); err != nil {
		return err
	}
	if err := Walk(visit, f.ReferencedTable); err != nil {
		return err
	}
	return Walk(visit, f.ReferencedColumns)
}

// HasOnTable returns true if the show statement has an "on" clause
func (node *Show) HasOnTable() bool {
	return node.OnTable.Name.v != ""
}

// HasTable returns true if the show statement has a parsed table name.
// Not all show statements parse table names.
func (node *Show) HasTable() bool {
	return node.Table.Name.v != ""
}

func (node *Show) walkSubtree(visit Visit) error {
	return nil
}

// ShowTablesOpt is show tables option
type ShowTablesOpt struct {
	Full   string
	DbName string
	Filter *ShowFilter
}

// ShowFilter is show tables filter
type ShowFilter struct {
	Like   string
	Filter Expr
}

func (node *ShowFilter) walkSubtree(visit Visit) error {
	return nil
}

func (node *Use) walkSubtree(visit Visit) error {
	return Walk(visit, node.DBName)
}

func (node *Begin) walkSubtree(visit Visit) error {
	return nil
}

func (node *Commit) walkSubtree(visit Visit) error {
	return nil
}

func (node *Rollback) walkSubtree(visit Visit) error {
	return nil
}

func (node *OtherRead) walkSubtree(visit Visit) error {
	return nil
}

func (node *OtherAdmin) walkSubtree(visit Visit) error {
	return nil
}

// Comments represents a list of comments.
type Comments [][]byte

func (node Comments) walkSubtree(visit Visit) error {
	return nil
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

func (node SelectExprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// SelectExpr represents a SELECT expression.
type SelectExpr interface {
	iSelectExpr()
	SQLNode
}

func (*StarExpr) iSelectExpr()    {}
func (*AliasedExpr) iSelectExpr() {}
func (Nextval) iSelectExpr()      {}

// StarExpr defines a '*' or 'table.*' expression.
type StarExpr struct {
	TableName TableName
}

func (node *StarExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.TableName,
	)
}

// AliasedExpr defines an aliased SELECT expression.
type AliasedExpr struct {
	Expr Expr
	As   ColIdent
}

func (node *AliasedExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
		node.As,
	)
}

// Nextval defines the NEXT VALUE expression.
type Nextval struct {
	Expr Expr
}

func (node Nextval) walkSubtree(visit Visit) error {
	return Walk(visit, node.Expr)
}

// Columns represents an insert column list.
type Columns []ColIdent

func (node Columns) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// FindColumn finds a column in the column list, returning
// the index if it exists or -1 otherwise
func (node Columns) FindColumn(col ColIdent) int {
	for i, colName := range node {
		if colName.Equal(col) {
			return i
		}
	}
	return -1
}

// Partitions is a type alias for Columns so we can handle printing efficiently
type Partitions Columns

func (node Partitions) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

func (node TableExprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// TableExpr represents a table expression.
type TableExpr interface {
	iTableExpr()
	SQLNode
}

func (*AliasedTableExpr) iTableExpr() {}
func (*ParenTableExpr) iTableExpr()   {}
func (*JoinTableExpr) iTableExpr()    {}

// AliasedTableExpr represents a table expression
// coupled with an optional alias or index hint.
// If As is empty, no alias was used.
type AliasedTableExpr struct {
	Expr       SimpleTableExpr
	Partitions Partitions
	As         TableIdent
	Hints      *IndexHints
}

func (node *AliasedTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
		node.As,
		node.Hints,
	)
}

// RemoveHints returns a new AliasedTableExpr with the hints removed.
func (node *AliasedTableExpr) RemoveHints() *AliasedTableExpr {
	noHints := *node
	noHints.Hints = nil
	return &noHints
}

// SimpleTableExpr represents a simple table expression.
type SimpleTableExpr interface {
	iSimpleTableExpr()
	SQLNode
}

func (TableName) iSimpleTableExpr() {}
func (*Subquery) iSimpleTableExpr() {}

// TableNames is a list of TableName.
type TableNames []TableName

func (node TableNames) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// TableName represents a table  name.
// Qualifier, if specified, represents a database or keyspace.
// TableName is a value struct whose fields are case sensitive.
// This means two TableName vars can be compared for equality
// and a TableName can also be used as key in a map.
type TableName struct {
	Name, Qualifier TableIdent
}

func (node TableName) walkSubtree(visit Visit) error {
	return Walk(
		visit,
		node.Name,
		node.Qualifier,
	)
}

// IsEmpty returns true if TableName is nil or empty.
func (node TableName) IsEmpty() bool {
	// If Name is empty, Qualifier is also empty.
	return node.Name.IsEmpty()
}

// ToViewName returns a TableName acceptable for use as a VIEW. VIEW names are
// always lowercase, so ToViewName lowercasese the name. Databases are case-sensitive
// so Qualifier is left untouched.
func (node TableName) ToViewName() TableName {
	return TableName{
		Qualifier: node.Qualifier,
		Name:      NewTableIdent(strings.ToLower(node.Name.v)),
	}
}

// ParenTableExpr represents a parenthesized list of TableExpr.
type ParenTableExpr struct {
	Exprs TableExprs
}

func (node *ParenTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Exprs,
	)
}

// JoinCondition represents the join conditions (either a ON or USING clause)
// of a JoinTableExpr.
type JoinCondition struct {
	On    Expr
	Using Columns
}

func (node JoinCondition) walkSubtree(visit Visit) error {
	return Walk(
		visit,
		node.On,
		node.Using,
	)
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	LeftExpr  TableExpr
	Join      string
	RightExpr TableExpr
	Condition JoinCondition
}

// JoinTableExpr.Join
const (
	JoinStr             = "join"
	StraightJoinStr     = "straight_join"
	LeftJoinStr         = "left join"
	RightJoinStr        = "right join"
	NaturalJoinStr      = "natural join"
	NaturalLeftJoinStr  = "natural left join"
	NaturalRightJoinStr = "natural right join"
)

func (node *JoinTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.LeftExpr,
		node.RightExpr,
		node.Condition,
	)
}

// IndexHints represents a list of index hints.
type IndexHints struct {
	Type    string
	Indexes []ColIdent
}

// Index hints.
const (
	UseStr    = "use "
	IgnoreStr = "ignore "
	ForceStr  = "force "
)

func (node *IndexHints) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	for _, n := range node.Indexes {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Where.Type
const (
	WhereStr  = "where"
	HavingStr = "having"
)

// NewWhere creates a WHERE or HAVING clause out
// of a Expr. If the expression is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

func (node *Where) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

// *********** Expressions

// Expr represents an expression.
type Expr interface {
	iExpr()
	// replace replaces any subexpression that matches
	// from with to. The implementation can use the
	// replaceExprs convenience function.
	replace(from, to Expr) bool
	SQLNode
}

type (
	// AndExpr represents an AND expression.
	AndExpr struct {
		Left, Right Expr
	}

	// OrExpr represents an OR expression.
	OrExpr struct {
		Left, Right Expr
	}

	// NotExpr represents a NOT expression.
	NotExpr struct {
		Expr Expr
	}

	// ParenExpr represents a parenthesized boolean expression.
	ParenExpr struct {
		Expr Expr
	}

	// ComparisonExpr represents a two-value comparison expression.
	ComparisonExpr struct {
		Operator    string
		Left, Right Expr
		Escape      Expr
	}

	// RangeCond represents a BETWEEN or a NOT BETWEEN expression.
	RangeCond struct {
		Operator string
		Left     Expr
		From, To Expr
	}

	// IsExpr represents an IS ... or an IS NOT ... expression.
	IsExpr struct {
		Operator string
		Expr     Expr
	}

	// ExistsExpr represents an EXISTS expression.
	ExistsExpr struct {
		Subquery *Subquery
	}

	// SQLVal represents a single value.
	SQLVal struct {
		Type ValType
		Val  []byte
	}

	// NullVal represents a NULL value.
	NullVal struct{}

	// BoolVal is true or false.
	BoolVal bool

	// ColName represents a column name.
	ColName struct {
		// Metadata is not populated by the parser.
		// It's a placeholder for analyzers to store
		// additional data, typically info about which
		// table or column this node references.
		Metadata  interface{}
		Name      ColIdent
		Qualifier TableName
	}

	// ColTuple represents a list of column values.
	// It can be ValTuple, Subquery, ListArg.
	ColTuple interface {
		iColTuple()
		Expr
	}

	// Subquery represents a subquery.
	Subquery struct {
		Select SelectStatement
	}

	// ListArg represents a named list argument.
	ListArg []byte

	// ValTuple represents a tuple of actual values.
	ValTuple Exprs

	// BinaryExpr represents a binary value expression.
	BinaryExpr struct {
		Operator    string
		Left, Right Expr
	}

	// UnaryExpr represents a unary value expression.
	UnaryExpr struct {
		Operator string
		Expr     Expr
	}

	// IntervalExpr represents a date-time INTERVAL expression.
	IntervalExpr struct {
		Expr Expr
		Unit string
	}

	// TimestampFuncExpr represents the function and arguments for TIMESTAMP{ADD,DIFF} functions.
	TimestampFuncExpr struct {
		Name  string
		Expr1 Expr
		Expr2 Expr
		Unit  string
	}

	// CollateExpr represents dynamic collate operator.
	CollateExpr struct {
		Expr    Expr
		Charset string
	}

	// FuncExpr represents a function call.
	FuncExpr struct {
		Qualifier TableIdent
		Name      ColIdent
		Distinct  bool
		Exprs     SelectExprs
	}

	// GroupConcatExpr represents a call to GROUP_CONCAT
	GroupConcatExpr struct {
		Distinct  string
		Exprs     SelectExprs
		OrderBy   OrderBy
		Separator string
	}

	// ValuesFuncExpr represents a function call.
	ValuesFuncExpr struct {
		Name *ColName
	}

	// SubstrExpr represents a call to SubstrExpr(column, value_expression) or SubstrExpr(column, value_expression,value_expression)
	// also supported syntax SubstrExpr(column from value_expression for value_expression).
	// Additionally to column names, SubstrExpr is also supported for string values, e.g.:
	// SubstrExpr('static string value', value_expression, value_expression)
	// In this case StrVal will be set instead of Name.
	SubstrExpr struct {
		Name   *ColName
		StrVal *SQLVal
		From   Expr
		To     Expr
	}

	// ConvertExpr represents a call to CONVERT(expr, type)
	// or it's equivalent CAST(expr AS type). Both are rewritten to the former.
	ConvertExpr struct {
		Expr Expr
		Type *ConvertType
	}

	// ConvertUsingExpr represents a call to CONVERT(expr USING charset).
	ConvertUsingExpr struct {
		Expr Expr
		Type string
	}

	// MatchExpr represents a call to the MATCH function
	MatchExpr struct {
		Columns SelectExprs
		Expr    Expr
		Option  string
	}

	// CaseExpr represents a CASE expression.
	CaseExpr struct {
		Expr  Expr
		Whens []*When
		Else  Expr
	}

	// Default represents a DEFAULT expression.
	Default struct {
		ColName string
	}

	// When represents a WHEN sub-expression.
	When struct {
		Cond Expr
		Val  Expr
	}

	// CurTimeFuncExpr represents the function and arguments for CURRENT DATE/TIME functions
	// supported functions are documented in the grammar
	CurTimeFuncExpr struct {
		Name ColIdent
		Fsp  Expr // fractional seconds precision, integer from 0 to 6
	}
)

// iExpr ensures that only expressions nodes can be assigned to a Expr
func (*AndExpr) iExpr()           {}
func (*OrExpr) iExpr()            {}
func (*NotExpr) iExpr()           {}
func (*ParenExpr) iExpr()         {}
func (*ComparisonExpr) iExpr()    {}
func (*RangeCond) iExpr()         {}
func (*IsExpr) iExpr()            {}
func (*ExistsExpr) iExpr()        {}
func (*SQLVal) iExpr()            {}
func (*NullVal) iExpr()           {}
func (BoolVal) iExpr()            {}
func (*ColName) iExpr()           {}
func (ValTuple) iExpr()           {}
func (*Subquery) iExpr()          {}
func (ListArg) iExpr()            {}
func (*BinaryExpr) iExpr()        {}
func (*UnaryExpr) iExpr()         {}
func (*IntervalExpr) iExpr()      {}
func (*CollateExpr) iExpr()       {}
func (*FuncExpr) iExpr()          {}
func (*TimestampFuncExpr) iExpr() {}
func (*CurTimeFuncExpr) iExpr()   {}
func (*CaseExpr) iExpr()          {}
func (*ValuesFuncExpr) iExpr()    {}
func (*ConvertExpr) iExpr()       {}
func (*SubstrExpr) iExpr()        {}
func (*ConvertUsingExpr) iExpr()  {}
func (*MatchExpr) iExpr()         {}
func (*GroupConcatExpr) iExpr()   {}
func (*Default) iExpr()           {}

// ReplaceExpr finds the from expression from root
// and replaces it with to. If from matches root,
// then to is returned.
func ReplaceExpr(root, from, to Expr) Expr {
	if root == from {
		return to
	}
	root.replace(from, to)
	return root
}

// replaceExprs is a convenience function used by implementors
// of the replace method.
func replaceExprs(from, to Expr, exprs ...*Expr) bool {
	for _, expr := range exprs {
		if *expr == nil {
			continue
		}
		if *expr == from {
			*expr = to
			return true
		}
		if (*expr).replace(from, to) {
			return true
		}
	}
	return false
}

// Exprs represents a list of value expressions.
// It's not a valid expression because it's not parenthesized.
type Exprs []Expr

func (node Exprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

func (node *AndExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
	)
}

func (node *AndExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right)
}

func (node *OrExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
	)
}

func (node *OrExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right)
}

func (node *NotExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *NotExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

func (node *ParenExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *ParenExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

// ComparisonExpr.Operator
const (
	EqualStr             = "="
	LessThanStr          = "<"
	GreaterThanStr       = ">"
	LessEqualStr         = "<="
	GreaterEqualStr      = ">="
	NotEqualStr          = "!="
	NullSafeEqualStr     = "<=>"
	InStr                = "in"
	NotInStr             = "not in"
	LikeStr              = "like"
	NotLikeStr           = "not like"
	RegexpStr            = "regexp"
	NotRegexpStr         = "not regexp"
	JSONExtractOp        = "->"
	JSONUnquoteExtractOp = "->>"
)

func (node *ComparisonExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
		node.Escape,
	)
}

func (node *ComparisonExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right, &node.Escape)
}

// IsImpossible returns true if the comparison in the expression can never evaluate to true.
// Note that this is not currently exhaustive to ALL impossible comparisons.
func (node *ComparisonExpr) IsImpossible() bool {
	var left, right *SQLVal
	var ok bool
	if left, ok = node.Left.(*SQLVal); !ok {
		return false
	}
	if right, ok = node.Right.(*SQLVal); !ok {
		return false
	}
	if node.Operator == NotEqualStr && left.Type == right.Type {
		if len(left.Val) != len(right.Val) {
			return false
		}

		for i := range left.Val {
			if left.Val[i] != right.Val[i] {
				return false
			}
		}
		return true
	}
	return false
}

// RangeCond.Operator
const (
	BetweenStr    = "between"
	NotBetweenStr = "not between"
)

func (node *RangeCond) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.From,
		node.To,
	)
}

func (node *RangeCond) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.From, &node.To)
}

// IsExpr.Operator
const (
	IsNullStr     = "is null"
	IsNotNullStr  = "is not null"
	IsTrueStr     = "is true"
	IsNotTrueStr  = "is not true"
	IsFalseStr    = "is false"
	IsNotFalseStr = "is not false"
)

func (node *IsExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *IsExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

func (node *ExistsExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Subquery,
	)
}

func (node *ExistsExpr) replace(from, to Expr) bool {
	return false
}

// ExprFromValue converts the given Value into an Expr or returns an error.
func ExprFromValue(value sqltypes.Value) (Expr, error) {
	// The type checks here follow the rules defined in sqltypes/types.go.
	switch {
	case value.Type() == sqltypes.Null:
		return &NullVal{}, nil
	case value.IsIntegral():
		return NewIntVal(value.ToBytes()), nil
	case value.IsFloat() || value.Type() == sqltypes.Decimal:
		return NewFloatVal(value.ToBytes()), nil
	case value.IsQuoted():
		return NewStrVal(value.ToBytes()), nil
	default:
		// We cannot support sqltypes.Expression, or any other invalid type.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot convert value %v to AST", value)
	}
}

// ValType specifies the type for SQLVal.
type ValType int

// These are the possible Valtype values.
// HexNum represents a 0x... value. It cannot
// be treated as a simple value because it can
// be interpreted differently depending on the
// context.
const (
	StrVal = ValType(iota)
	IntVal
	FloatVal
	HexNum
	HexVal
	ValArg
	BitVal
)

// NewStrVal builds a new StrVal.
func NewStrVal(in []byte) *SQLVal {
	return &SQLVal{Type: StrVal, Val: in}
}

// NewIntVal builds a new IntVal.
func NewIntVal(in []byte) *SQLVal {
	return &SQLVal{Type: IntVal, Val: in}
}

// NewFloatVal builds a new FloatVal.
func NewFloatVal(in []byte) *SQLVal {
	return &SQLVal{Type: FloatVal, Val: in}
}

// NewHexNum builds a new HexNum.
func NewHexNum(in []byte) *SQLVal {
	return &SQLVal{Type: HexNum, Val: in}
}

// NewHexVal builds a new HexVal.
func NewHexVal(in []byte) *SQLVal {
	return &SQLVal{Type: HexVal, Val: in}
}

// NewBitVal builds a new BitVal containing a bit literal.
func NewBitVal(in []byte) *SQLVal {
	return &SQLVal{Type: BitVal, Val: in}
}

// NewValArg builds a new ValArg.
func NewValArg(in []byte) *SQLVal {
	return &SQLVal{Type: ValArg, Val: in}
}

func (node *SQLVal) walkSubtree(visit Visit) error {
	return nil
}

func (node *SQLVal) replace(from, to Expr) bool {
	return false
}

// HexDecode decodes the hexval into bytes.
func (node *SQLVal) HexDecode() ([]byte, error) {
	dst := make([]byte, hex.DecodedLen(len([]byte(node.Val))))
	_, err := hex.Decode(dst, []byte(node.Val))
	if err != nil {
		return nil, err
	}
	return dst, err
}

func (node *NullVal) walkSubtree(visit Visit) error {
	return nil
}

func (node *NullVal) replace(from, to Expr) bool {
	return false
}

func (node BoolVal) walkSubtree(visit Visit) error {
	return nil
}

func (node BoolVal) replace(from, to Expr) bool {
	return false
}

func (node *ColName) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Name,
		node.Qualifier,
	)
}

func (node *ColName) replace(from, to Expr) bool {
	return false
}

// Equal returns true if the column names match.
func (node *ColName) Equal(c *ColName) bool {
	// Failsafe: ColName should not be empty.
	if node == nil || c == nil {
		return false
	}
	return node.Name.Equal(c.Name) && node.Qualifier == c.Qualifier
}

func (ValTuple) iColTuple()  {}
func (*Subquery) iColTuple() {}
func (ListArg) iColTuple()   {}

func (node ValTuple) walkSubtree(visit Visit) error {
	return Walk(visit, Exprs(node))
}

func (node ValTuple) replace(from, to Expr) bool {
	for i := range node {
		if replaceExprs(from, to, &node[i]) {
			return true
		}
	}
	return false
}

func (node *Subquery) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Select,
	)
}

func (node *Subquery) replace(from, to Expr) bool {
	return false
}

func (node ListArg) walkSubtree(visit Visit) error {
	return nil
}

func (node ListArg) replace(from, to Expr) bool {
	return false
}

// BinaryExpr.Operator
const (
	BitAndStr     = "&"
	BitOrStr      = "|"
	BitXorStr     = "^"
	PlusStr       = "+"
	MinusStr      = "-"
	MultStr       = "*"
	DivStr        = "/"
	IntDivStr     = "div"
	ModStr        = "%"
	ShiftLeftStr  = "<<"
	ShiftRightStr = ">>"
)

func (node *BinaryExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
	)
}

func (node *BinaryExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right)
}

// UnaryExpr.Operator
const (
	UPlusStr   = "+"
	UMinusStr  = "-"
	TildaStr   = "~"
	BangStr    = "!"
	BinaryStr  = "binary "
	UBinaryStr = "_binary "
	Utf8mb4Str = "_utf8mb4 "
)

func (node *UnaryExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *UnaryExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

func (node *IntervalExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *IntervalExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

func (node *TimestampFuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr1,
		node.Expr2,
	)
}

func (node *TimestampFuncExpr) replace(from, to Expr) bool {
	if replaceExprs(from, to, &node.Expr1) {
		return true
	}
	if replaceExprs(from, to, &node.Expr2) {
		return true
	}
	return false
}

func (node *CurTimeFuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Fsp,
	)
}

func (node *CurTimeFuncExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Fsp)
}

func (node *CollateExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *CollateExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

func (node *FuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Qualifier,
		node.Name,
		node.Exprs,
	)
}

func (node *FuncExpr) replace(from, to Expr) bool {
	for _, sel := range node.Exprs {
		aliased, ok := sel.(*AliasedExpr)
		if !ok {
			continue
		}
		if replaceExprs(from, to, &aliased.Expr) {
			return true
		}
	}
	return false
}

// Aggregates is a map of all aggregate functions.
var Aggregates = map[string]bool{
	"avg":          true,
	"bit_and":      true,
	"bit_or":       true,
	"bit_xor":      true,
	"count":        true,
	"group_concat": true,
	"max":          true,
	"min":          true,
	"std":          true,
	"stddev_pop":   true,
	"stddev_samp":  true,
	"stddev":       true,
	"sum":          true,
	"var_pop":      true,
	"var_samp":     true,
	"variance":     true,
}

// IsAggregate returns true if the function is an aggregate.
func (node *FuncExpr) IsAggregate() bool {
	return Aggregates[node.Name.Lowered()]
}

func (node *GroupConcatExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Exprs,
		node.OrderBy,
	)
}

func (node *GroupConcatExpr) replace(from, to Expr) bool {
	for _, sel := range node.Exprs {
		aliased, ok := sel.(*AliasedExpr)
		if !ok {
			continue
		}
		if replaceExprs(from, to, &aliased.Expr) {
			return true
		}
	}
	for _, order := range node.OrderBy {
		if replaceExprs(from, to, &order.Expr) {
			return true
		}
	}
	return false
}

func (node *ValuesFuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Name,
	)
}

func (node *ValuesFuncExpr) replace(from, to Expr) bool {
	return false
}

func (node *SubstrExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.From, &node.To)
}

func (node *SubstrExpr) walkSubtree(visit Visit) error {
	if node == nil || node.Name == nil {
		return nil
	}
	return Walk(
		visit,
		node.Name,
		node.From,
		node.To,
	)
}

func (node *ConvertExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
		node.Type,
	)
}

func (node *ConvertExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

func (node *ConvertUsingExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *ConvertUsingExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}

// ConvertType represents the type in call to CONVERT(expr, type)
type ConvertType struct {
	Type     string
	Length   *SQLVal
	Scale    *SQLVal
	Operator string
	Charset  string
}

// this string is "character set" and this comment is required
const (
	CharacterSetStr = " character set"
	CharsetStr      = "charset"
)

func (node *ConvertType) walkSubtree(visit Visit) error {
	return nil
}

// MatchExpr.Option
const (
	BooleanModeStr                           = " in boolean mode"
	NaturalLanguageModeStr                   = " in natural language mode"
	NaturalLanguageModeWithQueryExpansionStr = " in natural language mode with query expansion"
	QueryExpansionStr                        = " with query expansion"
)

func (node *MatchExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Columns,
		node.Expr,
	)
}

func (node *MatchExpr) replace(from, to Expr) bool {
	for _, sel := range node.Columns {
		aliased, ok := sel.(*AliasedExpr)
		if !ok {
			continue
		}
		if replaceExprs(from, to, &aliased.Expr) {
			return true
		}
	}
	return replaceExprs(from, to, &node.Expr)
}

func (node *CaseExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	if err := Walk(visit, node.Expr); err != nil {
		return err
	}
	for _, n := range node.Whens {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return Walk(visit, node.Else)
}

func (node *CaseExpr) replace(from, to Expr) bool {
	for _, when := range node.Whens {
		if replaceExprs(from, to, &when.Cond, &when.Val) {
			return true
		}
	}
	return replaceExprs(from, to, &node.Expr, &node.Else)
}

func (node *Default) walkSubtree(visit Visit) error {
	return nil
}

func (node *Default) replace(from, to Expr) bool {
	return false
}

func (node *When) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Cond,
		node.Val,
	)
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

func (node GroupBy) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

func (node OrderBy) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// Order represents an ordering expression.
type Order struct {
	Expr      Expr
	Direction string
}

// Order.Direction
const (
	AscScr  = "asc"
	DescScr = "desc"
)

func (node *Order) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Rowcount Expr
}

func (node *Limit) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Offset,
		node.Rowcount,
	)
}

// Values represents a VALUES clause.
type Values []ValTuple

func (node Values) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

func (node UpdateExprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Name *ColName
	Expr Expr
}

func (node *UpdateExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Name,
		node.Expr,
	)
}

// SetExprs represents a list of set expressions.
type SetExprs []*SetExpr

func (node SetExprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// SetExpr represents a set expression.
type SetExpr struct {
	Name ColIdent
	Expr Expr
}

// SetExpr.Expr, for SET TRANSACTION ... or START TRANSACTION
const (
	// TransactionStr is the Name for a SET TRANSACTION statement
	TransactionStr = "transaction"

	IsolationLevelReadUncommitted = "isolation level read uncommitted"
	IsolationLevelReadCommitted   = "isolation level read committed"
	IsolationLevelRepeatableRead  = "isolation level repeatable read"
	IsolationLevelSerializable    = "isolation level serializable"

	TxReadOnly  = "read only"
	TxReadWrite = "read write"
)

func (node *SetExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Name,
		node.Expr,
	)
}

// OnDup represents an ON DUPLICATE KEY clause.
type OnDup UpdateExprs

func (node OnDup) walkSubtree(visit Visit) error {
	return Walk(visit, UpdateExprs(node))
}

// ColIdent is a case insensitive SQL identifier. It will be escaped with
// backquotes if necessary.
type ColIdent struct {
	// This artifact prevents this struct from being compared
	// with itself. It consumes no space as long as it's not the
	// last field in the struct.
	_            [0]struct{ _ []byte }
	val, lowered string
}

// NewColIdent makes a new ColIdent.
func NewColIdent(str string) ColIdent {
	return ColIdent{
		val: str,
	}
}

func (node ColIdent) walkSubtree(visit Visit) error {
	return nil
}

// IsEmpty returns true if the name is empty.
func (node ColIdent) IsEmpty() bool {
	return node.val == ""
}

// String returns the unescaped column name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node ColIdent) String() string {
	return node.val
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node ColIdent) CompliantName() string {
	return compliantName(node.val)
}

// Lowered returns a lower-cased column name.
// This function should generally be used only for optimizing
// comparisons.
func (node ColIdent) Lowered() string {
	if node.val == "" {
		return ""
	}
	if node.lowered == "" {
		node.lowered = strings.ToLower(node.val)
	}
	return node.lowered
}

// Equal performs a case-insensitive compare.
func (node ColIdent) Equal(in ColIdent) bool {
	return node.Lowered() == in.Lowered()
}

// EqualString performs a case-insensitive compare with str.
func (node ColIdent) EqualString(str string) bool {
	return node.Lowered() == strings.ToLower(str)
}

// MarshalJSON marshals into JSON.
func (node ColIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.val)
}

// UnmarshalJSON unmarshals from JSON.
func (node *ColIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.val = result
	return nil
}

// TableIdent is a case sensitive SQL identifier. It will be escaped with
// backquotes if necessary.
type TableIdent struct {
	v string
}

// NewTableIdent creates a new TableIdent.
func NewTableIdent(str string) TableIdent {
	return TableIdent{v: str}
}

func (node TableIdent) walkSubtree(visit Visit) error {
	return nil
}

// IsEmpty returns true if TabIdent is empty.
func (node TableIdent) IsEmpty() bool {
	return node.v == ""
}

// String returns the unescaped table name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node TableIdent) String() string {
	return node.v
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node TableIdent) CompliantName() string {
	return compliantName(node.v)
}

// MarshalJSON marshals into JSON.
func (node TableIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.v)
}

// UnmarshalJSON unmarshals from JSON.
func (node *TableIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.v = result
	return nil
}

func formatID(buf *TrackedBuffer, original, lowered string) {
	isDbSystemVariable := false
	if len(original) > 1 && original[:2] == "@@" {
		isDbSystemVariable = true
	}

	for i, c := range original {
		if !isLetter(uint16(c)) && (!isDbSystemVariable || !isCarat(uint16(c))) {
			if i == 0 || !isDigit(uint16(c)) {
				goto mustEscape
			}
		}
	}
	if _, ok := keywords[lowered]; ok {
		goto mustEscape
	}
	buf.Myprintf("%s", original)
	return

mustEscape:
	buf.WriteByte('`')
	for _, c := range original {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
}

func compliantName(in string) string {
	var buf strings.Builder
	for i, c := range in {
		if !isLetter(uint16(c)) {
			if i == 0 || !isDigit(uint16(c)) {
				buf.WriteByte('_')
				continue
			}
		}
		buf.WriteRune(c)
	}
	return buf.String()
}
