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
	"fmt"
	"io"
	"strconv"
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Walk calls postVisit on every node.
// If postVisit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...SQLNode) error {
	for _, node := range nodes {
		err := VisitSQLNode(node, visit)
		if err != nil {
			return err
		}
	}
	return nil
}

// Visit defines the signature of a function that
// can be used to postVisit all nodes of a parse tree.
// returning false on kontinue means that children will not be visited
// returning an error will abort the visitation and return the error
type Visit func(node SQLNode) (kontinue bool, err error)

// Append appends the SQLNode to the buffer.
func Append(buf *strings.Builder, node SQLNode) {
	tbuf := &TrackedBuffer{
		Builder: buf,
		fast:    true,
	}
	node.formatFast(tbuf)
}

// IndexColumn describes a column or expression in an index definition with optional length (for column)
type IndexColumn struct {
	// Only one of Column or Expression can be specified
	// Length is an optional field which is only applicable when Column is used
	Column     IdentifierCI
	Length     *Literal
	Expression Expr
	Direction  OrderDirection
}

// LengthScaleOption is used for types that have an optional length
// and scale
type LengthScaleOption struct {
	Length *Literal
	Scale  *Literal
}

// IndexOption is used for trailing options for indexes: COMMENT, KEY_BLOCK_SIZE, USING, WITH PARSER
type IndexOption struct {
	Name   string
	Value  *Literal
	String string
}

// TableOption is used for create table options like AUTO_INCREMENT, INSERT_METHOD, etc
type TableOption struct {
	Name          string
	Value         *Literal
	String        string
	Tables        TableNames
	CaseSensitive bool
}

// ColumnKeyOption indicates whether or not the given column is defined as an
// index element and contains the type of the option
type ColumnKeyOption int

const (
	ColKeyNone ColumnKeyOption = iota
	ColKeyPrimary
	ColKeySpatialKey
	ColKeyFulltextKey
	ColKeyUnique
	ColKeyUniqueKey
	ColKey
)

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

// MatchAction indicates the type of match for a referential constraint, so
// a `MATCH FULL`, `MATCH SIMPLE` or `MATCH PARTIAL`.
type MatchAction int

const (
	// DefaultAction indicates no action was explicitly specified.
	DefaultMatch MatchAction = iota
	Full
	Partial
	Simple
)

// ShowTablesOpt is show tables option
type ShowTablesOpt struct {
	Full   string
	DbName string
	Filter *ShowFilter
}

// ValType specifies the type for Literal.
type ValType int

// These are the possible Valtype values.
// HexNum represents a 0x... value. It cannot
// be treated as a simple value because it can
// be interpreted differently depending on the
// context.
const (
	StrVal = ValType(iota)
	IntVal
	DecimalVal
	FloatVal
	HexNum
	HexVal
	BitVal
	DateVal
	TimeVal
	TimestampVal
)

// queryOptimizerPrefix is the prefix of an optimizer hint comment.
const queryOptimizerPrefix = "/*+"

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
	return SQLTypeToQueryType(ct.Type, ct.Unsigned)
}

func SQLTypeToQueryType(typeName string, unsigned bool) querypb.Type {
	switch keywordVals[strings.ToLower(typeName)] {
	case TINYINT:
		if unsigned {
			return sqltypes.Uint8
		}
		return sqltypes.Int8
	case SMALLINT:
		if unsigned {
			return sqltypes.Uint16
		}
		return sqltypes.Int16
	case MEDIUMINT:
		if unsigned {
			return sqltypes.Uint24
		}
		return sqltypes.Int24
	case INT, INTEGER:
		if unsigned {
			return sqltypes.Uint32
		}
		return sqltypes.Int32
	case BIGINT:
		if unsigned {
			return sqltypes.Uint64
		}
		return sqltypes.Int64
	case BOOL, BOOLEAN:
		return sqltypes.Uint8
	case TEXT:
		return sqltypes.Text
	case TINYTEXT:
		return sqltypes.Text
	case MEDIUMTEXT:
		return sqltypes.Text
	case LONGTEXT:
		return sqltypes.Text
	case BLOB:
		return sqltypes.Blob
	case TINYBLOB:
		return sqltypes.Blob
	case MEDIUMBLOB:
		return sqltypes.Blob
	case LONGBLOB:
		return sqltypes.Blob
	case CHAR:
		return sqltypes.Char
	case VARCHAR:
		return sqltypes.VarChar
	case BINARY:
		return sqltypes.Binary
	case VARBINARY:
		return sqltypes.VarBinary
	case DATE:
		return sqltypes.Date
	case TIME:
		return sqltypes.Time
	case DATETIME:
		return sqltypes.Datetime
	case TIMESTAMP:
		return sqltypes.Timestamp
	case YEAR:
		return sqltypes.Year
	case FLOAT_TYPE, FLOAT4_TYPE:
		return sqltypes.Float32
	case DOUBLE, FLOAT8_TYPE:
		return sqltypes.Float64
	case DECIMAL, DECIMAL_TYPE:
		return sqltypes.Decimal
	case BIT:
		return sqltypes.Bit
	case ENUM:
		return sqltypes.Enum
	case SET:
		return sqltypes.Set
	case JSON:
		return sqltypes.TypeJSON
	case GEOMETRY:
		return sqltypes.Geometry
	case POINT:
		return sqltypes.Geometry
	case LINESTRING:
		return sqltypes.Geometry
	case POLYGON:
		return sqltypes.Geometry
	case GEOMETRYCOLLECTION:
		return sqltypes.Geometry
	case MULTIPOINT:
		return sqltypes.Geometry
	case MULTILINESTRING:
		return sqltypes.Geometry
	case MULTIPOLYGON:
		return sqltypes.Geometry
	}
	return sqltypes.Null
}

// AddQueryHint adds the given string to list of comment.
// If the list is empty, one will be created containing the query hint.
// If the list already contains a query hint, the given string will be merged with the existing one.
// This is done because only one query hint is allowed per query.
func (node *ParsedComments) AddQueryHint(queryHint string) (Comments, error) {
	if queryHint == "" {
		if node == nil {
			return nil, nil
		}
		return node.comments, nil
	}

	var newComments Comments
	var hasQueryHint bool

	if node != nil {
		for _, comment := range node.comments {
			if strings.HasPrefix(comment, queryOptimizerPrefix) {
				if hasQueryHint {
					return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Must have only one query hint")
				}
				hasQueryHint = true
				idx := strings.Index(comment, "*/")
				if idx == -1 {
					return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Query hint comment is malformed")
				}
				if strings.Contains(comment, queryHint) {
					newComments = append(Comments{comment}, newComments...)
					continue
				}
				newComment := fmt.Sprintf("%s %s */", strings.TrimSpace(comment[:idx]), queryHint)
				newComments = append(Comments{newComment}, newComments...)
				continue
			}
			newComments = append(newComments, comment)
		}
	}
	if !hasQueryHint {
		queryHintCommentStr := fmt.Sprintf("%s %s */", queryOptimizerPrefix, queryHint)
		newComments = append(Comments{queryHintCommentStr}, newComments...)
	}
	return newComments, nil
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

var _ ConstraintInfo = &ForeignKeyDefinition{}

func (f *ForeignKeyDefinition) iConstraintInfo() {}

var _ ConstraintInfo = &CheckConstraintDefinition{}

func (c *CheckConstraintDefinition) iConstraintInfo() {}

// FindColumn finds a column in the column list, returning
// the index if it exists or -1 otherwise
func (node Columns) FindColumn(col IdentifierCI) int {
	for i, colName := range node {
		if colName.Equal(col) {
			return i
		}
	}
	return -1
}

// RemoveHints returns a new AliasedTableExpr with the hints removed.
func (node *AliasedTableExpr) RemoveHints() *AliasedTableExpr {
	noHints := *node
	noHints.Hints = nil
	return &noHints
}

// TableName returns a TableName pointing to this table expr
func (node *AliasedTableExpr) TableName() (TableName, error) {
	if !node.As.IsEmpty() {
		return TableName{Name: node.As}, nil
	}

	tableName, ok := node.Expr.(TableName)
	if !ok {
		return TableName{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: the AST has changed. This should not be possible")
	}

	return tableName, nil
}

// IsEmpty returns true if TableName is nil or empty.
func (node TableName) IsEmpty() bool {
	// If Name is empty, Qualifier is also empty.
	return node.Name.IsEmpty()
}

// NewWhere creates a WHERE or HAVING clause out
// of a Expr. If the expression is nil, it returns nil.
func NewWhere(typ WhereType, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// ReplaceExpr finds the from expression from root
// and replaces it with to. If from matches root,
// then to is returned.
func ReplaceExpr(root, from, to Expr) Expr {
	tmp := SafeRewrite(root, stopWalking, replaceExpr(from, to))

	expr, success := tmp.(Expr)
	if !success {
		log.Errorf("Failed to rewrite expression. Rewriter returned a non-expression:  %s", String(tmp))
		return from
	}

	return expr
}

func stopWalking(e SQLNode, _ SQLNode) bool {
	switch e.(type) {
	case *ExistsExpr, *Literal, *Subquery, *ValuesFuncExpr, *Default:
		return false
	default:
		return true
	}
}

func replaceExpr(from, to Expr) func(cursor *Cursor) bool {
	return func(cursor *Cursor) bool {
		if cursor.Node() == from {
			cursor.Replace(to)
		}
		return true
	}
}

// IsImpossible returns true if the comparison in the expression can never evaluate to true.
// Note that this is not currently exhaustive to ALL impossible comparisons.
func (node *ComparisonExpr) IsImpossible() bool {
	var left, right *Literal
	var ok bool
	if left, ok = node.Left.(*Literal); !ok {
		return false
	}
	if right, ok = node.Right.(*Literal); !ok {
		return false
	}
	if node.Operator == NotEqualOp && left.Type == right.Type {
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

// NewStrLiteral builds a new StrVal.
func NewStrLiteral(in string) *Literal {
	return &Literal{Type: StrVal, Val: in}
}

// NewIntLiteral builds a new IntVal.
func NewIntLiteral(in string) *Literal {
	return &Literal{Type: IntVal, Val: in}
}

func NewDecimalLiteral(in string) *Literal {
	return &Literal{Type: DecimalVal, Val: in}
}

// NewFloatLiteral builds a new FloatVal.
func NewFloatLiteral(in string) *Literal {
	return &Literal{Type: FloatVal, Val: in}
}

// NewHexNumLiteral builds a new HexNum.
func NewHexNumLiteral(in string) *Literal {
	return &Literal{Type: HexNum, Val: in}
}

// NewHexLiteral builds a new HexVal.
func NewHexLiteral(in string) *Literal {
	return &Literal{Type: HexVal, Val: in}
}

// NewBitLiteral builds a new BitVal containing a bit literal.
func NewBitLiteral(in string) *Literal {
	return &Literal{Type: BitVal, Val: in}
}

// NewDateLiteral builds a new Date.
func NewDateLiteral(in string) *Literal {
	return &Literal{Type: DateVal, Val: in}
}

// NewTimeLiteral builds a new Date.
func NewTimeLiteral(in string) *Literal {
	return &Literal{Type: TimeVal, Val: in}
}

// NewTimestampLiteral builds a new Date.
func NewTimestampLiteral(in string) *Literal {
	return &Literal{Type: TimestampVal, Val: in}
}

// NewArgument builds a new ValArg.
func NewArgument(in string) *Argument {
	return &Argument{Name: in, Type: sqltypes.Unknown}
}

func parseBindVariable(yylex yyLexer, bvar string) *Argument {
	markBindVariable(yylex, bvar)
	return NewArgument(bvar)
}

func NewTypedArgument(in string, t sqltypes.Type) *Argument {
	return &Argument{Name: in, Type: t}
}

// NewListArg builds a new ListArg.
func NewListArg(in string) ListArg {
	return ListArg(in)
}

// String returns ListArg as a string.
func (node ListArg) String() string {
	return string(node)
}

// Bytes return the []byte
func (node *Literal) Bytes() []byte {
	return []byte(node.Val)
}

// HexDecode decodes the hexval into bytes.
func (node *Literal) HexDecode() ([]byte, error) {
	return hex.DecodeString(node.Val)
}

func (node *Literal) SQLType() sqltypes.Type {
	switch node.Type {
	case StrVal:
		return sqltypes.VarChar
	case IntVal:
		return sqltypes.Int64
	case FloatVal:
		return sqltypes.Float64
	case DecimalVal:
		return sqltypes.Decimal
	case HexNum:
		return sqltypes.HexNum
	case HexVal:
		return sqltypes.HexVal
	case BitVal:
		return sqltypes.HexNum
	case DateVal:
		return sqltypes.Date
	case TimeVal:
		return sqltypes.Time
	case TimestampVal:
		return sqltypes.Datetime
	default:
		return -1
	}
}

// Equal returns true if the column names match.
func (node *ColName) Equal(c *ColName) bool {
	// Failsafe: ColName should not be empty.
	if node == nil || c == nil {
		return false
	}
	return node.Name.Equal(c.Name) && node.Qualifier == c.Qualifier
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

// NewIdentifierCI makes a new IdentifierCI.
func NewIdentifierCI(str string) IdentifierCI {
	return IdentifierCI{
		val: str,
	}
}

// NewColName makes a new ColName
func NewColName(str string) *ColName {
	return &ColName{
		Name: NewIdentifierCI(str),
	}
}

// NewColNameWithQualifier makes a new ColName pointing to a specific table
func NewColNameWithQualifier(identifier string, table TableName) *ColName {
	return &ColName{
		Name: NewIdentifierCI(identifier),
		Qualifier: TableName{
			Name:      NewIdentifierCS(table.Name.String()),
			Qualifier: NewIdentifierCS(table.Qualifier.String()),
		},
	}
}

// NewSelect is used to create a select statement
func NewSelect(comments Comments, exprs SelectExprs, selectOptions []string, into *SelectInto, from TableExprs, where *Where, groupBy GroupBy, having *Where, windows NamedWindows) *Select {
	var cache *bool
	var distinct, straightJoinHint, sqlFoundRows bool

	for _, option := range selectOptions {
		switch strings.ToLower(option) {
		case DistinctStr:
			distinct = true
		case SQLCacheStr:
			truth := true
			cache = &truth
		case SQLNoCacheStr:
			truth := false
			cache = &truth
		case StraightJoinHint:
			straightJoinHint = true
		case SQLCalcFoundRowsStr:
			sqlFoundRows = true
		}
	}
	return &Select{
		Cache:            cache,
		Comments:         comments.Parsed(),
		Distinct:         distinct,
		StraightJoinHint: straightJoinHint,
		SQLCalcFoundRows: sqlFoundRows,
		SelectExprs:      exprs,
		Into:             into,
		From:             from,
		Where:            where,
		GroupBy:          groupBy,
		Having:           having,
		Windows:          windows,
	}
}

// UpdateSetExprsScope updates the scope of the variables in SetExprs.
func UpdateSetExprsScope(setExprs SetExprs, scope Scope) SetExprs {
	for _, setExpr := range setExprs {
		setExpr.Var.Scope = scope
	}
	return setExprs
}

// NewSetVariable returns a variable that can be used with SET.
func NewSetVariable(str string, scope Scope) *Variable {
	return &Variable{Name: createIdentifierCI(str), Scope: scope}
}

// NewSetStatement returns a Set struct
func NewSetStatement(comments *ParsedComments, exprs SetExprs) *Set {
	return &Set{Exprs: exprs, Comments: comments}
}

// NewVariableExpression returns an expression the evaluates to a variable at runtime.
// The AtCount and the prefix of the name of the variable will decide how it's evaluated
func NewVariableExpression(str string, at AtCount) *Variable {
	l := strings.ToLower(str)
	v := &Variable{
		Name: createIdentifierCI(str),
	}

	switch at {
	case DoubleAt:
		switch {
		case strings.HasPrefix(l, "local."):
			v.Name = createIdentifierCI(str[6:])
			v.Scope = SessionScope
		case strings.HasPrefix(l, "session."):
			v.Name = createIdentifierCI(str[8:])
			v.Scope = SessionScope
		case strings.HasPrefix(l, "global."):
			v.Name = createIdentifierCI(str[7:])
			v.Scope = GlobalScope
		case strings.HasPrefix(l, "vitess_metadata."):
			v.Name = createIdentifierCI(str[16:])
			v.Scope = VitessMetadataScope
		case strings.HasSuffix(l, TransactionIsolationStr) || strings.HasSuffix(l, TransactionReadOnlyStr):
			v.Scope = NextTxScope
		default:
			v.Scope = SessionScope
		}
	case SingleAt:
		v.Scope = VariableScope
	case NoAt:
		panic("we should never see NoAt here")
	}

	return v
}

func createIdentifierCI(str string) IdentifierCI {
	size := len(str)
	if str[0] == '`' && str[size-1] == '`' {
		str = str[1 : size-1]
	}
	return NewIdentifierCI(str)
}

// NewOffset creates an offset and returns it
func NewOffset(v int, original Expr) *Offset {
	return &Offset{V: v, Original: original}
}

// IsEmpty returns true if the name is empty.
func (node IdentifierCI) IsEmpty() bool {
	return node.val == ""
}

// String returns the unescaped column name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node IdentifierCI) String() string {
	return node.val
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node IdentifierCI) CompliantName() string {
	return compliantName(node.val)
}

// Lowered returns a lower-cased column name.
// This function should generally be used only for optimizing
// comparisons.
func (node IdentifierCI) Lowered() string {
	if node.val == "" {
		return ""
	}
	if node.lowered == "" {
		node.lowered = strings.ToLower(node.val)
	}
	return node.lowered
}

// Equal performs a case-insensitive compare.
func (node IdentifierCI) Equal(in IdentifierCI) bool {
	return node.Lowered() == in.Lowered()
}

// EqualString performs a case-insensitive compare with str.
func (node IdentifierCI) EqualString(str string) bool {
	return node.Lowered() == strings.ToLower(str)
}

// MarshalJSON marshals into JSON.
func (node IdentifierCI) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.val)
}

// UnmarshalJSON unmarshals from JSON.
func (node *IdentifierCI) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.val = result
	return nil
}

// NewIdentifierCS creates a new IdentifierCS.
func NewIdentifierCS(str string) IdentifierCS {
	// Use StringClone on the table name to ensure it is not pinned to the
	// underlying query string that has been generated by the parser. This
	// could lead to a significant increase in memory usage when the table
	// name comes from a large query.
	return IdentifierCS{v: strings.Clone(str)}
}

// IsEmpty returns true if TabIdent is empty.
func (node IdentifierCS) IsEmpty() bool {
	return node.v == ""
}

// String returns the unescaped table name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node IdentifierCS) String() string {
	return node.v
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node IdentifierCS) CompliantName() string {
	return compliantName(node.v)
}

// MarshalJSON marshals into JSON.
func (node IdentifierCS) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.v)
}

// UnmarshalJSON unmarshals from JSON.
func (node *IdentifierCS) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.v = result
	return nil
}

func containEscapableChars(s string, at AtCount) bool {
	isDbSystemVariable := at != NoAt

	for i := range s {
		c := uint16(s[i])
		letter := isLetter(c)
		systemVarChar := isDbSystemVariable && isCarat(c)
		if !(letter || systemVarChar) {
			if i == 0 || !isDigit(c) {
				return true
			}
		}
	}

	return false
}

func formatID(buf *TrackedBuffer, original string, at AtCount) {
	if buf.escape == escapeNoIdentifiers {
		buf.WriteString(original)
		return
	}
	_, isKeyword := keywordLookupTable.LookupString(original)
	if buf.escape == escapeAllIdentifiers || isKeyword || containEscapableChars(original, at) {
		writeEscapedString(buf, original)
	} else {
		buf.WriteString(original)
	}
}

func writeEscapedString(buf *TrackedBuffer, original string) {
	buf.WriteByte('`')
	for _, c := range original {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
}

func CompliantString(in SQLNode) string {
	s := String(in)
	return compliantName(s)
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

// AddOrder adds an order by element
func (node *Select) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetOrderBy sets the order by clause
func (node *Select) SetOrderBy(orderBy OrderBy) {
	node.OrderBy = orderBy
}

// GetOrderBy gets the order by clause
func (node *Select) GetOrderBy() OrderBy {
	return node.OrderBy
}

// SetLimit sets the limit clause
func (node *Select) SetLimit(limit *Limit) {
	node.Limit = limit
}

// GetLimit gets the limit
func (node *Select) GetLimit() *Limit {
	return node.Limit
}

// SetLock sets the lock clause
func (node *Select) SetLock(lock Lock) {
	node.Lock = lock
}

// SetInto sets the into clause
func (node *Select) SetInto(into *SelectInto) {
	node.Into = into
}

// SetWith sets the with clause to a select statement
func (node *Select) SetWith(with *With) {
	node.With = with
}

// MakeDistinct makes the statement distinct
func (node *Select) MakeDistinct() {
	node.Distinct = true
}

// GetColumnCount return SelectExprs count.
func (node *Select) GetColumnCount() int {
	return len(node.SelectExprs)
}

// GetColumns gets the columns
func (node *Select) GetColumns() SelectExprs {
	return node.SelectExprs
}

// SetComments implements the Commented interface
func (node *Select) SetComments(comments Comments) {
	node.Comments = comments.Parsed()
}

// GetParsedComments implements the Commented interface
func (node *Select) GetParsedComments() *ParsedComments {
	return node.Comments
}

// AddWhere adds the boolean expression to the
// WHERE clause as an AND condition.
func (node *Select) AddWhere(expr Expr) {
	node.Where = addPredicate(node.Where, expr)
}

// AddHaving adds the boolean expression to the
// HAVING clause as an AND condition.
func (node *Select) AddHaving(expr Expr) {
	node.Having = addPredicate(node.Having, expr)
	node.Having.Type = HavingClause
}

// AddGroupBy adds a grouping expression, unless it's already present
func (node *Select) AddGroupBy(expr Expr) {
	for _, gb := range node.GroupBy {
		if Equals.Expr(gb, expr) {
			// group by columns are sets - duplicates don't add anything, so we can just skip these
			return
		}
	}
	node.GroupBy = append(node.GroupBy, expr)
}

// AddWhere adds the boolean expression to the
// WHERE clause as an AND condition.
func (node *Update) AddWhere(expr Expr) {
	node.Where = addPredicate(node.Where, expr)
}

func addPredicate(where *Where, pred Expr) *Where {
	if where == nil {
		return &Where{
			Type: WhereClause,
			Expr: pred,
		}
	}
	where.Expr = &AndExpr{
		Left:  where.Expr,
		Right: pred,
	}
	return where
}

// AddOrder adds an order by element
func (node *Union) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetOrderBy sets the order by clause
func (node *Union) SetOrderBy(orderBy OrderBy) {
	node.OrderBy = orderBy
}

// GetOrderBy gets the order by clause
func (node *Union) GetOrderBy() OrderBy {
	return node.OrderBy
}

// SetLimit sets the limit clause
func (node *Union) SetLimit(limit *Limit) {
	node.Limit = limit
}

// GetLimit gets the limit
func (node *Union) GetLimit() *Limit {
	return node.Limit
}

// GetColumns gets the columns
func (node *Union) GetColumns() SelectExprs {
	return node.Left.GetColumns()
}

// SetLock sets the lock clause
func (node *Union) SetLock(lock Lock) {
	node.Lock = lock
}

// SetInto sets the into clause
func (node *Union) SetInto(into *SelectInto) {
	node.Into = into
}

// SetWith sets the with clause to a union statement
func (node *Union) SetWith(with *With) {
	node.With = with
}

// MakeDistinct implements the SelectStatement interface
func (node *Union) MakeDistinct() {
	node.Distinct = true
}

// GetColumnCount implements the SelectStatement interface
func (node *Union) GetColumnCount() int {
	return node.Left.GetColumnCount()
}

// SetComments implements the SelectStatement interface
func (node *Union) SetComments(comments Comments) {
	node.Left.SetComments(comments)
}

// GetParsedComments implements the SelectStatement interface
func (node *Union) GetParsedComments() *ParsedComments {
	return node.Left.GetParsedComments()
}

func requiresParen(stmt SelectStatement) bool {
	switch node := stmt.(type) {
	case *Union:
		return len(node.OrderBy) != 0 || node.Lock != 0 || node.Into != nil || node.Limit != nil
	case *Select:
		return len(node.OrderBy) != 0 || node.Lock != 0 || node.Into != nil || node.Limit != nil
	}

	return false
}

func setLockInSelect(stmt SelectStatement, lock Lock) {
	stmt.SetLock(lock)
}

// ToString returns the string associated with the DDLAction Enum
func (action DDLAction) ToString() string {
	switch action {
	case CreateDDLAction:
		return CreateStr
	case AlterDDLAction:
		return AlterStr
	case DropDDLAction:
		return DropStr
	case RenameDDLAction:
		return RenameStr
	case TruncateDDLAction:
		return TruncateStr
	case CreateVindexDDLAction:
		return CreateVindexStr
	case DropVindexDDLAction:
		return DropVindexStr
	case AddVschemaTableDDLAction:
		return AddVschemaTableStr
	case DropVschemaTableDDLAction:
		return DropVschemaTableStr
	case AddColVindexDDLAction:
		return AddColVindexStr
	case DropColVindexDDLAction:
		return DropColVindexStr
	case AddSequenceDDLAction:
		return AddSequenceStr
	case AddAutoIncDDLAction:
		return AddAutoIncStr
	default:
		return "Unknown DDL Action"
	}
}

// ToString returns the string associated with the Scope enum
func (scope Scope) ToString() string {
	switch scope {
	case SessionScope:
		return SessionStr
	case GlobalScope:
		return GlobalStr
	case VitessMetadataScope:
		return VitessMetadataStr
	case VariableScope:
		return VariableStr
	case NoScope, NextTxScope:
		return ""
	default:
		return "Unknown Scope"
	}
}

// ToString returns the IgnoreStr if ignore is true.
func (ignore Ignore) ToString() string {
	if ignore {
		return IgnoreStr
	}
	return ""
}

// ToString returns the string associated with the type of lock
func (lock Lock) ToString() string {
	switch lock {
	case NoLock:
		return NoLockStr
	case ForUpdateLock:
		return ForUpdateStr
	case ShareModeLock:
		return ShareModeStr
	default:
		return "Unknown lock"
	}
}

// ToString returns the string associated with WhereType
func (whereType WhereType) ToString() string {
	switch whereType {
	case WhereClause:
		return WhereStr
	case HavingClause:
		return HavingStr
	default:
		return "Unknown where type"
	}
}

// ToString returns the string associated with JoinType
func (joinType JoinType) ToString() string {
	switch joinType {
	case NormalJoinType:
		return JoinStr
	case StraightJoinType:
		return StraightJoinStr
	case LeftJoinType:
		return LeftJoinStr
	case RightJoinType:
		return RightJoinStr
	case NaturalJoinType:
		return NaturalJoinStr
	case NaturalLeftJoinType:
		return NaturalLeftJoinStr
	case NaturalRightJoinType:
		return NaturalRightJoinStr
	default:
		return "Unknown join type"
	}
}

// ToString returns the operator as a string
func (op ComparisonExprOperator) ToString() string {
	switch op {
	case EqualOp:
		return EqualStr
	case LessThanOp:
		return LessThanStr
	case GreaterThanOp:
		return GreaterThanStr
	case LessEqualOp:
		return LessEqualStr
	case GreaterEqualOp:
		return GreaterEqualStr
	case NotEqualOp:
		return NotEqualStr
	case NullSafeEqualOp:
		return NullSafeEqualStr
	case InOp:
		return InStr
	case NotInOp:
		return NotInStr
	case LikeOp:
		return LikeStr
	case NotLikeOp:
		return NotLikeStr
	case RegexpOp:
		return RegexpStr
	case NotRegexpOp:
		return NotRegexpStr
	default:
		return "Unknown ComparisonExpOperator"
	}
}

// ToString returns the operator as a string
func (op IsExprOperator) ToString() string {
	switch op {
	case IsNullOp:
		return IsNullStr
	case IsNotNullOp:
		return IsNotNullStr
	case IsTrueOp:
		return IsTrueStr
	case IsNotTrueOp:
		return IsNotTrueStr
	case IsFalseOp:
		return IsFalseStr
	case IsNotFalseOp:
		return IsNotFalseStr
	default:
		return "Unknown IsExprOperator"
	}
}

// ToString returns the operator as a string
func (op BinaryExprOperator) ToString() string {
	switch op {
	case BitAndOp:
		return BitAndStr
	case BitOrOp:
		return BitOrStr
	case BitXorOp:
		return BitXorStr
	case PlusOp:
		return PlusStr
	case MinusOp:
		return MinusStr
	case MultOp:
		return MultStr
	case DivOp:
		return DivStr
	case IntDivOp:
		return IntDivStr
	case ModOp:
		return ModStr
	case ShiftLeftOp:
		return ShiftLeftStr
	case ShiftRightOp:
		return ShiftRightStr
	case JSONExtractOp:
		return JSONExtractOpStr
	case JSONUnquoteExtractOp:
		return JSONUnquoteExtractOpStr
	default:
		return "Unknown BinaryExprOperator"
	}
}

// ToString returns the partition type as a string
func (partitionType PartitionByType) ToString() string {
	switch partitionType {
	case HashType:
		return HashTypeStr
	case KeyType:
		return KeyTypeStr
	case ListType:
		return ListTypeStr
	case RangeType:
		return RangeTypeStr
	default:
		return "Unknown PartitionByType"
	}
}

// ToString returns the partition value range type as a string
func (t PartitionValueRangeType) ToString() string {
	switch t {
	case LessThanType:
		return LessThanTypeStr
	case InType:
		return InTypeStr
	default:
		return "Unknown PartitionValueRangeType"
	}
}

// ToString returns the operator as a string
func (op UnaryExprOperator) ToString() string {
	switch op {
	case UPlusOp:
		return UPlusStr
	case UMinusOp:
		return UMinusStr
	case TildaOp:
		return TildaStr
	case BangOp:
		return BangStr
	case NStringOp:
		return NStringStr
	default:
		return "Unknown UnaryExprOperator"
	}
}

// ToString returns the option as a string
func (option MatchExprOption) ToString() string {
	switch option {
	case NoOption:
		return NoOptionStr
	case BooleanModeOpt:
		return BooleanModeStr
	case NaturalLanguageModeOpt:
		return NaturalLanguageModeStr
	case NaturalLanguageModeWithQueryExpansionOpt:
		return NaturalLanguageModeWithQueryExpansionStr
	case QueryExpansionOpt:
		return QueryExpansionStr
	default:
		return "Unknown MatchExprOption"
	}
}

// ToString returns the direction as a string
func (dir OrderDirection) ToString() string {
	switch dir {
	case AscOrder:
		return AscScr
	case DescOrder:
		return DescScr
	default:
		return "Unknown OrderDirection"
	}
}

// ToString returns the type as a string
func (ty IndexHintType) ToString() string {
	switch ty {
	case UseOp:
		return UseStr
	case IgnoreOp:
		return IgnoreStr
	case ForceOp:
		return ForceStr
	default:
		return "Unknown IndexHintType"
	}
}

// ToString returns the type as a string
func (ty IndexHintForType) ToString() string {
	switch ty {
	case NoForType:
		return ""
	case JoinForType:
		return JoinForStr
	case GroupByForType:
		return GroupByForStr
	case OrderByForType:
		return OrderByForStr
	default:
		return "Unknown IndexHintForType"
	}
}

// ToString returns the type as a string
func (ty TrimFuncType) ToString() string {
	switch ty {
	case NormalTrimType:
		return NormalTrimStr
	case LTrimType:
		return LTrimStr
	case RTrimType:
		return RTrimStr
	default:
		return "Unknown TrimFuncType"
	}
}

// ToString returns the type as a string
func (ty TrimType) ToString() string {
	switch ty {
	case NoTrimType:
		return ""
	case BothTrimType:
		return BothTrimStr
	case LeadingTrimType:
		return LeadingTrimStr
	case TrailingTrimType:
		return TrailingTrimStr
	default:
		return "Unknown TrimType"
	}
}

// ToString returns the type as a string
func (ty FrameUnitType) ToString() string {
	switch ty {
	case FrameRowsType:
		return FrameRowsStr
	case FrameRangeType:
		return FrameRangeStr
	default:
		return "Unknown FrameUnitType"
	}
}

// ToString returns the type as a string
func (ty FramePointType) ToString() string {
	switch ty {
	case CurrentRowType:
		return CurrentRowStr
	case UnboundedPrecedingType:
		return UnboundedPrecedingStr
	case UnboundedFollowingType:
		return UnboundedFollowingStr
	case ExprPrecedingType:
		return ExprPrecedingStr
	case ExprFollowingType:
		return ExprFollowingStr
	default:
		return "Unknown FramePointType"
	}
}

// ToString returns the type as a string
func (ty ArgumentLessWindowExprType) ToString() string {
	switch ty {
	case CumeDistExprType:
		return CumeDistExprStr
	case DenseRankExprType:
		return DenseRankExprStr
	case PercentRankExprType:
		return PercentRankExprStr
	case RankExprType:
		return RankExprStr
	case RowNumberExprType:
		return RowNumberExprStr
	default:
		return "Unknown ArgumentLessWindowExprType"
	}
}

// ToString returns the type as a string
func (ty NullTreatmentType) ToString() string {
	switch ty {
	case RespectNullsType:
		return RespectNullsStr
	case IgnoreNullsType:
		return IgnoreNullsStr
	default:
		return "Unknown NullTreatmentType"
	}
}

// ToString returns the type as a string
func (ty FromFirstLastType) ToString() string {
	switch ty {
	case FromFirstType:
		return FromFirstStr
	case FromLastType:
		return FromLastStr
	default:
		return "Unknown FromFirstLastType"
	}
}

// ToString returns the type as a string
func (ty FirstOrLastValueExprType) ToString() string {
	switch ty {
	case FirstValueExprType:
		return FirstValueExprStr
	case LastValueExprType:
		return LastValueExprStr
	default:
		return "Unknown FirstOrLastValueExprType"
	}
}

// ToString returns the type as a string
func (ty LagLeadExprType) ToString() string {
	switch ty {
	case LagExprType:
		return LagExprStr
	case LeadExprType:
		return LeadExprStr
	default:
		return "Unknown LagLeadExprType"
	}
}

// ToString returns the type as a string
func (ty JSONAttributeType) ToString() string {
	switch ty {
	case DepthAttributeType:
		return DepthAttributeStr
	case ValidAttributeType:
		return ValidAttributeStr
	case TypeAttributeType:
		return TypeAttributeStr
	case LengthAttributeType:
		return LengthAttributeStr
	default:
		return "Unknown JSONAttributeType"
	}
}

// ToString returns the type as a string
func (ty JSONValueModifierType) ToString() string {
	switch ty {
	case JSONArrayAppendType:
		return JSONArrayAppendStr
	case JSONArrayInsertType:
		return JSONArrayInsertStr
	case JSONInsertType:
		return JSONInsertStr
	case JSONReplaceType:
		return JSONReplaceStr
	case JSONSetType:
		return JSONSetStr
	default:
		return "Unknown JSONValueModifierType"
	}
}

// ToString returns the type as a string
func (ty JSONValueMergeType) ToString() string {
	switch ty {
	case JSONMergeType:
		return JSONMergeStr
	case JSONMergePatchType:
		return JSONMergePatchStr
	case JSONMergePreserveType:
		return JSONMergePreserveStr
	default:
		return "Unknown JSONValueMergeType"
	}
}

// ToString returns the type as a string
func (ty LockingFuncType) ToString() string {
	switch ty {
	case GetLock:
		return GetLockStr
	case IsFreeLock:
		return IsFreeLockStr
	case IsUsedLock:
		return IsUsedLockStr
	case ReleaseAllLocks:
		return ReleaseAllLocksStr
	case ReleaseLock:
		return ReleaseLockStr
	default:
		return "Unknown LockingFuncType"
	}
}

// ToString returns the type as a string
func (ty PerformanceSchemaType) ToString() string {
	switch ty {
	case FormatBytesType:
		return FormatBytesStr
	case FormatPicoTimeType:
		return FormatPicoTimeStr
	case PsCurrentThreadIDType:
		return PsCurrentThreadIDStr
	case PsThreadIDType:
		return PsThreadIDStr
	default:
		return "Unknown PerformaceSchemaType"
	}
}

// ToString returns the type as a string
func (ty GTIDType) ToString() string {
	switch ty {
	case GTIDSubsetType:
		return GTIDSubsetStr
	case GTIDSubtractType:
		return GTIDSubtractStr
	case WaitForExecutedGTIDSetType:
		return WaitForExecutedGTIDSetStr
	case WaitUntilSQLThreadAfterGTIDSType:
		return WaitUntilSQLThreadAfterGTIDSStr
	default:
		return "Unknown GTIDType"
	}
}

// ToString returns the type as a string
func (ty ExplainType) ToString() string {
	switch ty {
	case EmptyType:
		return EmptyStr
	case TreeType:
		return TreeStr
	case JSONType:
		return JSONStr
	case VitessType:
		return VitessStr
	case VTExplainType:
		return VTExplainStr
	case TraditionalType:
		return TraditionalStr
	case AnalyzeType:
		return AnalyzeStr
	default:
		return "Unknown ExplainType"
	}
}

// ToString returns the type as a string
func (ty VExplainType) ToString() string {
	switch ty {
	case PlanVExplainType:
		return PlanStr
	case QueriesVExplainType:
		return QueriesStr
	case AllVExplainType:
		return AllVExplainStr
	default:
		return "Unknown VExplainType"
	}
}

// ToString returns the type as a string
func (ty IntervalTypes) ToString() string {
	switch ty {
	case IntervalYear:
		return YearStr
	case IntervalQuarter:
		return QuarterStr
	case IntervalMonth:
		return MonthStr
	case IntervalWeek:
		return WeekStr
	case IntervalDay:
		return DayStr
	case IntervalHour:
		return HourStr
	case IntervalMinute:
		return MinuteStr
	case IntervalSecond:
		return SecondStr
	case IntervalMicrosecond:
		return MicrosecondStr
	case IntervalYearMonth:
		return YearMonthStr
	case IntervalDayHour:
		return DayHourStr
	case IntervalDayMinute:
		return DayMinuteStr
	case IntervalDaySecond:
		return DaySecondStr
	case IntervalHourMinute:
		return HourMinuteStr
	case IntervalHourSecond:
		return HourSecondStr
	case IntervalMinuteSecond:
		return MinuteSecondStr
	case IntervalDayMicrosecond:
		return DayMicrosecondStr
	case IntervalHourMicrosecond:
		return HourMicrosecondStr
	case IntervalMinuteMicrosecond:
		return MinuteMicrosecondStr
	case IntervalSecondMicrosecond:
		return SecondMicrosecondStr
	default:
		return "Unknown IntervalType"
	}
}

// ToString returns the type as a string
func (sel SelectIntoType) ToString() string {
	switch sel {
	case IntoOutfile:
		return IntoOutfileStr
	case IntoOutfileS3:
		return IntoOutfileS3Str
	case IntoDumpfile:
		return IntoDumpfileStr
	default:
		return "Unknown Select Into Type"
	}
}

// ToString returns the type as a string
func (node DatabaseOptionType) ToString() string {
	switch node {
	case CharacterSetType:
		return CharacterSetStr
	case CollateType:
		return CollateStr
	case EncryptionType:
		return EncryptionStr
	default:
		return "Unknown DatabaseOptionType Type"
	}
}

// ToString returns the type as a string
func (ty LockType) ToString() string {
	switch ty {
	case Read:
		return ReadStr
	case ReadLocal:
		return ReadLocalStr
	case Write:
		return WriteStr
	case LowPriorityWrite:
		return LowPriorityWriteStr
	default:
		return "Unknown LockType"
	}
}

// ToString returns ShowCommandType as a string
func (ty ShowCommandType) ToString() string {
	switch ty {
	case Charset:
		return CharsetStr
	case Collation:
		return CollationStr
	case Column:
		return ColumnStr
	case CreateDb:
		return CreateDbStr
	case CreateE:
		return CreateEStr
	case CreateF:
		return CreateFStr
	case CreateProc:
		return CreateProcStr
	case CreateTbl:
		return CreateTblStr
	case CreateTr:
		return CreateTrStr
	case CreateV:
		return CreateVStr
	case Database:
		return DatabaseStr
	case Engines:
		return EnginesStr
	case FunctionC:
		return FunctionCStr
	case Function:
		return FunctionStr
	case GtidExecGlobal:
		return GtidExecGlobalStr
	case Index:
		return IndexStr
	case OpenTable:
		return OpenTableStr
	case Plugins:
		return PluginsStr
	case Privilege:
		return PrivilegeStr
	case ProcedureC:
		return ProcedureCStr
	case Procedure:
		return ProcedureStr
	case StatusGlobal:
		return StatusGlobalStr
	case StatusSession:
		return StatusSessionStr
	case Table:
		return TablesStr
	case TableStatus:
		return TableStatusStr
	case Trigger:
		return TriggerStr
	case VariableGlobal:
		return VariableGlobalStr
	case VariableSession:
		return VariableSessionStr
	case VGtidExecGlobal:
		return VGtidExecGlobalStr
	case VitessMigrations:
		return VitessMigrationsStr
	case VitessReplicationStatus:
		return VitessReplicationStatusStr
	case VitessShards:
		return VitessShardsStr
	case VitessTablets:
		return VitessTabletsStr
	case VitessTarget:
		return VitessTargetStr
	case VitessVariables:
		return VitessVariablesStr
	case VschemaTables:
		return VschemaTablesStr
	case VschemaVindexes:
		return VschemaVindexesStr
	case Warnings:
		return WarningsStr
	case Keyspace:
		return KeyspaceStr
	default:
		return "" +
			"Unknown ShowCommandType"
	}
}

// ToString returns the DropKeyType as a string
func (key DropKeyType) ToString() string {
	switch key {
	case PrimaryKeyType:
		return PrimaryKeyTypeStr
	case ForeignKeyType:
		return ForeignKeyTypeStr
	case NormalKeyType:
		return NormalKeyTypeStr
	case CheckKeyType:
		return CheckKeyTypeStr
	default:
		return "Unknown DropKeyType"
	}
}

// ToString returns the LockOptionType as a string
func (lock LockOptionType) ToString() string {
	switch lock {
	case NoneType:
		return NoneTypeStr
	case DefaultType:
		return DefaultTypeStr
	case SharedType:
		return SharedTypeStr
	case ExclusiveType:
		return ExclusiveTypeStr
	default:
		return "Unknown type LockOptionType"
	}
}

// ToString returns the string associated with JoinType
func (columnFormat ColumnFormat) ToString() string {
	switch columnFormat {
	case FixedFormat:
		return keywordStrings[FIXED]
	case DynamicFormat:
		return keywordStrings[DYNAMIC]
	case DefaultFormat:
		return keywordStrings[DEFAULT]
	default:
		return "Unknown column format type"
	}
}

// ToString returns the TxAccessMode type as a string
func (ty TxAccessMode) ToString() string {
	switch ty {
	case WithConsistentSnapshot:
		return WithConsistentSnapshotStr
	case ReadWrite:
		return ReadWriteStr
	case ReadOnly:
		return ReadOnlyStr
	default:
		return "Unknown Transaction Access Mode"
	}
}

// CompliantName is used to get the name of the bind variable to use for this column name
func (node *ColName) CompliantName() string {
	if !node.Qualifier.IsEmpty() {
		return node.Qualifier.Name.CompliantName() + "_" + node.Name.CompliantName()
	}
	return node.Name.CompliantName()
}

// isExprAliasForCurrentTimeStamp returns true if the Expr provided is an alias for CURRENT_TIMESTAMP
func isExprAliasForCurrentTimeStamp(expr Expr) bool {
	switch node := expr.(type) {
	case *FuncExpr:
		return node.Name.EqualString("current_timestamp") || node.Name.EqualString("now") || node.Name.EqualString("localtimestamp") || node.Name.EqualString("localtime")
	case *CurTimeFuncExpr:
		return node.Name.EqualString("current_timestamp") || node.Name.EqualString("now") || node.Name.EqualString("localtimestamp") || node.Name.EqualString("localtime")
	}
	return false
}

// AtCount represents the '@' count in IdentifierCI
type AtCount int

const (
	// NoAt represents no @
	NoAt AtCount = iota
	// SingleAt represents @
	SingleAt
	// DoubleAt represents @@
	DoubleAt
)

// encodeSQLString encodes the string as a SQL string.
func encodeSQLString(val string) string {
	return sqltypes.EncodeStringSQL(val)
}

// ToString prints the list of table expressions as a string
// To be used as an alternate for String for []TableExpr
func ToString(exprs []TableExpr) string {
	buf := NewTrackedBuffer(nil)
	prefix := ""
	for _, expr := range exprs {
		buf.astPrintf(nil, "%s%v", prefix, expr)
		prefix = ", "
	}
	return buf.String()
}

func formatIdentifier(id string) string {
	buf := NewTrackedBuffer(nil)
	formatID(buf, id, NoAt)
	return buf.String()
}

func formatAddress(address string) string {
	if len(address) > 0 && address[0] == '\'' {
		return address
	}
	buf := NewTrackedBuffer(nil)
	formatID(buf, address, NoAt)
	return buf.String()
}

// ContainsAggregation returns true if the expression contains aggregation
func ContainsAggregation(e SQLNode) bool {
	hasAggregates := false
	_ = Walk(func(node SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *Offset:
			// offsets here indicate that a possible aggregation has already been handled by an input
			// so we don't need to worry about aggregation in the original
			return false, nil
		case AggrFunc:
			hasAggregates = true
			return false, io.EOF
		}

		return true, nil
	}, e)
	return hasAggregates
}

// GetFirstSelect gets the first select statement
func GetFirstSelect(selStmt SelectStatement) *Select {
	if selStmt == nil {
		return nil
	}
	switch node := selStmt.(type) {
	case *Select:
		return node
	case *Union:
		return GetFirstSelect(node.Left)
	}
	panic("[BUG]: unknown type for SelectStatement")
}

// GetAllSelects gets all the select statement s
func GetAllSelects(selStmt SelectStatement) []*Select {
	switch node := selStmt.(type) {
	case *Select:
		return []*Select{node}
	case *Union:
		return append(GetAllSelects(node.Left), GetAllSelects(node.Right)...)
	}
	panic("[BUG]: unknown type for SelectStatement")
}

// SetArgName sets argument name.
func (es *ExtractedSubquery) SetArgName(n string) {
	es.argName = n
	es.updateAlternative()
}

// SetHasValuesArg sets has_values argument.
func (es *ExtractedSubquery) SetHasValuesArg(n string) {
	es.hasValuesArg = n
	es.updateAlternative()
}

// GetArgName returns argument name.
func (es *ExtractedSubquery) GetArgName() string {
	return es.argName
}

// GetHasValuesArg returns has values argument.
func (es *ExtractedSubquery) GetHasValuesArg() string {
	return es.hasValuesArg

}

func (es *ExtractedSubquery) updateAlternative() {
	switch original := es.Original.(type) {
	case *ExistsExpr:
		es.alternative = NewArgument(es.hasValuesArg)
	case *Subquery:
		es.alternative = NewArgument(es.argName)
	case *ComparisonExpr:
		// other_side = :__sq
		cmp := &ComparisonExpr{
			Left:     es.OtherSide,
			Right:    NewArgument(es.argName),
			Operator: original.Operator,
		}
		var expr Expr = cmp
		switch original.Operator {
		case InOp:
			// :__sq_has_values = 1 and other_side in ::__sq
			cmp.Right = NewListArg(es.argName)
			hasValue := &ComparisonExpr{Left: NewArgument(es.hasValuesArg), Right: NewIntLiteral("1"), Operator: EqualOp}
			expr = AndExpressions(hasValue, cmp)
		case NotInOp:
			// :__sq_has_values = 0 or other_side not in ::__sq
			cmp.Right = NewListArg(es.argName)
			hasValue := &ComparisonExpr{Left: NewArgument(es.hasValuesArg), Right: NewIntLiteral("0"), Operator: EqualOp}
			expr = &OrExpr{hasValue, cmp}
		}
		es.alternative = expr
	}
}

// ColumnName returns the alias if one was provided, otherwise prints the AST
func (ae *AliasedExpr) ColumnName() string {
	if !ae.As.IsEmpty() {
		return ae.As.String()
	}

	if col, ok := ae.Expr.(*ColName); ok {
		return col.Name.String()
	}

	return String(ae.Expr)
}

// AllAggregation returns true if all the expressions contain aggregation
func (s SelectExprs) AllAggregation() bool {
	for _, k := range s {
		if !ContainsAggregation(k) {
			return false
		}
	}
	return true
}

func isExprLiteral(expr Expr) bool {
	switch expr := expr.(type) {
	case *Literal:
		return true
	case BoolVal:
		return true
	case *UnaryExpr:
		return isExprLiteral(expr.Expr)
	default:
		return false
	}
}

func defaultRequiresParens(ct *ColumnType) bool {
	// in 5.7 null value should be without parenthesis, in 8.0 it is allowed either way.
	// so it is safe to not keep parenthesis around null.
	if _, isNullVal := ct.Options.Default.(*NullVal); isNullVal {
		return false
	}

	switch strings.ToUpper(ct.Type) {
	case "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYBLOB", "BLOB", "MEDIUMBLOB",
		"LONGBLOB", "JSON", "GEOMETRY", "POINT",
		"LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING",
		"MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return true
	}

	if isExprLiteral(ct.Options.Default) || isExprAliasForCurrentTimeStamp(ct.Options.Default) {
		return false
	}

	return true
}

// RemoveKeyspaceFromColName removes the Qualifier.Qualifier on all ColNames in the expression tree
func RemoveKeyspaceFromColName(expr Expr) {
	RemoveKeyspace(expr)
}

// RemoveKeyspace removes the Qualifier.Qualifier on all ColNames in the AST
func RemoveKeyspace(in SQLNode) {
	// Walk will only return an error if we return an error from the inner func. safe to ignore here
	_ = Walk(func(node SQLNode) (kontinue bool, err error) {
		switch col := node.(type) {
		case *ColName:
			if !col.Qualifier.Qualifier.IsEmpty() {
				col.Qualifier.Qualifier = NewIdentifierCS("")
			}
		}
		return true, nil
	}, in)
}

func convertStringToInt(integer string) int {
	val, _ := strconv.Atoi(integer)
	return val
}

// SplitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters. Outer parenthesis are removed. Precedence
// should be taken into account if expressions are recombined.
func SplitAndExpression(filters []Expr, node Expr) []Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *AndExpr:
		filters = SplitAndExpression(filters, node.Left)
		return SplitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}

// AndExpressions ands together two or more expressions, minimising the expr when possible
func AndExpressions(exprs ...Expr) Expr {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		result := (Expr)(nil)
	outer:
		// we'll loop and remove any duplicates
		for i, expr := range exprs {
			if expr == nil {
				continue
			}
			if result == nil {
				result = expr
				continue outer
			}

			for j := 0; j < i; j++ {
				if Equals.Expr(expr, exprs[j]) {
					continue outer
				}
			}
			result = &AndExpr{Left: result, Right: expr}
		}
		return result
	}
}

// Equals is the default Comparator for AST expressions.
var Equals = &Comparator{}

// ToString returns the type as a string
func (ty GeomPropertyType) ToString() string {
	switch ty {
	case IsEmpty:
		return IsEmptyStr
	case IsSimple:
		return IsSimpleStr
	case Envelope:
		return EnvelopeStr
	case GeometryType:
		return GeometryTypeStr
	case Dimension:
		return DimensionStr
	default:
		return "Unknown GeomPropertyType"
	}
}

// ToString returns the type as a string
func (ty PointPropertyType) ToString() string {
	switch ty {
	case XCordinate:
		return XCordinateStr
	case YCordinate:
		return YCordinateStr
	case Latitude:
		return LatitudeStr
	case Longitude:
		return LongitudeStr
	default:
		return "Unknown PointPropertyType"
	}
}

// ToString returns the type as a string
func (ty LinestrPropType) ToString() string {
	switch ty {
	case EndPoint:
		return EndPointStr
	case IsClosed:
		return IsClosedStr
	case Length:
		return LengthStr
	case NumPoints:
		return NumPointsStr
	case PointN:
		return PointNStr
	case StartPoint:
		return StartPointStr
	default:
		return "Unknown LinestrPropType"
	}
}

// ToString returns the type as a string
func (ty PolygonPropType) ToString() string {
	switch ty {
	case Area:
		return AreaStr
	case Centroid:
		return CentroidStr
	case ExteriorRing:
		return ExteriorRingStr
	case InteriorRingN:
		return InteriorRingNStr
	case NumInteriorRings:
		return NumInteriorRingsStr
	default:
		return "Unknown PolygonPropType"
	}
}

// ToString returns the type as a string
func (ty GeomCollPropType) ToString() string {
	switch ty {
	case GeometryN:
		return GeometryNStr
	case NumGeometries:
		return NumGeometriesStr
	default:
		return "Unknown GeomCollPropType"
	}
}

// ToString returns the type as a string
func (ty GeomFromHashType) ToString() string {
	switch ty {
	case LatitudeFromHash:
		return LatitudeFromHashStr
	case LongitudeFromHash:
		return LongitudeFromHashStr
	case PointFromHash:
		return PointFromHashStr
	default:
		return "Unknown GeomFromGeoHashType"
	}
}

// ToString returns the type as a string
func (ty GeomFormatType) ToString() string {
	switch ty {
	case BinaryFormat:
		return BinaryFormatStr
	case TextFormat:
		return TextFormatStr
	default:
		return "Unknown GeomFormatType"
	}
}

// ToString returns the type as a string
func (ty GeomFromWktType) ToString() string {
	switch ty {
	case GeometryFromText:
		return GeometryFromTextStr
	case GeometryCollectionFromText:
		return GeometryCollectionFromTextStr
	case PointFromText:
		return PointFromTextStr
	case PolygonFromText:
		return PolygonFromTextStr
	case LineStringFromText:
		return LineStringFromTextStr
	case MultiPointFromText:
		return MultiPointFromTextStr
	case MultiLinestringFromText:
		return MultiLinestringFromTextStr
	case MultiPolygonFromText:
		return MultiPolygonFromTextStr
	default:
		return "Unknown GeomFromWktType"
	}
}

// ToString returns the type as a string
func (ty GeomFromWkbType) ToString() string {
	switch ty {
	case GeometryFromWKB:
		return GeometryFromWKBStr
	case GeometryCollectionFromWKB:
		return GeometryCollectionFromWKBStr
	case PointFromWKB:
		return PointFromWKBStr
	case PolygonFromWKB:
		return PolygonFromWKBStr
	case LineStringFromWKB:
		return LineStringFromWKBStr
	case MultiPointFromWKB:
		return MultiPointFromWKBStr
	case MultiLinestringFromWKB:
		return MultiLinestringFromWKBStr
	case MultiPolygonFromWKB:
		return MultiPolygonFromWKBStr
	default:
		return "Unknown GeomFromWktType"
	}
}

func getAliasedTableExprFromTableName(tblName TableName) *AliasedTableExpr {
	return &AliasedTableExpr{
		Expr: tblName,
	}
}
