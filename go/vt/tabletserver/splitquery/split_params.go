package splitquery

import (
	"fmt"

	"github.com/youtube/vitess/go/cistring"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// SplitParams stores the context for a splitquery computation. It is used by
// both the Splitter object and the SplitAlgorithm object and caches some data that is used by
// both.
type SplitParams struct {
	// The following fields are directly given by the caller -- they have a corresponding
	// parameter in each constructor.
	sql           string
	bindVariables map[string]interface{}
	splitColumns  []sqlparser.ColIdent
	// Exactly one of splitCount, numRowsPerQueryPart will be given by the caller.
	// See the two NewSplitParams... constructors below. The other field member
	// will be computed using the equation: max(1, floor(numTableRows / x)),
	// where numTableRows is the approximate number of rows in the table (referenced in 'sql') taken
	// from the information schema of the database, and 'x' is the given field member.
	splitCount          int64
	numRowsPerQueryPart int64

	// The fields below will be computed by the appropriate constructor.

	// The schema of the table referenced in the SELECT query given in 'sql'.
	splitTableSchema *schema.Table
	splitColumnTypes []querypb.Type
	// The AST for the SELECT query given in 'sql'.
	selectAST *sqlparser.Select
}

// NewSplitParamsGivenNumRowsPerQueryPart returns a new SplitParams object to be used in
// a splitquery request in which the Vitess client specified a numRowsPerQueryPart parameter.
// See NewSplitParamsGivenSplitCount for the constructor to use if the Vitess client specified
// a splitCount parameter.
//
// Parameters:
//
// 'sql' is the SQL query to split. The query must satisfy the restrictions found in the
// documentation of the vtgate.SplitQueryRequest.query protocol buffer field.
//
// 'bindVariables' are the bind-variables for the sql query.
//
// 'splitColumns' is the list of splitColumns to use. These must adhere to the restrictions found in
// the documentation of the vtgate.SplitQueryRequest.split_column protocol buffer field.
// If splitColumns is empty, the split columns used are the primary key columns (in order).
//
// 'numRowsPerQueryPart' is the desired number of rows per query part returned. The actual number
// may be different depending on the split-algorithm used.
//
// 'schema' should map a table name to a schema.Table. It is used for looking up the split-column
// types and error checking.
func NewSplitParamsGivenNumRowsPerQueryPart(
	sql string,
	bindVariables map[string]interface{},
	splitColumns []sqlparser.ColIdent,
	numRowsPerQueryPart int64,
	schema map[string]*schema.Table) (*SplitParams, error) {
	if numRowsPerQueryPart <= 0 {
		return nil, fmt.Errorf("numRowsPerQueryPart must be positive. Got: %v",
			numRowsPerQueryPart)
	}
	result, err := newSplitParams(sql, bindVariables, splitColumns, schema)
	if err != nil {
		return nil, err
	}
	result.numRowsPerQueryPart = numRowsPerQueryPart
	result.splitCount = int64Max(1, result.splitTableSchema.TableRows.Get()/numRowsPerQueryPart)
	return result, nil
}

// NewSplitParamsGivenSplitCount returns a new SplitParams object to be used in
// a splitquery request in which the Vitess client specified a splitCount parameter.
// See NewSplitParamsGivenNumRowsPerQueryPart for the constructor to use if the Vitess client
// specified a numRowsPerQueryPart parameter.
//
// Parameters:
//
// 'sql' is the SQL query to split. The query must satisfy the restrictions found in the
// documentation of the vtgate.SplitQueryRequest.query protocol buffer field.
//
// 'bindVariables' are the bind-variables for the sql query.
//
// 'splitColumns' is the list of splitColumns to use. These must adhere to the restrictions found in
// the documentation of the vtgate.SplitQueryRequest.split_column protocol buffer field.
// If splitColumns is empty, the split columns used are the primary key columns (in order).
//
// 'splitCount' is the desired splitCount to use. The actual number may be different depending on
// the split-algorithm used.
//
// 'schema' should map a table name to a schema.Table. It is used for looking up the split-column
// types and error checking.
func NewSplitParamsGivenSplitCount(
	sql string,
	bindVariables map[string]interface{},
	splitColumns []sqlparser.ColIdent,
	splitCount int64,
	schema map[string]*schema.Table) (*SplitParams, error) {

	if splitCount <= 0 {
		return nil, fmt.Errorf("splitCount must be positive. Got: %v",
			splitCount)
	}
	result, err := newSplitParams(sql, bindVariables, splitColumns, schema)
	if err != nil {
		return nil, err
	}
	result.splitCount = splitCount
	result.numRowsPerQueryPart = int64Max(1, result.splitTableSchema.TableRows.Get()/splitCount)
	return result, nil
}

// GetSplitTableName returns the name of the table to split.
func (sp *SplitParams) GetSplitTableName() string {
	return sp.splitTableSchema.Name
}

// newSplitParams validates and initializes all the fields except splitCount and
// numRowsPerQueryPart. It contains the common code for the constructors above.
func newSplitParams(sql string, bindVariables map[string]interface{}, splitColumns []sqlparser.ColIdent,
	schema map[string]*schema.Table) (*SplitParams, error) {

	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed parsing query: '%v', err: '%v'", sql, err)
	}
	selectAST, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a select statement")
	}
	if selectAST.Distinct != "" || selectAST.GroupBy != nil ||
		selectAST.Having != nil || len(selectAST.From) != 1 ||
		selectAST.OrderBy != nil || selectAST.Limit != nil ||
		selectAST.Lock != "" {
		return nil, fmt.Errorf("unsupported query: %v", sql)
	}
	var aliasedTableExpr *sqlparser.AliasedTableExpr
	aliasedTableExpr, ok = selectAST.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported FROM clause in query: %v", sql)
	}
	tableName := sqlparser.GetTableName(aliasedTableExpr.Expr)
	if tableName == "" {
		return nil, fmt.Errorf("unsupported FROM clause in query"+
			" (must be a simple table expression): %v", sql)
	}
	tableSchema, ok := schema[tableName]
	if tableSchema == nil {
		return nil, fmt.Errorf("can't find table in schema")
	}
	if len(splitColumns) == 0 {
		splitColumns = getPrimaryKeyColumns(tableSchema)
		if len(splitColumns) == 0 {
			panic(fmt.Sprintf("getPrimaryKeyColumns() returned an empty slice. %+v", tableSchema))
		}
	}
	if !areColumnsAPrefixOfAnIndex(splitColumns, tableSchema) {
		return nil, fmt.Errorf("split-columns must be a prefix of the columns composing"+
			" an index. Sql: %v, split-columns: %v", sql, splitColumns)
	}
	// Get the split-columns types.
	splitColumnTypes := make([]querypb.Type, 0, len(splitColumns))
	for _, splitColumn := range splitColumns {
		i := tableSchema.FindColumn(splitColumn.Val())
		if i == -1 {
			return nil, fmt.Errorf("can't find split-column: %v", splitColumn)
		}
		splitColumnTypes = append(splitColumnTypes, tableSchema.Columns[i].Type)
	}

	return &SplitParams{
		sql:              sql,
		bindVariables:    bindVariables,
		splitColumns:     splitColumns,
		splitColumnTypes: splitColumnTypes,
		selectAST:        selectAST,
		splitTableSchema: tableSchema,
	}, nil
}

// getPrimaryKeyColumns returns the list of primary-key column names, in order, for the
// given table.
func getPrimaryKeyColumns(table *schema.Table) []sqlparser.ColIdent {
	result := make([]sqlparser.ColIdent, 0, len(table.PKColumns))
	for _, pkColIndex := range table.PKColumns {
		result = append(result, sqlparser.ColIdent(table.Columns[pkColIndex].Name))
	}
	return result
}

// areColumnsAPrefixOfAnIndex returns true if 'columns' form a prefix of the columns that
// make up some index in 'table'.
func areColumnsAPrefixOfAnIndex(columns []sqlparser.ColIdent, table *schema.Table) bool {
	for _, index := range table.Indexes {
		if isColIdentSlicePrefix(columns, index.Columns) {
			return true
		}
	}
	return false
}

// isColIdentSlicePrefix returns true if 'potentialPrefix' is a prefix of the slice
// 'slice'.
func isColIdentSlicePrefix(potentialPrefix []sqlparser.ColIdent, slice []cistring.CIString) bool {
	if len(potentialPrefix) > len(slice) {
		return false
	}
	for i := range potentialPrefix {
		if potentialPrefix[i].Lowered() != slice[i].Lowered() {
			return false
		}
	}
	return true
}

// areSplitColumnsPrimaryKey returns true if the splitColumns in 'splitParams'
// are the primary key columns in order.
func (sp *SplitParams) areSplitColumnsPrimaryKey() bool {
	pkCols := getPrimaryKeyColumns(sp.splitTableSchema)
	if len(sp.splitColumns) != len(pkCols) {
		return false
	}
	for i := 0; i < len(sp.splitColumns); i++ {
		if sp.splitColumns[i].Lowered() != pkCols[i].Lowered() {
			return false
		}
	}
	return true
}
