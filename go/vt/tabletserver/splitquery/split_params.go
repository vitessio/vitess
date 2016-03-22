package splitquery

import (
	"fmt"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// SplitParams stores the parameters for splitting a query.
type SplitParams struct {
	sql                 string
	bindVariables       map[string]interface{}
	splitColumns        []string
	splitColumnTypes    []querypb.Type
	splitCount          int64
	numRowsPerQueryPart int64

	// Will contain the AST for the SELECT query.
	selectAST *sqlparser.Select
}

// NewSplitParamsWithNumRowsPerQueryPart returns a new SplitParams object.
// Parameters:
// 'sql' is the SQL query to split. The query must satisfy the restrictions found in the
// documentation of the vtgate.SplitQueryRequest.query protocol buffer field.
// 'bindVariables' are the bind-variables for the sql query.
// 'splitColumns' the list of splitColumns to use. These must adhere to the restrictions found in
// the documentation of the vtgate.SplitQueryRequest.split_column protocol buffer field.
// If splitColumns is nil, the split columns used are the primary key columns (in order).
func NewSplitParamsWithNumRowsPerQueryPart(
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	numRowsPerQueryPart int64,
	schema map[string]*schema.Table) (*SplitParams, error) {
	result, err := newSplitParams(sql, bindVariables, splitColumns, schema)
	if err != nil {
		return nil, err
	}
	result.numRowsPerQueryPart = numRowsPerQueryPart
	return result, nil
}

func newSplitParams(sql string, bindVariables map[string]interface{}, splitColumns []string,
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
	table, ok := schema[tableName]
	if table == nil {
		return nil, fmt.Errorf("can't find table in schema")
	}
	if len(splitColumns) == 0 {
		// TODO(erez): Make split-columns default to the primary key columns
		return nil, fmt.Errorf("split-columns are empty")
	}
	// Get the split-columns types.
	splitColumnTypes := make([]querypb.Type, 0, len(splitColumns))
	for _, splitColumn := range splitColumns {
		i := table.FindColumn(splitColumn)
		if i == -1 {
			return nil, fmt.Errorf("can't find split-column: %v", splitColumn)
		}
		splitColumnTypes = append(splitColumnTypes, table.Columns[i].Type)
	}

	// TODO(erez): Check that splitColumns are a prefix of an index.
	return &SplitParams{
		sql:              sql,
		bindVariables:    bindVariables,
		splitColumns:     splitColumns,
		splitColumnTypes: splitColumnTypes,
		selectAST:        selectAST,
	}, nil
}
