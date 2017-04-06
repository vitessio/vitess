package splitquery

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// SplitParams stores the context for a splitquery computation. It is used by
// both the Splitter object and the SplitAlgorithm object and caches some data that is used by
// both.
type SplitParams struct {
	// The following fields are directly given by the caller -- they have a corresponding
	// parameter in each constructor.
	sql           string
	bindVariables map[string]interface{}

	// Exactly one of splitCount, numRowsPerQueryPart will be given by the caller.
	// See the two NewSplitParams... constructors below. The other field member
	// will be computed using the equation: max(1, floor(numTableRows / x)),
	// where numTableRows is the approximate number of rows in the table (referenced in 'sql') taken
	// from the information schema of the database, and 'x' is the given field member.
	splitCount          int64
	numRowsPerQueryPart int64

	// The fields below will be computed by the appropriate constructor.

	splitColumns []*schema.TableColumn
	// The schema of the table referenced in the SELECT query given in 'sql'.
	splitTableSchema *schema.Table
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
// 'splitColumnNames' should contain the names of split columns to use. These must adhere to the
// restrictions found in the documentation of the vtgate.SplitQueryRequest.split_column protocol
// buffer field. If splitColumnNames is empty, the split columns used are the primary key columns
// (in order).
//
// 'numRowsPerQueryPart' is the desired number of rows per query part returned. The actual number
// may be different depending on the split-algorithm used.
//
// 'schema' should map a table name to a schema.Table. It is used for looking up the split-column
// types and error checking.
func NewSplitParamsGivenNumRowsPerQueryPart(
	query querytypes.BoundQuery,
	splitColumnNames []sqlparser.ColIdent,
	numRowsPerQueryPart int64,
	schema map[string]*schema.Table,
) (*SplitParams, error) {
	if numRowsPerQueryPart <= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"numRowsPerQueryPart must be positive. Got: %v", numRowsPerQueryPart)
	}
	result, err := newSplitParams(query, splitColumnNames, schema)
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
// 'splitColumnNames' should contain the names of split columns to use. These must adhere to the
// restrictions found in the documentation of the vtgate.SplitQueryRequest.split_column protocol
// buffer field. If splitColumnNames is empty, the split columns used are the primary key columns
// (in order).
//
// 'splitCount' is the desired splitCount to use. The actual number may be different depending on
// the split-algorithm used.
//
// 'schema' should map a table name to a schema.Table. It is used for looking up the split-column
// types and error checking.
func NewSplitParamsGivenSplitCount(
	query querytypes.BoundQuery,
	splitColumnNames []sqlparser.ColIdent,
	splitCount int64,
	schema map[string]*schema.Table,
) (*SplitParams, error) {
	if splitCount <= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"splitCount must be positive. Got: %v", splitCount)
	}
	result, err := newSplitParams(query, splitColumnNames, schema)
	if err != nil {
		return nil, err
	}
	result.splitCount = splitCount
	result.numRowsPerQueryPart = int64Max(1, result.splitTableSchema.TableRows.Get()/splitCount)
	return result, nil
}

// GetSplitTableName returns the name of the table to split.
func (sp *SplitParams) GetSplitTableName() sqlparser.TableIdent {
	return sp.splitTableSchema.Name
}

// newSplitParams validates and initializes all the fields except splitCount and
// numRowsPerQueryPart. It contains the common code for the constructors above.
func newSplitParams(
	query querytypes.BoundQuery,
	splitColumnNames []sqlparser.ColIdent,
	schemaMap map[string]*schema.Table,
) (*SplitParams, error) {
	statement, err := sqlparser.Parse(query.Sql)
	if err != nil {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT, "failed parsing query: '%v', err: '%v'", query.Sql, err)
	}
	selectAST, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "not a select statement")
	}
	if selectAST.Distinct != "" || selectAST.GroupBy != nil ||
		selectAST.Having != nil || len(selectAST.From) != 1 ||
		selectAST.OrderBy != nil || selectAST.Limit != nil ||
		selectAST.Lock != "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported query: %v", query.Sql)
	}
	var aliasedTableExpr *sqlparser.AliasedTableExpr
	aliasedTableExpr, ok = selectAST.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_INVALID_ARGUMENT, "unsupported FROM clause in query: %v", query.Sql)
	}
	tableName := sqlparser.GetTableName(aliasedTableExpr.Expr)
	if tableName.IsEmpty() {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported FROM clause in query"+
			" (must be a simple table expression): %v", query.Sql)
	}
	tableSchema, ok := schemaMap[tableName.String()]
	if tableSchema == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "can't find table in schema")
	}

	// Get the schema.TableColumn representation of each splitColumnName.
	var splitColumns []*schema.TableColumn
	if len(splitColumnNames) == 0 {
		splitColumns = getPrimaryKeyColumns(tableSchema)
		if len(splitColumns) == 0 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"no split columns where given and the queried table has"+
					" no primary key columns (is the table a view? Running SplitQuery on a view"+
					" is not supported). query: %v", query.Sql)
		}
	} else {
		splitColumns, err = findSplitColumnsInSchema(splitColumnNames, tableSchema)
		if err != nil {
			return nil, err
		}
		if !areColumnsAPrefixOfAnIndex(splitColumns, tableSchema) {
			return nil, vterrors.Errorf(
				vtrpcpb.Code_INVALID_ARGUMENT,
				"split-columns must be a prefix of the columns composing"+
					" an index. Sql: %v, split-columns: %v", query.Sql, splitColumns)
		}
	}

	if len(splitColumns) == 0 {
		panic(fmt.Sprintf(
			"Empty set of split columns. splitColumns: %+v, tableSchema: %+v",
			splitColumns, tableSchema))
	}

	return &SplitParams{
		sql:              query.Sql,
		bindVariables:    query.BindVariables,
		splitColumns:     splitColumns,
		selectAST:        selectAST,
		splitTableSchema: tableSchema,
	}, nil
}

func findSplitColumnsInSchema(
	splitColumnNames []sqlparser.ColIdent, tableSchema *schema.Table,
) ([]*schema.TableColumn, error) {
	result := make([]*schema.TableColumn, 0, len(splitColumnNames))
	for _, splitColumnName := range splitColumnNames {
		i := tableSchema.FindColumn(splitColumnName)
		if i == -1 {
			return nil, vterrors.Errorf(
				vtrpcpb.Code_INVALID_ARGUMENT,
				"can't find split column: %v", splitColumnName)
		}
		result = append(result, &tableSchema.Columns[i])
	}
	return result, nil
}

// getPrimaryKeyColumns returns the list of primary-key column names, in order, for the
// given table.
func getPrimaryKeyColumns(table *schema.Table) []*schema.TableColumn {
	result := make([]*schema.TableColumn, 0, len(table.PKColumns))
	for _, pkColIndex := range table.PKColumns {
		result = append(result, &table.Columns[pkColIndex])
	}
	return result
}

// areColumnsAPrefixOfAnIndex returns true if 'columns' form a prefix of the columns that
// make up some index in 'table'.
func areColumnsAPrefixOfAnIndex(columns []*schema.TableColumn, table *schema.Table) bool {
	for _, index := range table.Indexes {
		if areColumnsAPrefixOfIndex(columns, index) {
			return true
		}
	}
	return false
}

// areColumnsAPrefixOfIndex returns true if 'potentialPrefix' forms a prefix of the columns
// composing 'index'.
func areColumnsAPrefixOfIndex(potentialPrefix []*schema.TableColumn, index *schema.Index) bool {
	if len(potentialPrefix) > len(index.Columns) {
		return false
	}
	for i := range potentialPrefix {
		if !potentialPrefix[i].Name.Equal(index.Columns[i]) {
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
	// Compare the names of sp.splitColumns to the names of pkCols.
	for i := 0; i < len(sp.splitColumns); i++ {
		if !sp.splitColumns[i].Name.Equal(pkCols[i].Name) {
			return false
		}
	}
	return true
}
