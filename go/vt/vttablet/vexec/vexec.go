package vexec

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	// TableQualifier is the standard schema used by VExec commands
	TableQualifier = "_vt"
)

// ValColumns map column name to SQLVal, for col=Val expressions in a WHERE clause
type ValColumns map[string](*sqlparser.SQLVal)

// TabletVExec is a utility structure, created when a VExec command is intercepted on the tablet.
// This structure will parse and analyze the query, and make available some useful data.
// VExec interceptors receive an instance of this struct so they can run more analysis/checks
// on the given query, and potentially modify it.
type TabletVExec struct {
	Workflow   string
	Keyspace   string
	Query      string
	Stmt       sqlparser.Statement
	TableName  string
	WhereCols  ValColumns
	UpdateCols ValColumns
}

// NewTabletVExec creates a new instance of TabletVExec
func NewTabletVExec(workflow, keyspace string) *TabletVExec {
	return &TabletVExec{
		Workflow: workflow,
		Keyspace: keyspace,
	}
}

// ColumnStringVal returns a string value from a given column, or error if the column is not found
func (e *TabletVExec) ColumnStringVal(columns ValColumns, colName string) (string, error) {
	val, ok := columns[colName]
	if !ok {
		return "", fmt.Errorf("Could not find value for column %s", colName)
	}
	return string(val.Val), nil
}

// analyzeWhereColumns identifies column names in a WHERE clause that have a comparison expression
// e.g. will return `keyspace` in a "WHERE keyspace='abc'"
// will not return `keyspace` in a "WHERE keyspace LIKE '%'"
func (e *TabletVExec) analyzeWhereEqualsColumns(where *sqlparser.Where) ValColumns {
	cols := ValColumns{}
	if where == nil {
		return cols
	}
	exprs := sqlparser.SplitAndExpression(nil, where.Expr)
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.ComparisonExpr:
			if expr.Operator != sqlparser.EqualStr {
				continue
			}
			qualifiedName, ok := expr.Left.(*sqlparser.ColName)
			if !ok {
				continue
			}
			if val, ok := expr.Right.(*sqlparser.SQLVal); ok {
				cols[qualifiedName.Name.String()] = val
			}
		}
	}
	return cols
}

// analyzeUpdateColumns analyses the columns modified by an UPDATE statement.
// it returns the columns that are updated with a literal
// e.g. in this statement: UPDATE tbl SET name='foo', val=3, status=other_column+2
// the function returns name: 'foo' and val: 3, but does not return `status` column
func (e *TabletVExec) analyzeUpdateColumns(update *sqlparser.Update) ValColumns {
	cols := ValColumns{}
	for _, col := range update.Exprs {
		if val, ok := col.Expr.(*sqlparser.SQLVal); ok {
			cols[col.Name.Name.Lowered()] = val
		}
	}
	return cols
}

// analyzeStatement analyzes a given statement and produces the following ingredients, useful for
// VExec interceptors:
// - table name
// - column names with values, for col=VAL in a WHERE expression
//   e.g. in "UPDATE my_table SET ... WHERE keyspace='test' AND shard='-80' AND status > 2", the
//   ValColumns are "keyspace" and "shard" with matching values. `status` is a range operator therefore
//   not included.package vexec
//   Equals operator is of special importance because it is known to filter results. An interceptor may
//   require, for example, that a `DELETE` statement includes a WHERE with a UNIQUE KEY column with Equals operator
//   to ensure we're not doing anything too risky.
func (e *TabletVExec) analyzeStatement() error {
	switch stmt := e.Stmt.(type) {
	case *sqlparser.Update:
		e.TableName = sqlparser.String(stmt.TableExprs)
		e.WhereCols = e.analyzeWhereEqualsColumns(stmt.Where)
		e.UpdateCols = e.analyzeUpdateColumns(stmt)
	case *sqlparser.Delete:
		e.TableName = sqlparser.String(stmt.TableExprs)
		e.WhereCols = e.analyzeWhereEqualsColumns(stmt.Where)
	case *sqlparser.Select:
		e.TableName = sqlparser.String(stmt.From)
		e.WhereCols = e.analyzeWhereEqualsColumns(stmt.Where)
	default:
		return fmt.Errorf("query not supported by vexec: %+v", sqlparser.String(stmt))
	}
	return nil
}

// AnalyzeQuery analyzes a given statement and produces the following ingredients, useful for
// VExec interceptors:
// - parsed statement
// - table name
// - column names with values, for col=VAL in a WHERE expression
//   e.g. in "UPDATE my_table SET ... WHERE keyspace='test' AND shard='-80' AND status > 2", the
//   ValColumns are "keyspace" and "shard" with matching values. `status` is a range operator therefore
//   not included.package vexec
//   Equals operator is of special importance because it is known to filter results. An interceptor may
//   require, for example, that a `DELETE` statement includes a WHERE with a UNIQUE KEY column with Equals operator
//   to ensure we're not doing anything too risky.
func (e *TabletVExec) AnalyzeQuery(ctx context.Context, query string) (err error) {
	if e.Stmt, err = sqlparser.Parse(query); err != nil {
		return err
	}
	e.Query = query
	if err := e.analyzeStatement(); err != nil {
		return err
	}
	return nil
}
