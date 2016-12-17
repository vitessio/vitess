package splitquery

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

// FullScanAlgorithm implements the SplitAlgorithmInterface and represents the full-scan algorithm
// for generating the boundary tuples. The algorithm regards the table as ordered (ascendingly) by
// the split columns. It then returns boundary tuples from rows which are
// splitParams.numRowsPerQueryPart rows apart. More precisely, it works as follows:
// It iteratively executes the following query over the replica’s database (recall that MySQL
// performs tuple comparisons lexicographically):
//    SELECT <split_columns> FROM <table>
//                           WHERE :prev_boundary <= (<split_columns>)
//                           ORDER BY <split_columns>
//                           LIMIT <num_rows_per_query_part>, 1
// where <split_columns> denotes the ordered list of split columns and <table> is the
// value of the FROM clause.
// The 'prev_boundary' bind variable holds a tuple consisting of split column values.
// It is updated after each iteration with the result of the query. In the query executed in the
// first iteration (the initial query) the term ':prev_boundary <= (<split_columns>)' is
// omitted.
// The algorithm stops when the query returns no results. The result of this algorithm is the list
// consisting of the result of each query in order.
//
// Actually, the code below differs slightly from the above description: the lexicographial tuple
// inequality in the query above is re-written to use only scalar comparisons since MySQL
// does not optimize queries involving tuple inequalities correctly. Instead of using a single
// tuple bind variable: 'prev_boundary', the code uses a list of scalar bind-variables--one for each
// element of the tuple. The bind variable storing the tuple element corresponding to a
// split-column named 'x' is called <prevBindVariablePrefix><x>, where prevBindVariablePrefix is
// the string constant defined below.
type FullScanAlgorithm struct {
	splitParams *SplitParams
	sqlExecuter SQLExecuter

	prevBindVariableNames []string
	initialQuery          *querytypes.BoundQuery
	noninitialQuery       *querytypes.BoundQuery
}

// NewFullScanAlgorithm constructs a new FullScanAlgorithm.
func NewFullScanAlgorithm(
	splitParams *SplitParams, sqlExecuter SQLExecuter) (*FullScanAlgorithm, error) {

	if !splitParams.areSplitColumnsPrimaryKey() {
		return nil, fmt.Errorf("Using the FULL_SCAN algorithm requires split columns to be"+
			" the primary key. Got: %+v", splitParams)
	}
	result := &FullScanAlgorithm{
		splitParams:           splitParams,
		sqlExecuter:           sqlExecuter,
		prevBindVariableNames: buildPrevBindVariableNames(splitParams.splitColumns),
	}
	result.initialQuery = buildInitialQuery(splitParams)
	result.noninitialQuery = buildNoninitialQuery(splitParams, result.prevBindVariableNames)
	return result, nil
}

// getSplitColumns is part of the SplitAlgorithmInterface interface
func (a *FullScanAlgorithm) getSplitColumns() []*schema.TableColumn {
	return a.splitParams.splitColumns
}

func (a *FullScanAlgorithm) generateBoundaries() ([]tuple, error) {
	prevTuple, err := a.executeQuery(a.initialQuery)
	if err != nil {
		return nil, err
	}
	result := make([]tuple, 0, a.splitParams.splitCount)
	var iteration int64
	// We have to allow for more than splitCount query-parts since we use an estimated
	// tableSize in the equation 'numRowsPerQueryPart * splitCount = tableSize'.
	maxIterations := 10 * a.splitParams.splitCount
	for iteration = 0; prevTuple != nil; iteration++ {
		if iteration > maxIterations {
			panic(fmt.Sprintf("splitquery.FullScanAlgorithm.generateBoundaries(): didn't terminate"+
				" after %v iterations (=10*splitCount). FullScanAlgorithm: %v", maxIterations, a))
		}
		result = append(result, prevTuple)
		a.populatePrevTupleInBindVariables(prevTuple, a.noninitialQuery.BindVariables)
		prevTuple, err = a.executeQuery(a.noninitialQuery)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (a *FullScanAlgorithm) populatePrevTupleInBindVariables(
	prevTuple tuple, bindVariables map[string]interface{}) {
	if len(prevTuple) != len(a.prevBindVariableNames) {
		panic(fmt.Sprintf("len(prevTuple) != len(a.prevBindVariableNames): %v != %v",
			len(prevTuple), len(a.prevBindVariableNames)))
	}
	for i, tupleElement := range prevTuple {
		bindVariables[a.prevBindVariableNames[i]] = tupleElement.ToNative()
	}
}

// buildInitialQuery returns the initial query to execute to get the
// initial boundary tuple.
// If the query to split (given in splitParams.sql) is
//    "SELECT <select exprs> FROM <table> WHERE <where>",
// the Sql field of the result will be:
// "SELECT sc_1,sc_2,...,sc_n FROM <table>
//                            ORDER BY <split_columns>
//                            LIMIT splitParams.numRowsPerQueryPart, 1",
// The BindVariables field of the result will contain a deep-copy of splitParams.BindVariables.
func buildInitialQuery(splitParams *SplitParams) *querytypes.BoundQuery {
	resultSelectAST := buildInitialQueryAST(splitParams)
	return &querytypes.BoundQuery{
		Sql:           sqlparser.String(resultSelectAST),
		BindVariables: cloneBindVariables(splitParams.bindVariables),
	}
}

func buildInitialQueryAST(splitParams *SplitParams) *sqlparser.Select {
	if splitParams.selectAST.Limit != nil {
		panic(fmt.Sprintf(
			"splitquery.buildInitialQueryAST(): splitParams query already has a LIMIT clause: %v",
			*splitParams))
	}
	resultSelectAST := *splitParams.selectAST
	resultSelectAST.Where = nil
	resultSelectAST.SelectExprs = convertColumnsToSelectExprs(splitParams.splitColumns)
	resultSelectAST.Limit = buildLimitClause(splitParams.numRowsPerQueryPart, 1)
	resultSelectAST.OrderBy = buildOrderByClause(splitParams.splitColumns)
	return &resultSelectAST
}

// buildNonInitialQuery returns the non-initial query to execute to get the
// noninitial boundary tuples.
// If the query to split (given in splitParams.sql) is
//    "SELECT <select exprs> FROM <table> WHERE <where>",
// the Sql field of the result will be:
// "SELECT sc_1,sc_2,...,sc_n FROM <table>
//                            WHERE :prev_sc_1,...,:prev_sc_n) <= (sc_1,...,sc_n)
//                            ORDER BY <split_columns>
//                            LIMIT splitParams.numRowsPerQueryPart, 1",
// where sc_1,...,sc_n are the split columns,
// and :prev_sc_1,...,:_prev_sc_n are the bind variable names for the previous tuple.
// The BindVariables field of the result will contain a deep-copy of splitParams.BindVariables.
// The new "prev_<sc>" bind variables are not populated yet.
func buildNoninitialQuery(
	splitParams *SplitParams, prevBindVariableNames []string) *querytypes.BoundQuery {
	resultSelectAST := buildInitialQueryAST(splitParams)
	addAndTermToWhereClause(
		resultSelectAST,
		constructTupleInequality(
			convertBindVariableNamesToValExpr(prevBindVariableNames),
			convertColumnsToValExpr(splitParams.splitColumns),
			false /* strict */))
	return &querytypes.BoundQuery{
		Sql:           sqlparser.String(resultSelectAST),
		BindVariables: cloneBindVariables(splitParams.bindVariables),
	}
}

func convertColumnsToSelectExprs(columns []*schema.TableColumn) sqlparser.SelectExprs {
	result := make([]sqlparser.SelectExpr, 0, len(columns))
	for _, column := range columns {
		result = append(result,
			&sqlparser.NonStarExpr{
				Expr: &sqlparser.ColName{
					Name: column.Name,
				},
			})
	}
	return result
}

func buildLimitClause(offset, rowcount int64) *sqlparser.Limit {
	return &sqlparser.Limit{
		Offset:   sqlparser.NumVal(fmt.Sprintf("%d", offset)),
		Rowcount: sqlparser.NumVal(fmt.Sprintf("%d", rowcount)),
	}
}

func buildOrderByClause(splitColumns []*schema.TableColumn) sqlparser.OrderBy {
	result := make(sqlparser.OrderBy, 0, len(splitColumns))
	for _, splitColumn := range splitColumns {
		result = append(result,
			&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: splitColumn.Name},
				Direction: sqlparser.AscScr,
			},
		)
	}
	return result
}

const (
	prevBindVariablePrefix string = "_splitquery_prev_"
)

func buildPrevBindVariableNames(splitColumns []*schema.TableColumn) []string {
	result := make([]string, 0, len(splitColumns))
	for _, splitColumn := range splitColumns {
		result = append(result, prevBindVariablePrefix+splitColumn.Name.Lowered())
	}
	return result
}

func (a *FullScanAlgorithm) executeQuery(boundQuery *querytypes.BoundQuery) (tuple, error) {
	sqlResult, err := a.sqlExecuter.SQLExecute(
		boundQuery.Sql,
		boundQuery.BindVariables)
	if err != nil {
		return nil, err
	}
	if len(sqlResult.Rows) == 0 {
		return nil, nil
	}
	if len(sqlResult.Rows) == 1 {
		if len(sqlResult.Rows[0]) != len(a.splitParams.splitColumns) {
			panic(fmt.Sprintf("splitquery.executeQuery: expected a tuple of length %v. Got tuple: %v",
				len(a.splitParams.splitColumns), sqlResult.Rows[0]))
		}
		return sqlResult.Rows[0], nil
	}
	panic(fmt.Sprintf("splitquery.executeQuery: got more than 1 row from query: %v. result: %v",
		*boundQuery, sqlResult))
}
