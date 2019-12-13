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

package splitquery

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// FullScanAlgorithm implements the SplitAlgorithmInterface and represents the full-scan algorithm
// for generating the boundary tuples. The algorithm regards the table as ordered (ascendingly) by
// the split columns. It then returns boundary tuples from rows which are
// splitParams.numRowsPerQueryPart rows apart. More precisely, it works as follows:
// It iteratively executes the following query over the replica’s database (recall that MySQL
// performs tuple comparisons lexicographically):
//    SELECT <split_columns> FROM <table> FORCE INDEX (PRIMARY)
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
	initialQuery          *querypb.BoundQuery
	noninitialQuery       *querypb.BoundQuery
}

// NewFullScanAlgorithm constructs a new FullScanAlgorithm.
func NewFullScanAlgorithm(
	splitParams *SplitParams, sqlExecuter SQLExecuter) (*FullScanAlgorithm, error) {

	if !splitParams.areSplitColumnsPrimaryKey() {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"Using the FULL_SCAN algorithm requires split columns to be"+
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
	// We used to have a safety check that makes sure the number of iterations does not
	// exceed 10*a.splitParams.splitCount. The splitCount parameter was calculated from
	// the estimated number of rows in the information schema, which could have been grossly
	// inaccurate (more than 10 times too low).
	for iteration = 0; prevTuple != nil; iteration++ {
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
	prevTuple tuple, bindVariables map[string]*querypb.BindVariable) {
	if len(prevTuple) != len(a.prevBindVariableNames) {
		panic(fmt.Sprintf("len(prevTuple) != len(a.prevBindVariableNames): %v != %v",
			len(prevTuple), len(a.prevBindVariableNames)))
	}
	for i, tupleElement := range prevTuple {
		bindVariables[a.prevBindVariableNames[i]] = sqltypes.ValueBindVariable(tupleElement)
	}
}

// buildInitialQuery returns the initial query to execute to get the
// initial boundary tuple.
// If the query to split (given in splitParams.sql) is
//    "SELECT <select exprs> FROM <table> WHERE <where>",
// the Sql field of the result will be:
// "SELECT sc_1,sc_2,...,sc_n FROM <table>
//                            FORCE INDEX (PRIMARY)
//                            ORDER BY <split_columns>
//                            LIMIT splitParams.numRowsPerQueryPart, 1",
// The BindVariables field of the result will contain a deep-copy of splitParams.BindVariables.
func buildInitialQuery(splitParams *SplitParams) *querypb.BoundQuery {
	resultSelectAST := buildInitialQueryAST(splitParams)
	return &querypb.BoundQuery{
		Sql:           sqlparser.String(resultSelectAST),
		BindVariables: cloneBindVariables(splitParams.bindVariables),
	}
}

func buildInitialQueryAST(splitParams *SplitParams) *sqlparser.Select {
	return &sqlparser.Select{
		SelectExprs: convertColumnsToSelectExprs(splitParams.splitColumns),
		// For the scanning here, we override any specified index hint to be the
		// PRIMARY index. If we do not override, even if the user doesn't specify a hint, MySQL
		// sometimes decides to use a different index than the primary key which results with a
		// significant increase in running time.
		// Note that we do not override the index for the actual query part since the list of
		// columns selected there is different; so overriding it there may hurt performance.
		From:    buildFromClause(splitParams.GetSplitTableName()),
		Limit:   buildLimitClause(splitParams.numRowsPerQueryPart, 1),
		OrderBy: buildOrderByClause(splitParams.splitColumns),
	}
}

// buildNonInitialQuery returns the non-initial query to execute to get the
// noninitial boundary tuples.
// If the query to split (given in splitParams.sql) is
//    "SELECT <select exprs> FROM <table> WHERE <where>",
// the Sql field of the result will be:
// "SELECT sc_1,sc_2,...,sc_n FROM <table> FORCE INDEX (PRIMARY)
//                            WHERE :prev_sc_1,...,:prev_sc_n) <= (sc_1,...,sc_n)
//                            ORDER BY <split_columns>
//                            LIMIT splitParams.numRowsPerQueryPart, 1",
// where sc_1,...,sc_n are the split columns,
// and :prev_sc_1,...,:_prev_sc_n are the bind variable names for the previous tuple.
// The BindVariables field of the result will contain a deep-copy of splitParams.BindVariables.
// The new "prev_<sc>" bind variables are not populated yet.
func buildNoninitialQuery(
	splitParams *SplitParams, prevBindVariableNames []string) *querypb.BoundQuery {
	resultSelectAST := buildInitialQueryAST(splitParams)
	addAndTermToWhereClause(
		resultSelectAST,
		constructTupleInequality(
			convertBindVariableNamesToExpr(prevBindVariableNames),
			convertColumnsToExpr(splitParams.splitColumns),
			false /* strict */))
	return &querypb.BoundQuery{
		Sql:           sqlparser.String(resultSelectAST),
		BindVariables: cloneBindVariables(splitParams.bindVariables),
	}
}

func convertColumnsToSelectExprs(columns []*schema.TableColumn) sqlparser.SelectExprs {
	result := make([]sqlparser.SelectExpr, 0, len(columns))
	for _, column := range columns {
		result = append(result,
			&sqlparser.AliasedExpr{
				Expr: &sqlparser.ColName{
					Name: column.Name,
				},
			})
	}
	return result
}

func buildFromClause(splitTableName sqlparser.TableIdent) sqlparser.TableExprs {
	return sqlparser.TableExprs{
		&sqlparser.AliasedTableExpr{
			Expr: sqlparser.TableName{Name: splitTableName},
			Hints: &sqlparser.IndexHints{
				Type:    sqlparser.ForceStr,
				Indexes: []sqlparser.ColIdent{sqlparser.NewColIdent("PRIMARY")},
			},
		},
	}
}

func buildLimitClause(offset, rowcount int64) *sqlparser.Limit {
	return &sqlparser.Limit{
		Offset:   sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", offset))),
		Rowcount: sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", rowcount))),
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
		result = append(result, prevBindVariablePrefix+splitColumn.Name.CompliantName())
	}
	return result
}

func (a *FullScanAlgorithm) executeQuery(boundQuery *querypb.BoundQuery) (tuple, error) {
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
			panic(fmt.Sprintf("splitquery.executeQuery: expected a tuple of length %v."+
				" Got tuple: %v", len(a.splitParams.splitColumns), sqlResult.Rows[0]))
		}
		return sqlResult.Rows[0], nil
	}
	panic(fmt.Sprintf("splitquery.executeQuery: got more than 1 row from query: %v. result: %v",
		*boundQuery, sqlResult))
}
