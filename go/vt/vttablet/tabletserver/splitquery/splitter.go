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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Splitter is used to drive the splitting procedure.
type Splitter struct {
	algorithm   SplitAlgorithmInterface
	splitParams *SplitParams

	startBindVariableNames []string
	endBindVariableNames   []string
	firstQueryPartSQL      string
	middleQueryPartSQL     string
	lastQueryPartSQL       string
}

// NewSplitter creates a new Splitter object.
func NewSplitter(splitParams *SplitParams, algorithm SplitAlgorithmInterface) *Splitter {
	var splitter Splitter
	splitter.splitParams = splitParams
	splitter.algorithm = algorithm
	splitter.splitParams = splitParams

	splitColumns := algorithm.getSplitColumns()
	splitter.startBindVariableNames = make([]string, 0, len(splitColumns))
	splitter.endBindVariableNames = make([]string, 0, len(splitColumns))
	for _, splitColumn := range splitColumns {
		splitter.startBindVariableNames = append(
			splitter.startBindVariableNames, startBindVariablePrefix+splitColumn.Name.CompliantName())
		splitter.endBindVariableNames = append(
			splitter.endBindVariableNames, endBindVariablePrefix+splitColumn.Name.CompliantName())
	}
	splitter.initQueryPartSQLs()
	return &splitter
}

// Split does the actual work of splitting the query.
// It returns a slice of *querypb.QuerySplit objects representing
// the query parts.
func (splitter *Splitter) Split() ([]*querypb.QuerySplit, error) {
	var boundaries []tuple
	var err error
	boundaries, err = splitter.algorithm.generateBoundaries()
	if err != nil {
		return nil, err
	}
	boundaries = append(boundaries, nil)
	splits := []*querypb.QuerySplit{}
	var start tuple
	for _, end := range boundaries {
		splits = append(splits, splitter.constructQueryPart(start, end))
		start = end
	}
	return splits, nil
}

// initQueryPartSQLs initializes the firstQueryPartSQL, middleQueryPartSQL and lastQueryPartSQL
// fields.
func (splitter *Splitter) initQueryPartSQLs() {
	splitColumns := convertColumnsToExpr(splitter.algorithm.getSplitColumns())
	startBindVariables := convertBindVariableNamesToExpr(splitter.startBindVariableNames)
	endBindVariables := convertBindVariableNamesToExpr(splitter.endBindVariableNames)
	splitColsLessThanEnd := constructTupleInequality(
		splitColumns,
		endBindVariables,
		true /* strict */)
	splitColsGreaterThanOrEqualToStart := constructTupleInequality(
		startBindVariables,
		splitColumns,
		false /* not strict */)

	splitter.firstQueryPartSQL = sqlparser.String(
		queryWithAdditionalWhere(splitter.splitParams.selectAST, splitColsLessThanEnd))
	splitter.middleQueryPartSQL = sqlparser.String(
		queryWithAdditionalWhere(splitter.splitParams.selectAST,
			&sqlparser.AndExpr{
				Left:  &sqlparser.ParenExpr{Expr: splitColsGreaterThanOrEqualToStart},
				Right: &sqlparser.ParenExpr{Expr: splitColsLessThanEnd},
			}))
	splitter.lastQueryPartSQL = sqlparser.String(
		queryWithAdditionalWhere(splitter.splitParams.selectAST, splitColsGreaterThanOrEqualToStart))
}

func (splitter *Splitter) constructQueryPart(start, end tuple) *querypb.QuerySplit {
	result := &querypb.QuerySplit{}
	result.Query = &querypb.BoundQuery{
		BindVariables: cloneBindVariables(splitter.splitParams.bindVariables),
	}
	// TODO(erez): Fill result.RowCount
	if start != nil {
		populateBoundaryBindVariables(
			start, splitter.startBindVariableNames, result.Query.BindVariables)
	}
	if end != nil {
		populateBoundaryBindVariables(
			end, splitter.endBindVariableNames, result.Query.BindVariables)
	}
	switch {
	case start == nil && end == nil:
		// If there's no upper or lower bound then just use the original query as the query part.
		// This can happen if the boundaries list is empty.
		result.Query.Sql = splitter.splitParams.sql
	case start == nil && end != nil:
		result.Query.Sql = splitter.firstQueryPartSQL
	case start != nil && end != nil:
		result.Query.Sql = splitter.middleQueryPartSQL
	case start != nil && end == nil:
		result.Query.Sql = splitter.lastQueryPartSQL
	}
	return result
}

//
// Below are utility functions called by the Splitter methods above.
//

// populateBoundaryBindVariables populates 'resultBindVariables' with new bind variables.
// The ith bind-variable has name bindVariableNames[i] and value inputTuple[i].
// The function panics if a bind variable name already exists in 'resultBindVariables'.
func populateBoundaryBindVariables(
	inputTuple tuple, bindVariableNames []string, resultBindVariables map[string]*querypb.BindVariable) {
	if len(inputTuple) != len(bindVariableNames) {
		panic(fmt.Sprintf("len(inputTuple) != len(bindVariableNames): %v != %v",
			len(inputTuple), len(bindVariableNames)))
	}
	for i := range inputTuple {
		populateNewBindVariable(bindVariableNames[i], sqltypes.ValueBindVariable(inputTuple[i]), resultBindVariables)
	}
}

func convertColumnsToExpr(columns []*schema.TableColumn) []sqlparser.Expr {
	valExprs := make([]sqlparser.Expr, 0, len(columns))
	for _, column := range columns {
		valExprs = append(valExprs, &sqlparser.ColName{Name: column.Name})
	}
	return valExprs
}

func convertBindVariableNamesToExpr(bindVariableNames []string) []sqlparser.Expr {
	valExprs := make([]sqlparser.Expr, 0, len(bindVariableNames))
	for _, bindVariableName := range bindVariableNames {
		valExprs = append(valExprs, sqlparser.NewValArg([]byte([]byte(":"+bindVariableName))))
	}
	return valExprs
}

// constructTupleInequality constructs a boolean expression representing a tuple lexicographical
// comparison using only scalar comparisons.
//
// MySQL does support tuple-inequalities ((a,b) <= (c,d) and interpretes them using the
// lexicographical ordering of tuples. However, it does not optimize queries with such inequalities
// well. Specifically, it does not recognize that a query with such inequalities, that involve only
// the columns of an index, can be done as an index scan. Rather, it resorts to a full-table scan.
// Thus, we convert such tuple inequalties to an expression involving only scalar inequalties.
//
// The expression returned by this function represents the lexicographical comparisons:
// lhsTuple <= rhsTuple, if strict is false, or lhsTuple < rhsTuple, otherwise.
// For example: if lhsTuple = (l1, l2) and rhsTuple = (r1, r2) then the returned expression is
// (l1 < r1) or ((l1 = r1) and (l2 <= r2)) if strict is false,
// and
// (l1 < r1) or ((l1 = r1) and (l2 < r2)), otherwise.
func constructTupleInequality(
	lhsTuple []sqlparser.Expr, rhsTuple []sqlparser.Expr, strict bool) sqlparser.Expr {
	if len(lhsTuple) != len(rhsTuple) {
		panic(fmt.Sprintf("len(lhsTuple)!=len(rhsTuple): %v!=%v", len(lhsTuple), len(rhsTuple)))
	}
	if len(lhsTuple) == 0 {
		panic("len(lhsTuple)==0")
	}
	// The actual work of this function is done in a "nested" function
	// 'constructTupleInequalityUnchecked' which does not further check the lengths.
	// It's a recursive function and so we must define the 'constructTupleInequalityUnchecked'
	// variable beforehand.
	var constructTupleInequalityUnchecked func(
		lhsTuple []sqlparser.Expr, rhsTuple []sqlparser.Expr, strict bool) sqlparser.Expr
	constructTupleInequalityUnchecked = func(
		lhsTuple []sqlparser.Expr, rhsTuple []sqlparser.Expr, strict bool) sqlparser.Expr {
		if len(lhsTuple) == 1 {
			op := sqlparser.LessEqualStr
			if strict {
				op = sqlparser.LessThanStr
			}
			return &sqlparser.ComparisonExpr{
				Operator: op,
				Left:     lhsTuple[0],
				Right:    rhsTuple[0],
			}
		}
		restOfTupleInequality := constructTupleInequalityUnchecked(lhsTuple[1:], rhsTuple[1:], strict)
		if len(lhsTuple[1:]) > 1 {
			// A non-scalar inequality needs to be parenthesized since we combine it below with
			// other expressions.
			restOfTupleInequality = &sqlparser.ParenExpr{
				Expr: restOfTupleInequality,
			}
		}
		// Return:
		// lhsTuple[0] < rhsTuple[0] OR
		// ( lhsTuple[0] = rhsTuple[0] AND restOfTupleInequality)
		return &sqlparser.OrExpr{
			Left: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LessThanStr,
				Left:     lhsTuple[0],
				Right:    rhsTuple[0],
			},
			Right: &sqlparser.ParenExpr{
				Expr: &sqlparser.AndExpr{
					Left: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     lhsTuple[0],
						Right:    rhsTuple[0],
					},
					Right: restOfTupleInequality,
				},
			},
		}
	}

	return constructTupleInequalityUnchecked(lhsTuple, rhsTuple, strict)
}

// queryWithAdditionalWhere returns a copy of the given SELECT query with 'addedWhere' ANDed with
// the query's WHERE clause. If the query does not already have a WHERE clause, 'addedWhere'
// becomes the query's WHERE clause.
func queryWithAdditionalWhere(
	selectAST *sqlparser.Select, addedWhere sqlparser.Expr) *sqlparser.Select {
	result := *selectAST // Create a shallow-copy of 'selectAST'
	addAndTermToWhereClause(&result, addedWhere)
	return &result
}

const (
	startBindVariablePrefix = "_splitquery_start_"
	endBindVariablePrefix   = "_splitquery_end_"
)
