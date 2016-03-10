package splitquery

import (
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
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
func NewSplitter(
	splitParams *SplitParams,
	algorithm SplitAlgorithmInterface) *Splitter {
	var splitter Splitter
	splitter.splitParams = splitParams
	splitter.algorithm = algorithm
	splitter.splitParams = splitParams
	splitter.startBindVariableNames = make([]string, 0, len(splitter.splitParams.splitColumns))
	splitter.endBindVariableNames = make([]string, 0, len(splitter.splitParams.splitColumns))
	for _, splitColumn := range splitter.splitParams.splitColumns {
		splitter.startBindVariableNames = append(
			splitter.startBindVariableNames, startBindVariablePrefix+splitColumn)
		splitter.endBindVariableNames = append(
			splitter.endBindVariableNames, endBindVariablePrefix+splitColumn)
	}
	splitter.initQueryPartSQLs()
	return &splitter
}

// Split does the actual work of splitting the query.
// It returns a slice of querytypes.QuerySplit objects representing
// the query parts.
func (splitter *Splitter) Split() ([]querytypes.QuerySplit, error) {
	var boundaries []tuple
	var err error
	boundaries, err = splitter.algorithm.generateBoundaries()
	if err != nil {
		return nil, err
	}
	boundaries = append(boundaries, nil)
	splits := []querytypes.QuerySplit{}
	var start tuple
	for _, end := range boundaries {
		splits = append(splits, *splitter.constructQueryPart(start, end))
		start = end
	}
	return splits, nil
}

// initQueryPartSQLs initializes the firstQueryPartSQL, middleQueryPartSQL and lastQueryPartSQL
// fields.
func (splitter *Splitter) initQueryPartSQLs() {
	splitColumns := convertColumnNamesToValExpr(splitter.splitParams.splitColumns)
	startBindVariables := convertBindVariableNamesToValExpr(splitter.startBindVariableNames)
	endBindVariables := convertBindVariableNamesToValExpr(splitter.endBindVariableNames)
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
				Left:  &sqlparser.ParenBoolExpr{Expr: splitColsGreaterThanOrEqualToStart},
				Right: &sqlparser.ParenBoolExpr{Expr: splitColsLessThanEnd},
			}))
	splitter.lastQueryPartSQL = sqlparser.String(
		queryWithAdditionalWhere(splitter.splitParams.selectAST, splitColsGreaterThanOrEqualToStart))
}

func (splitter *Splitter) constructQueryPart(start, end tuple) *querytypes.QuerySplit {
	result := &querytypes.QuerySplit{}
	result.BindVariables = cloneBindVariables(splitter.splitParams.bindVariables)
	// TODO(erez): Fill result.RowCount
	if start != nil {
		populateBoundaryBindVariables(
			start, splitter.startBindVariableNames, result.BindVariables)
	}
	if end != nil {
		populateBoundaryBindVariables(
			end, splitter.endBindVariableNames, result.BindVariables)
	}
	switch {
	case start == nil && end == nil:
		// If there's no upper or lower bound then just use the original query as the query part.
		// This can happen if the boundaries list is empty.
		result.Sql = splitter.splitParams.sql
	case start == nil && end != nil:
		result.Sql = splitter.firstQueryPartSQL
	case start != nil && end != nil:
		result.Sql = splitter.middleQueryPartSQL
	case start != nil && end == nil:
		result.Sql = splitter.lastQueryPartSQL
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
	inputTuple tuple, bindVariableNames []string, resultBindVariables map[string]interface{}) {
	assertEqual(len(inputTuple), len(bindVariableNames))
	for i := range inputTuple {
		populateNewBindVariable(bindVariableNames[i], inputTuple[i].ToNative(), resultBindVariables)
	}
}

func convertColumnNamesToValExpr(colNames []string) []sqlparser.ValExpr {
	valExprs := make([]sqlparser.ValExpr, 0, len(colNames))
	for _, colName := range colNames {
		valExprs = append(valExprs, &sqlparser.ColName{Name: sqlparser.SQLName(colName)})
	}
	return valExprs
}

func convertBindVariableNamesToValExpr(bindVariableNames []string) []sqlparser.ValExpr {
	valExprs := make([]sqlparser.ValExpr, 0, len(bindVariableNames))
	for _, bindVariableName := range bindVariableNames {
		valExprs = append(valExprs, sqlparser.ValArg([]byte(":"+bindVariableName)))
	}
	return valExprs
}

// TODO(erez): Explain that mysql doesn't support tuple-inequalities well so we need to expand
// to use scala inequalities.
// constructTupleInequality constructs a boolean expression representing a tuple lexicographical
// comparison using only scalar comparisons.
// The expression returned repersents the lexicographical comparisons: lhsTuple <= rhsTuple,
// if strict is fale, or lhsTuple < rhsTuple, otherwise.
// For example: if lhsTuple = (l1, l2) and rhsTuple = (r1, r2) then the returned expression is
// (l1 < r1) or ((l1 = r1) and (l2 <= r2)) if strict is false,
// and
// (l1 < r1) or ((l1 = r1) and (l2 < r2)), otherwise.
func constructTupleInequality(
	lhsTuple []sqlparser.ValExpr, rhsTuple []sqlparser.ValExpr, strict bool) sqlparser.BoolExpr {
	assertEqual(len(lhsTuple), len(rhsTuple))
	assertGreaterOrEqual(len(lhsTuple), 1)
	return constructTupleInequalityUnchecked(lhsTuple, rhsTuple, strict)
}
func constructTupleInequalityUnchecked(
	lhsTuple []sqlparser.ValExpr, rhsTuple []sqlparser.ValExpr, strict bool) sqlparser.BoolExpr {
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
		// A non-scalar inequality need to be parenthesized since we combine it below with
		// other expressions.
		restOfTupleInequality = &sqlparser.ParenBoolExpr{
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
		Right: &sqlparser.ParenBoolExpr{
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

// queryWithAdditionalWhere returns a copy of the given SELECT query with 'addedWhere' ANDed with
// the query's WHERE clause. If the query does not already have a WHERE clause, 'addedWhere'
// becomes the query's WHERE clause.
func queryWithAdditionalWhere(
	selectAST *sqlparser.Select, addedWhere sqlparser.BoolExpr) *sqlparser.Select {
	result := *selectAST // Create a shallow-copy of 'selectAST'
	addAndTermToWhereClause(&result, addedWhere)
	return &result
}

const (
	startBindVariablePrefix = "_splitquery_start_"
	endBindVariablePrefix   = "_splitquery_end_"
)
