// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"golang.org/x/net/context"
)

// Select is a Primitive that extracts columns from its input, evaluates
// expressions, and renames columns.
//
// Select is not executable (it does not implement Plan).
type Select struct {
	// primitiveNode maintains the parent and children links.
	//
	// For Select, the children are as follows:
	//   [0]  -> FROM clause
	//   [1:] -> Subqueries
	primitiveNode

	// selectExprs is a list of column expressions.
	//
	// Since Select is not executable, these are only stored to track the mapping
	// between the Select primitive's inputs and outputs (used while optimizing),
	// and to generate a query if the Select is pushed into a Route.
	selectExprs sqlparser.SelectExprs
}

// Join is a Primitive that joins two result sets on a boolean expression.
//
// Join is not executable (it does not implement Plan) because we don't support
// evaluating arbitrary boolean expressions in VTGate. If possible, the
// optimizer will replace Join with something we can actually execute, like
// SelectJoinEqual.
type Join struct {
	// primitiveNode maintains the parent and children links.
	//
	// For Join, the children are as follows:
	//   [0] -> Left hand side
	//   [1] -> Right hand side
	primitiveNode

	// IsLeft is true if this is a LEFT OUTER JOIN. Otherwise it's an INNER JOIN.
	IsLeft bool
	// On is the boolean expression on which to join.
	On sqlparser.BoolExpr
}

// SelectJoinEqual is a Join in which the ON clause consists only of pairs of
// columns on each side that must be equal. It also can restrict the set of
// columns returned in the result, although it can't evaluate expressions.
//
// SelectJoinEqual is executable (it implements Plan).
type SelectJoinEqual struct {
	// primitiveNode maintains the parent and children links.
	//
	// For Join, the children are as follows:
	//   [0] -> Left hand side
	//   [1] -> Right hand side
	primitiveNode

	// IsLeft is true if this is a LEFT OUTER JOIN. Otherwise it's an INNER JOIN.
	IsLeft bool
	// ColPairs are the pairs of columns that must be equal.
	ColPairs [][2]*sqlparser.ColName
	// ResultCols are the columns from either side to return in the result.
	ResultCols []*sqlparser.ColName
}

// Execute performs a non-streaming join operation. It fetches rows from the LHS,
// builds the necessary join variables for each fetched row, and invokes the RHS.
// It then joins the left and right results based on the requested columns.
// If the LHS returned no rows and wantFields is set, it performs a field
// query to fetch the field info.
func (join *SelectJoinEqual) Execute(ctx context.Context, req *Request) (*sqltypes.Result, error) {
	left, right := join.children[0].(Plan), join.children[1].(Plan)

	lresult, err := left.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	// Save a copy of JoinVars and restore it when we're done.
	saved := copyBindVars(req.JoinVars)
	defer func() { req.JoinVars = saved }()

	result := &sqltypes.Result{}

	if len(lresult.Rows) == 0 {
		for _, pair := range join.ColPairs {
			// setJoinVar is an imaginary function that sets the bindVar according to
			// a *sqlparser.ColName.
			req.setJoinVar(pair[0], nil)
		}
		rresult, err := right.GetFields(ctx, req)
		if err != nil {
			return nil, err
		}
		result.Fields = joinFields(lresult.Fields, rresult.Fields, join.ResultCols)
		return result, nil
	}

	for _, lrow := range lresult.Rows {
		for _, pair := range join.ColPairs {
			// getCol is an imaginary function that gets the value of a column
			// according to a *sqlparser.ColName.
			req.setJoinVar(pair[0], lrow.getCol(pair[0], lresult.Fields))
		}
		rresult, err := right.Execute(ctx, req)
		if err != nil {
			return nil, err
		}
		if result.Fields == nil {
			result.Fields = joinFields(lresult.Fields, rresult.Fields, join.ResultCols)
		}
		for _, rrow := range rresult.Rows {
			result.Rows = append(result.Rows, joinRows(lrow, rrow, join.ResultCols))
		}
		if join.IsLeft && len(rresult.Rows) == 0 {
			result.Rows = append(result.Rows, joinRows(lrow, nil, join.ResultCols))
			result.RowsAffected++
		} else {
			result.RowsAffected += uint64(len(rresult.Rows))
		}
	}

	return result, nil
}
