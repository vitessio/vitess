// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package querytypes

import "github.com/youtube/vitess/go/sqltypes"

// This file defines QuerySplit

// QuerySplit represents a split of a query, used for MapReduce purposes.
type QuerySplit struct {
	// Sql is the query
	Sql string

	// BindVariables is the map of bind variables for the query
	BindVariables map[string]interface{}

	// RowCount is the approximate number of rows this query will return
	RowCount int64
}

// Equal compares two QuerySplit objects.
func (q *QuerySplit) Equal(q2 *QuerySplit) bool {
	return q.Sql == q2.Sql &&
		sqltypes.BindVariablesEqual(q.BindVariables, q2.BindVariables) &&
		q.RowCount == q2.RowCount
}

// QuerySplitsEqual compares two slices of QuerySplit objects.
func QuerySplitsEqual(x, y []QuerySplit) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !x[i].Equal(&y[i]) {
			return false
		}
	}
	return true
}
