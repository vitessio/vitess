/*
Copyright 2017 Google Inc.

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

package querytypes

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
		BindVariablesEqual(q.BindVariables, q2.BindVariables) &&
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
