// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
