// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package querytypes defines internal types used in the APIs to deal
// with queries.
package querytypes

import (
	"bytes"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sqldb"
)

// This file defines the BoundQuery type.
//
// In our internal code, the following rules are true:
// - a SQL query is always represented as a string.
// - bind variables for a SQL query are always represented as
//   map[string]interface{}.
// - the RPC layer converts from proto3 (or any other encoding) to these types.

// BoundQuery is one query in a QueryList.
// We only use it in arrays. For a single query, we just use Sql and
// BindVariables directly.
type BoundQuery struct {
	// Sql is the query
	Sql string

	// BindVariables is the map of bind variables for the query
	BindVariables map[string]interface{}
}

// QueryAsString prints a readable version of query+bind variables,
// and also truncates data if it's too long
func QueryAsString(sql string, bindVariables map[string]interface{}) string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "Sql: %q, BindVars: {", sqldb.TruncateForError(sql))
	for k, v := range bindVariables {
		var valString string;
		switch val := v.(type) {
		case []byte:
			valString = string(val);
		case string:
			valString = val;
		default:
			valString = fmt.Sprintf("%v", v);
		}

		fmt.Fprintf(buf, "%s: %q", k, sqldb.TruncateForError(valString));
	}
	fmt.Fprintf(buf, "}")
	return string(buf.Bytes())
}

func slimit(s string, max int) string {
	if l := len(s); l > max {
		return s[:max]
	}
	return s
}

// BoundQueriesEqual compares two slices of BoundQuery objects.
func BoundQueriesEqual(x, y []BoundQuery) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !BoundQueryEqual(&x[i], &y[i]) {
			return false
		}
	}
	return true
}

// BoundQueryEqual compares two BoundQuery objects.
func BoundQueryEqual(x, y *BoundQuery) bool {
	return x.Sql == y.Sql &&
		sqltypes.BindVariablesEqual(x.BindVariables, y.BindVariables)
}
