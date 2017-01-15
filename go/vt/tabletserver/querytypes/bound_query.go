// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package querytypes defines internal types used in the APIs to deal
// with queries.
package querytypes

import (
	"fmt"

	"github.com/gitql/vitess/go/bytes2"
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
	buf := bytes2.NewChunkedWriter(1024)
	fmt.Fprintf(buf, "Sql: %#v, BindVars: {", sql)
	for k, v := range bindVariables {
		switch val := v.(type) {
		case []byte:
			fmt.Fprintf(buf, "%s: %#v, ", k, slimit(string(val)))
		case string:
			fmt.Fprintf(buf, "%s: %#v, ", k, slimit(val))
		default:
			fmt.Fprintf(buf, "%s: %v, ", k, v)
		}
	}
	fmt.Fprintf(buf, "}")
	return string(buf.Bytes())
}

func slimit(s string) string {
	l := len(s)
	if l > 256 {
		l = 256
	}
	return s[:l]
}
