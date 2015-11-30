// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	"github.com/youtube/vitess/go/bytes2"
)

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

// BoundQuery is one query in a QueryList.
type BoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

//go:generate bsongen -file $GOFILE -type BoundQuery -o bound_query_bson.go

// QuerySplit represents a split of SplitQueryRequest.Query. RowCount is only
// approximate.
type QuerySplit struct {
	Sql           string
	BindVariables map[string]interface{}
	RowCount      int64
}
