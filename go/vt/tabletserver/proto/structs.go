// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	"github.com/youtube/vitess/go/bytes2"
	mproto "github.com/youtube/vitess/go/mysql/proto"
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

// SplitQueryRequest represents a request to split a Query into queries that
// each return a subset of the original query.
// SplitColumn: preferred column to split. Server will pick a random PK column
//              if this field is empty or returns an error if this field is not
//              empty but not found in schema info or not be indexed.
type SplitQueryRequest struct {
	Query       BoundQuery
	SplitColumn string
	SplitCount  int
	SessionID   int64
}

// QuerySplit represents a split of SplitQueryRequest.Query. RowCount is only
// approximate.
type QuerySplit struct {
	Query    BoundQuery
	RowCount int64
}

// SplitQueryResult represents the result of a SplitQueryRequest
type SplitQueryResult struct {
	Queries []QuerySplit
	Err     *mproto.RPCError
}

// CallerID is the BSON implementation of the proto3 vtrpc.CallerID
type CallerID struct {
	Principal    string
	Component    string
	Subcomponent string
}

//go:generate bsongen -file $GOFILE -type CallerID -o callerid_bson.go

// VTGateCallerID is the BSON implementation of the proto3 query.VTGateCallerID
type VTGateCallerID struct {
	Username string
}
