// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	"github.com/youtube/vitess/go/bytes2"
	mproto "github.com/youtube/vitess/go/mysql/proto"
)

type SessionParams struct {
	Keyspace string
	Shard    string
}

type SessionInfo struct {
	SessionId int64
}

type Query struct {
	Sql           string
	BindVariables map[string]interface{}
	SessionId     int64
	TransactionId int64
}

//go:generate bsongen -file $GOFILE -type Query -o query_bson.go

// String prints a readable version of Query, and also truncates
// data if it's too long
func (query *Query) String() string {
	buf := bytes2.NewChunkedWriter(1024)
	fmt.Fprintf(buf, "Sql: %#v, BindVars: {", query.Sql)
	for k, v := range query.BindVariables {
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

type BoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

//go:generate bsongen -file $GOFILE -type BoundQuery -o bound_query_bson.go

type QueryList struct {
	Queries       []BoundQuery
	SessionId     int64
	TransactionId int64
}

//go:generate bsongen -file $GOFILE -type QueryList -o query_list_bson.go

type QueryResultList struct {
	List []mproto.QueryResult
}

//go:generate bsongen -file $GOFILE -type QueryResultList -o query_result_list_bson.go

type Session struct {
	SessionId     int64
	TransactionId int64
}

//go:generate bsongen -file $GOFILE -type Session -o session_bson.go

type TransactionInfo struct {
	TransactionId int64
}

// SplitQueryRequest represents a request to split a Query into queries that
// each return a subset of the original query.
type SplitQueryRequest struct {
	Query      BoundQuery
	SplitCount int
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
}
