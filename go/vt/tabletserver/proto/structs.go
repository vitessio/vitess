// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	"github.com/youtube/vitess/go/bytes2"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

// SessionParams is passed to GetSessionId. The server will
// double-check the keyspace and shard are what the tablet is serving.
type SessionParams struct {
	Keyspace string
	Shard    string
}

// SessionInfo is returned by GetSessionId. Use the provided
// session_id in the Session object for any subsequent call.
type SessionInfo struct {
	SessionId int64
	Err       *mproto.RPCError
}

//go:generate bsongen -file $GOFILE -type SessionInfo -o session_info_bson.go

// Query is the payload to Execute.
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

// BoundQuery is one query in a QueryList.
type BoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

//go:generate bsongen -file $GOFILE -type BoundQuery -o bound_query_bson.go

// QueryList is the payload to ExecuteBatch.
type QueryList struct {
	Queries       []BoundQuery
	SessionId     int64
	AsTransaction bool
	TransactionId int64
}

//go:generate bsongen -file $GOFILE -type QueryList -o query_list_bson.go

// QueryResultList is the return type for ExecuteBatch.
type QueryResultList struct {
	List []sqltypes.Result
	Err  *mproto.RPCError
}

//go:generate bsongen -file $GOFILE -type QueryResultList -o query_result_list_bson.go

// Session is passed to all calls.
type Session struct {
	SessionId     int64
	TransactionId int64
}

//go:generate bsongen -file $GOFILE -type Session -o session_bson.go

// TransactionInfo is returned by Begin. Use the provided
// transaction_id in the Session object for any subsequent call to be inside
// the transaction.
type TransactionInfo struct {
	TransactionId int64
	Err           *mproto.RPCError
}

//go:generate bsongen -file $GOFILE -type TransactionInfo -o transaction_info_bson.go

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
