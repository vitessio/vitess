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

type QueryList struct {
	Queries       []BoundQuery
	SessionId     int64
	TransactionId int64
}

type QueryResultList struct {
	List []mproto.QueryResult
}

type Session struct {
	SessionId     int64
	TransactionId int64
}

type TransactionInfo struct {
	TransactionId int64
}

type DmlType struct {
	Table string
	Keys  []string
}

type DDLInvalidate struct {
	DDL string
}
