// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
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
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

type BoundQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

type QueryList struct {
	Queries       []BoundQuery
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

type QueryResultList struct {
	List []mproto.QueryResult
}

type Session struct {
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

type ConnectionInfo struct {
	ConnectionId int64
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
