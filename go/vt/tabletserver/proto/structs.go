// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "code.google.com/p/vitess/go/mysql/proto"
)

type SessionParams struct {
	DbName string
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

type QueryList struct {
	List []Query
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

type DmlType struct {
	Table string
	Keys  []interface{}
}

type CacheInvalidate struct {
	Position string
	Dmls     []DmlType
}

type DDLInvalidate struct {
	Position string
	DDL      string
}
