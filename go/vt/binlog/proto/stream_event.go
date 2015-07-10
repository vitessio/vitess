// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// StreamEvent represents one event for the update stream.
type StreamEvent struct {
	// Category can be "DML", "DDL", "ERR" or "POS"
	Category string

	// DML
	TableName        string
	PrimaryKeyFields []mproto.Field
	PrimaryKeyValues [][]sqltypes.Value

	// DDL or ERR
	Sql string

	// Timestamp is set for DML, DDL or ERR
	Timestamp int64

	// POS
	GTIDField myproto.GTIDField
}

//go:generate bsongen -file $GOFILE -type StreamEvent -o stream_event_bson.go
