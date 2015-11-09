// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/proto/query"
)

// StreamEvent represents one event for the update stream.
type StreamEvent struct {
	// Category can be "DML", "DDL", "ERR" or "POS"
	Category string

	// TableName, PrimaryKeyFields and PrimaryKeyValues are set for DML
	TableName        string
	PrimaryKeyFields []*query.Field
	PrimaryKeyValues [][]sqltypes.Value

	// Sql is set for DDL or ERR
	Sql string

	// Timestamp is set for DML, DDL or ERR
	Timestamp int64

	// TransactionID is set for POS
	TransactionID string
}
