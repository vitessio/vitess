// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// StreamEvent represents one event for the update stream.
type StreamEvent struct {
	// Category can be "DML", "DDL", "ERR" or "POS"
	Category string

	// DML
	TableName  string
	PKColNames []string
	PKValues   [][]interface{}

	// DDL or ERR
	Sql string

	// Timestamp is set for DML, DDL or ERR
	Timestamp int64

	// POS
	GroupId int64
}
