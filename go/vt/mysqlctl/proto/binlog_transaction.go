// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import ()

// Valid statement types in the binlogs.
const (
	BL_UNRECOGNIZED = iota
	BL_BEGIN
	BL_COMMIT
	BL_ROLLBACK
	BL_DML
	BL_DDL
	BL_SET
)

// BinlogTransaction represents one transaction as read from
// the binlog.
type BinlogTransaction struct {
	Statements []Statement
	GroupId    string
}

// Statement represents one statement as read from the binlog.
type Statement struct {
	Category int
	Sql      []byte
}
