// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

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

var BL_CATEGORY_NAMES = map[int]string{
	BL_UNRECOGNIZED: "BL_UNRECOGNIZED",
	BL_BEGIN:        "BL_BEGIN",
	BL_COMMIT:       "BL_COMMIT",
	BL_ROLLBACK:     "BL_ROLLBACK",
	BL_DML:          "BL_DML",
	BL_DDL:          "BL_DDL",
	BL_SET:          "BL_SET",
}

// BinlogTransaction represents one transaction as read from
// the binlog. Timestamp is set if the first statement was
// something like 'SET TIMESTAMP=...'
type BinlogTransaction struct {
	Statements []Statement
	Timestamp  int64
	GTIDField  myproto.GTIDField
}

// Statement represents one statement as read from the binlog.
type Statement struct {
	Category int
	Charset  *mproto.Charset
	Sql      []byte
}

// String pretty-prints a statement.
func (s Statement) String() string {
	if cat, ok := BL_CATEGORY_NAMES[s.Category]; ok {
		return fmt.Sprintf("{Category: %v, Charset: %v, Sql: %q}", cat, s.Charset, string(s.Sql))
	}
	return fmt.Sprintf("{Category: %v, Charset: %v, Sql: %q}", s.Category, s.Charset, string(s.Sql))
}
