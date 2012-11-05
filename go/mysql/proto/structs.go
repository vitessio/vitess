// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"code.google.com/p/vitess/go/sqltypes"
)

type Field struct {
	Name string
	Type int64
}

type QueryResult struct {
	Fields       []Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
}
