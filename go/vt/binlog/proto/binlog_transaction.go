// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
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

// BinlogTransaction represents one transaction as read from
// the binlog.
type BinlogTransaction struct {
	Statements []Statement
	GroupId    int64
}

// Statement represents one statement as read from the binlog.
type Statement struct {
	Category int
	Sql      []byte
}

func (blt *BinlogTransaction) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)
	MarshalStatementsBson(buf, "Statements", blt.Statements)
	bson.EncodeInt64(buf, "GroupId", blt.GroupId)
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func MarshalStatementsBson(buf *bytes2.ChunkedWriter, key string, statements []Statement) {
	bson.EncodePrefix(buf, bson.Array, key)
	lenWriter := bson.NewLenWriter(buf)
	for i, v := range statements {
		v.MarshalBson(buf, bson.Itoa(i))
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (blt *BinlogTransaction) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Statements":
			blt.Statements = UnmarshalStatementsBson(buf, kind)
		case "GroupId":
			blt.GroupId = bson.DecodeInt64(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func UnmarshalStatementsBson(buf *bytes.Buffer, kind byte) []Statement {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for BinlogTransaction.Statements", kind))
	}

	bson.Next(buf, 4)
	statements := make([]Statement, 0, 8)
	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for Query.Field", kind))
		}
		bson.SkipIndex(buf)
		var statement Statement
		statement.UnmarshalBson(buf, kind)
		statements = append(statements, statement)
		kind = bson.NextByte(buf)
	}
	return statements
}

func (stmt *Statement) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)
	bson.EncodeInt(buf, "Category", stmt.Category)
	bson.EncodeBinary(buf, "Sql", stmt.Sql)
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (stmt *Statement) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Category":
			stmt.Category = int(bson.DecodeInt64(buf, kind))
		case "Sql":
			stmt.Sql = bson.DecodeBinary(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}
