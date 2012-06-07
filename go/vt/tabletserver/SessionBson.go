// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
)

type Session struct {
	TransactionId int64
	ConnectionId  int64
	SessionId     int64
}

func (self *Session) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Long, "TransactionId")
	bson.EncodeUint64(buf, uint64(self.TransactionId))

	bson.EncodePrefix(buf, bson.Long, "ConnectionId")
	bson.EncodeUint64(buf, uint64(self.ConnectionId))

	bson.EncodePrefix(buf, bson.Long, "SessionId")
	bson.EncodeUint64(buf, uint64(self.SessionId))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *Session) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "TransactionId":
			self.TransactionId = bson.DecodeInt64(buf, kind)
		case "ConnectionId":
			self.ConnectionId = bson.DecodeInt64(buf, kind)
		case "SessionId":
			self.SessionId = bson.DecodeInt64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}
