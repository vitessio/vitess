// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	rpc "code.google.com/p/vitess/go/rpcplus"
)

type RequestBson struct {
	*rpc.Request
}

func (self *RequestBson) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "ServiceMethod")
	bson.EncodeString(buf, self.ServiceMethod)

	bson.EncodePrefix(buf, bson.Long, "Seq")
	bson.EncodeUint64(buf, uint64(self.Seq))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *RequestBson) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "ServiceMethod":
			self.ServiceMethod = bson.DecodeString(buf, kind)
		case "Seq":
			self.Seq = bson.DecodeUint64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

type ResponseBson struct {
	*rpc.Response
}

func (self *ResponseBson) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "ServiceMethod")
	bson.EncodeString(buf, self.ServiceMethod)

	bson.EncodePrefix(buf, bson.Long, "Seq")
	bson.EncodeUint64(buf, uint64(self.Seq))

	bson.EncodePrefix(buf, bson.Binary, "Error")
	bson.EncodeString(buf, self.Error)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (self *ResponseBson) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "ServiceMethod":
			self.ServiceMethod = bson.DecodeString(buf, kind)
		case "Seq":
			self.Seq = bson.DecodeUint64(buf, kind)
		case "Error":
			self.Error = bson.DecodeString(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}
