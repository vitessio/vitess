/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package bsonrpc

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"net/rpc"
)

type RequestBson struct {
	*rpc.Request
}

func (self *RequestBson) MarshalBson(buf *bytes.Buffer) {
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

func (self *ResponseBson) MarshalBson(buf *bytes.Buffer) {
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
