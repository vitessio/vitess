// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	rpc "github.com/youtube/vitess/go/rpcplus"
)

// RequestBson provides bson rpc request parameters
type RequestBson struct {
	*rpc.Request
}

// MarshalBson marshals request to the given writer with optional prefix
func (req *RequestBson) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "ServiceMethod", req.ServiceMethod)
	bson.EncodeUint64(buf, "Seq", req.Seq)

	lenWriter.Close()
}

// UnmarshalBson unmarshals request to the given byte buffer as verifying the
// kind
func (req *RequestBson) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "ServiceMethod":
			req.ServiceMethod = bson.DecodeString(buf, kind)
		case "Seq":
			req.Seq = bson.DecodeUint64(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

// ResponseBson provides bson rpc request parameters
type ResponseBson struct {
	*rpc.Response
}

// MarshalBson marshals response to the given writer with optional prefix
func (resp *ResponseBson) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "ServiceMethod", resp.ServiceMethod)
	bson.EncodeUint64(buf, "Seq", resp.Seq)
	bson.EncodeString(buf, "Error", resp.Error)

	lenWriter.Close()
}

// UnmarshalBson unmarshals response to the given byte buffer as verifying the
// kind
func (resp *ResponseBson) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "ServiceMethod":
			resp.ServiceMethod = bson.DecodeString(buf, kind)
		case "Seq":
			resp.Seq = bson.DecodeUint64(buf, kind)
		case "Error":
			resp.Error = bson.DecodeString(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}
