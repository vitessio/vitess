// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rpc contains RPC-related structs shared between many components.
package rpc

import (
	"bytes"

	"github.com/henryanand/vitess/go/bson"
)

// Unused is a placeholder type for args and reply values that aren't used.
// For example, a server might declare a method without args or reply:
//
//   func (s *Server) SomeMethod(ctx *Context, args *rpc.Unused, reply *rpc.Unused)
//
// The client would then call it like this:
//
//   client.rpcCall("SomeMethod", &rpc.Unused{}, &rpc.Unused{}, waitTime)
//
// Using Unused ensures that, when the server doesn't care about a value, it
// will silently ignore any value that is passed. With previous placeholder
// values, such as strings, certain values might result in panics in the BSON
// library.
//
// If a method declared with Unused as its args or reply is changed to accept
// a real struct, old clients will encode an empty struct, which will be
// silently ignored by the new servers. New clients talking to old servers will
// send the real struct, which will be silently ignored. This allows args or
// replies to be added to existing methods that previously did not use them.
type Unused struct{}

// UnmarshalBson skips over an encoded Unused in a backward-compatible way.
// Previous versions of Unused would BSON-encode a naked string value, which
// bson.EncodeSimple() would encode as a document with a field named
// bson.MAGICTAG. Here we skip over any document or sub-document without looking
// for the special MAGICTAG field name.
func (u *Unused) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for Unused", kind))
	}
	bson.Next(buf, 4)

	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		bson.ReadCString(buf)
		bson.Skip(buf, kind)
	}
}
