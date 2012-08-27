// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc

import (
	rpc "code.google.com/p/vitess/go/rpcplus"
	oldjson "code.google.com/p/vitess/go/rpcplus/jsonrpc"
	"code.google.com/p/vitess/go/rpcwrap"
)

func DialHTTP(network, address string) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, "json", oldjson.NewClientCodec)
}

func ServeRPC() {
	rpcwrap.ServeRPC("json", oldjson.NewServerCodec)
}

func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC("json", oldjson.NewServerCodec)
}

func ServeHTTP() {
	rpcwrap.ServeHTTP("json", oldjson.NewServerCodec)
}
