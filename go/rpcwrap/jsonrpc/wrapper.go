// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc

import (
	"code.google.com/p/vitess/go/rpcwrap"
	"net/rpc"
	oldjson "net/rpc/jsonrpc"
)

func DialHTTP(network, address string) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, "json", oldjson.NewClientCodec)
}

func ServeRPC() {
	rpcwrap.ServeRPC("json", oldjson.NewServerCodec)
}

func ServeHTTP() {
	rpcwrap.ServeHTTP("json", oldjson.NewServerCodec)
}
