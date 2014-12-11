// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc

import (
	"crypto/tls"
	"time"

	rpc "github.com/henryanand/vitess/go/rpcplus"
	oldjson "github.com/henryanand/vitess/go/rpcplus/jsonrpc"
	"github.com/henryanand/vitess/go/rpcwrap"
)

func DialHTTP(network, address string, connectTimeout time.Duration, config *tls.Config) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, "json", oldjson.NewClientCodec, connectTimeout, config)
}

func ServeRPC() {
	rpcwrap.ServeRPC("json", oldjson.NewServerCodec)
}

func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC("json", oldjson.NewServerCodec)
}
