// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package jsonrpc provides wrappers for json rpc communication
package jsonrpc

import (
	"crypto/tls"
	"time"

	rpc "github.com/youtube/vitess/go/rpcplus"
	oldjson "github.com/youtube/vitess/go/rpcplus/jsonrpc"
	"github.com/youtube/vitess/go/rpcwrap"
)

// DialHTTP dials a json rpc HTTP endpoint with optional TLS config
func DialHTTP(network, address string, connectTimeout time.Duration, config *tls.Config) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, "json", oldjson.NewClientCodec, connectTimeout, config)
}

// ServeRPC serves a json rpc endpoint using default server
func ServeRPC() {
	rpcwrap.ServeRPC("json", oldjson.NewServerCodec)
}

// ServeAuthRPC serves a json rpc endpoint using authentication enabled default
// server
func ServeAuthRPC() {
	rpcwrap.ServeAuthRPC("json", oldjson.NewServerCodec)
}
