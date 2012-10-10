// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	rpc "code.google.com/p/vitess/go/rpcplus"
)

// defines the RPC services
type ZkReader interface {
	Get(req *ZkPath, reply *ZkNode) error
	GetV(req *ZkPathV, reply *ZkNodeV) error
	Children(req *ZkPath, reply *ZkNode) error
}

// helper method to register the server (does interface checking)
func Register(zkReader ZkReader) {
	rpc.Register(zkReader)
}
