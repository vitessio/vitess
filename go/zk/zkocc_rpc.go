// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	rpc "code.google.com/p/vitess/go/rpcplus"
)

// defines the RPC services for zkocc
// the service name to use is 'ZkReader'
type ZkReader interface {
	Get(req *ZkPath, reply *ZkNode) error
	GetV(req *ZkPathV, reply *ZkNodeV) error
	Children(req *ZkPath, reply *ZkNode) error
}

// helper method to register the server (does interface checking)
func RegisterZkReader(zkReader ZkReader) {
	rpc.Register(zkReader)
}
