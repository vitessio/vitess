// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"errors"
	"net/rpc"
)

// the overall states the src/dst hosts can be in. eventually, some of these
// operations/states might be interleaved
/*
const (
	RUNNING = iota,
  STOPPED,
	COMPRESSING,
	COMPRESSED,
	TRANSFERRING,
	DECOMPRESSING,
	DECOMPRESSED,
	COPYING,
	PRUNING,
	REPLICATING,
	READY,
	HW_FAILURE
)
*/
var UnimplementedError = errors.New("unimplemented")

func connectToTabletServer(addr string) (client *rpc.Client, err error) {
	client, err = rpc.DialHTTP("tcp", addr)
	return
}
