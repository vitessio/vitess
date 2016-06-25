// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"
)

// Error codes returned by the zookeeper Go client:
// http://bazaar.launchpad.net/~gozk/gozk/trunk/view/head:/zk.go
func convertError(err error) error {
	switch typeErr := err.(type) {
	case *zookeeper.Error:
		switch typeErr.Code {
		case zookeeper.ZBADVERSION:
			return topo.ErrBadVersion
		case zookeeper.ZNONODE:
			return topo.ErrNoNode
		case zookeeper.ZNODEEXISTS:
			return topo.ErrNodeExists
		case zookeeper.ZNOTEMPTY:
			return topo.ErrNotEmpty
		case zookeeper.ZOPERATIONTIMEOUT:
			return topo.ErrTimeout
		}
	default:
		switch err {
		case context.Canceled:
			return topo.ErrInterrupted
		case context.DeadlineExceeded:
			return topo.ErrTimeout
		}
	}
	return err
}
