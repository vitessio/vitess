// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// Error codes returned by the zookeeper Go client:
func convertError(err error) error {
	switch err {
	case zookeeper.ErrBadVersion:
		return topo.ErrBadVersion
	case zookeeper.ErrNoNode:
		return topo.ErrNoNode
	case zookeeper.ErrNodeExists:
		return topo.ErrNodeExists
	case zookeeper.ErrNotEmpty:
		return topo.ErrNotEmpty
	case zookeeper.ErrSessionExpired:
		return topo.ErrTimeout
	case context.Canceled:
		return topo.ErrInterrupted
	case context.DeadlineExceeded:
		return topo.ErrTimeout
	}
	return err
}
