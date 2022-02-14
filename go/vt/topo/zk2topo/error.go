package zk2topo

import (
	"context"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/topo"
)

// Error codes returned by the zookeeper Go client:
func convertError(err error, node string) error {
	switch err {
	case zk.ErrBadVersion:
		return topo.NewError(topo.BadVersion, node)
	case zk.ErrNoNode:
		return topo.NewError(topo.NoNode, node)
	case zk.ErrNodeExists:
		return topo.NewError(topo.NodeExists, node)
	case zk.ErrNotEmpty:
		return topo.NewError(topo.NodeNotEmpty, node)
	case zk.ErrSessionExpired:
		return topo.NewError(topo.Timeout, node)
	case context.Canceled:
		return topo.NewError(topo.Interrupted, node)
	case context.DeadlineExceeded:
		return topo.NewError(topo.Timeout, node)
	}
	return err
}
