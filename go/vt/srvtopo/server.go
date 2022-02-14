/*
Package srvtopo contains a set of helper methods and classes to
use the topology service in a serving environment.
*/
package srvtopo

import (
	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

// Server is a subset of the topo.Server API that only contains
// the serving graph read-only calls used by clients to resolve
// serving addresses, and to get VSchema.
type Server interface {
	// GetTopoServer returns the full topo.Server instance.
	GetTopoServer() (*topo.Server, error)

	// GetSrvKeyspaceNames returns the list of keyspaces served in
	// the provided cell.
	GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error)

	// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)

	WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool)

	// WatchSrvVSchema starts watching the SrvVSchema object for
	// the provided cell.  It will call the callback when
	// a new value or an error occurs.
	WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool)
}
