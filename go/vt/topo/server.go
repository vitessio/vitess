/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"errors"
	"flag"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// Filenames for all object types.
const (
	CellInfoFile         = "CellInfo"
	KeyspaceFile         = "Keyspace"
	ShardFile            = "Shard"
	VSchemaFile          = "VSchema"
	ShardReplicationFile = "ShardReplication"
	TabletFile           = "Tablet"
	SrvVSchemaFile       = "SrvVSchema"
	SrvKeyspaceFile      = "SrvKeyspace"
)

// Path for all object types.
const (
	KeyspacesPath = "keyspaces"
	ShardsPath    = "shards"
	TabletsPath   = "tablets"
)

var (
	// ErrNodeExists is returned by functions to specify the
	// requested resource already exists.
	ErrNodeExists = errors.New("node already exists")

	// ErrNoNode is returned by functions to specify the requested
	// resource does not exist.
	ErrNoNode = errors.New("node doesn't exist")

	// ErrNotEmpty is returned by functions to specify a child of the
	// resource is still present and prevents the action from completing.
	ErrNotEmpty = errors.New("node not empty")

	// ErrTimeout is returned by functions that wait for a result
	// when the timeout value is reached.
	ErrTimeout = errors.New("deadline exceeded")

	// ErrInterrupted is returned by functions that wait for a result
	// when they are interrupted.
	ErrInterrupted = errors.New("interrupted")

	// ErrBadVersion is returned by an update function that
	// failed to update the data because the version was different
	ErrBadVersion = errors.New("bad node version")

	// ErrPartialResult is returned by a function that could only
	// get a subset of its results
	ErrPartialResult = errors.New("partial result")

	// ErrNoUpdateNeeded can be returned by an 'UpdateFields' method
	// to skip any update.
	ErrNoUpdateNeeded = errors.New("no update needed")
)

// Impl is the interface used to talk to a persistent
// backend storage server and locking service.
//
// Zookeeper is a good example of this, and go/vt/topo/zk2topo contains the
// implementation for this using zookeeper.
//
// Inside Google, we use Chubby.
//
// FIXME(alainjobart) we are deprecating this interface, to be
// replaced with a lower level interface defined by Backend.
type Impl interface {
	// Impl will eventually be entirely replaced with Backend, and
	// just disappear.
	Backend

	// topo.Server management interface.
	Close()

	//
	// Cell management, global
	//

	// GetKnownCells returns the list of known cells running our processes.
	// It is possible to find all tablets in the entire system
	// by then calling GetTabletsByCell on every cell, for instance.
	// They shall be sorted.
	GetKnownCells(ctx context.Context) ([]string, error)

	//
	// Serving Graph management, per cell.
	//

	// GetSrvKeyspaceNames returns the list of visible Keyspaces
	// in this cell. They shall be sorted.
	GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error)

	// UpdateSrvKeyspace updates the serving records for a cell, keyspace.
	UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error

	// DeleteSrvKeyspace deletes the cell-local serving records for a keyspace.
	// Can return ErrNoNode.
	DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error

	// GetSrvKeyspace reads a SrvKeyspace record.
	// Can return ErrNoNode.
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)

	// UpdateSrvVSchema updates the serving records for a cell.
	UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error

	// GetSrvVSchema reads a SrvVSchema record.
	// Can return ErrNoNode.
	GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error)

	//
	// Keyspace and Shard locks for actions, global.
	//

	// LockKeyspaceForAction locks the keyspace in order to
	// perform the action described by contents. It will wait for
	// the lock until at most ctx.Done(). The wait can be interrupted
	// by cancelling the context. It returns the lock path.
	//
	// Can return ErrTimeout or ErrInterrupted
	LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error)

	// UnlockKeyspaceForAction unlocks a keyspace.
	UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error

	// LockShardForAction locks the shard in order to
	// perform the action described by contents. It will wait for
	// the lock until at most ctx.Done(). The wait can be interrupted
	// by cancelling the context. It returns the lock path.
	//
	// Can return ErrTimeout or ErrInterrupted
	LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error)

	// UnlockShardForAction unlocks a shard.
	UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error

	//
	// V3 Schema management, global
	//

	// SaveVSchema saves the provided schema in the topo server.
	SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error

	// GetVSchema retrieves the schema from the topo server.
	//
	// Can return ErrNoNode
	GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error)
}

// Server is a wrapper type that can have extra methods.
// Outside modules should just use the Server object.
type Server struct {
	Impl
}

// SrvTopoServer is a subset of the Server API that only contains the serving
// graph read-only calls used by clients to resolve serving addresses,
// and how to get VSchema. It is mostly used by our discovery modules,
// and by vtgate.
type SrvTopoServer interface {
	GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error)
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)
	WatchSrvVSchema(ctx context.Context, cell string) (*WatchSrvVSchemaData, <-chan *WatchSrvVSchemaData, CancelFunc)
}

// Factory is a factory method to create Impl objects.
type Factory func(serverAddr, root string) (Impl, error)

var (
	// topoImplementation is the flag for which implementation to use.
	topoImplementation = flag.String("topo_implementation", "zookeeper", "the topology implementation to use")

	// topoGlobalServerAddress is the address of the global topology
	// server.
	topoGlobalServerAddress = flag.String("topo_global_server_address", "", "the address of the global topology server")

	// topoGlobalRoot is the root path to use for the global topology
	// server.
	topoGlobalRoot = flag.String("topo_global_root", "", "the path of the global topology data in the global topology server")

	// factories has the factories for the Impl objects.
	factories = make(map[string]Factory)
)

// RegisterFactory registers a Factory for an implementation for a Server.
// If an implementation with that name already exists, it log.Fatals out.
// Call this in the 'init' function in your topology implementation module.
func RegisterFactory(name string, factory Factory) {
	if factories[name] != nil {
		log.Fatalf("Duplicate topo.Factory registration for %v", name)
	}
	factories[name] = factory
}

// OpenServer returns a Server using the provided implementation,
// address and root.
func OpenServer(implementation, serverAddress, root string) (Server, error) {
	factory, ok := factories[implementation]
	if !ok {
		return Server{}, ErrNoNode
	}

	impl, err := factory(serverAddress, root)
	if err != nil {
		return Server{}, err
	}
	return Server{Impl: impl}, nil
}

// Open returns a Server using the command line parameter flags
// for implementation, address and root. It log.Fatals out if an error occurs.
func Open() Server {
	ts, err := OpenServer(*topoImplementation, *topoGlobalServerAddress, *topoGlobalRoot)
	if err != nil {
		log.Fatalf("Failed to open topo server (%v,%v,%v): %v", *topoImplementation, *topoGlobalServerAddress, *topoGlobalRoot, err)
	}
	return ts
}
