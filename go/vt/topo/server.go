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

/*
Package topo is the module responsible for interacting with the topology
service. It uses one Conn connection to the global topo service (with
possibly another one to a read-only version of the global topo service),
and one to each cell topo service.

It contains the plug-in interfaces Conn, Factory and Version that topo
implementations will use. We support Zookeeper, etcd, consul as real
topo servers, and in-memory, tee as test and utility topo servers.
Implementations are in sub-directories here.

In tests, we do not mock this package. Instead, we just use a memorytopo.

We also support copying data across topo servers (using helpers/copy.go
and the topo2topo cmd binary), and writing to two topo servers at the same
time (using helpers/tee.go). This is to facilitate migrations between
topo servers.

There are two test sub-packages associated with this code:
- test/ contains a test suite that is run against all of our implementations.
  It just performs a bunch of common topo server activities (create, list,
  delete various objects, ...). If a topo implementation passes all these
  tests, it most likely will work as expected in a real deployment.
- topotests/ contains tests that use a memorytopo to test the code in this
  package.
*/
package topo

import (
	"errors"
	"flag"
	"fmt"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/log"
)

const (
	// GlobalCell is the name of the global cell.  It is special
	// as it contains the global topology, and references the other cells.
	GlobalCell = "global"

	// GlobalReadOnlyCell is the name of the global read-only cell
	// connection cell name.
	GlobalReadOnlyCell = "global-read-only"
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
	CellsPath     = "cells"
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

// Factory is a factory interface to create Conn objects.
// Topo implementations will provide an implementation for this.
type Factory interface {
	// HasGlobalReadOnlyCell returns true if the global cell
	// has read-only replicas of the topology data. The global topology
	// is usually more expensive to read from / write to, as it is
	// replicated over many cells. Some topology services provide
	// more efficient way to read the data, like Observer servers
	// for Zookeeper. If this returns true, we will maintain
	// two connections for the global topology: the 'global' cell
	// for consistent reads and writes, and the 'global-read-only'
	// cell for reads only.
	HasGlobalReadOnlyCell(serverAddr, root string) bool

	// Create creates a topo.Conn object.
	Create(cell, serverAddr, root string) (Conn, error)
}

// Server is the main topo.Server object. We support two ways of creating one:
// 1. From an implementation, server address, and root path.
//    This uses a plugin mechanism, and we have implementations for
//    etcd, zookeeper and consul.
// 2. Specific implementations may have higher level creation methods
//    (in which case they may provide a more complex Factory).
//    We support memorytopo (for tests and processes that only need an
//    in-memory server), and tee (a helper implementation to transition
//    between one server implementation and another).
type Server struct {
	// globalCell is the main connection to the global topo service.
	// It is created once at construction time.
	globalCell Conn

	// globalReadOnlyCell is the read-only connection to the global
	// topo service. It will be equal to globalCell if we don't distinguish
	// the two.
	globalReadOnlyCell Conn

	// factory allows the creation of connections to various backends.
	// It is set at construction time.
	factory Factory

	// mu protects the following fields.
	mu sync.Mutex
	// cells contains clients configured to talk to a list of
	// topo instances representing local topo clusters. These
	// should be accessed with the ConnForCell() method, which
	// will read the list of addresses for that cell from the
	// global cluster and create clients as needed.
	cells map[string]Conn
}

type cellsToRegionsMap struct {
	mu sync.Mutex
	// cellsToRegions contains all cell->region mappings
	cellsToRegions map[string]string
}

var (
	// topoImplementation is the flag for which implementation to use.
	topoImplementation = flag.String("topo_implementation", "zookeeper", "the topology implementation to use")

	// topoGlobalServerAddress is the address of the global topology
	// server.
	topoGlobalServerAddress = flag.String("topo_global_server_address", "", "the address of the global topology server")

	// topoGlobalRoot is the root path to use for the global topology
	// server.
	topoGlobalRoot = flag.String("topo_global_root", "", "the path of the global topology data in the global topology server")

	// factories has the factories for the Conn objects.
	factories = make(map[string]Factory)

	regions = cellsToRegionsMap{
		cellsToRegions: make(map[string]string),
	}
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

// NewWithFactory creates a new Server based on the given Factory.
// It also opens the global cell connection.
func NewWithFactory(factory Factory, serverAddress, root string) (*Server, error) {
	conn, err := factory.Create(GlobalCell, serverAddress, root)
	if err != nil {
		return nil, err
	}

	var connReadOnly Conn
	if factory.HasGlobalReadOnlyCell(serverAddress, root) {
		connReadOnly, err = factory.Create(GlobalReadOnlyCell, serverAddress, root)
		if err != nil {
			return nil, err
		}
	} else {
		connReadOnly = conn
	}

	return &Server{
		globalCell:         conn,
		globalReadOnlyCell: connReadOnly,
		factory:            factory,
		cells:              make(map[string]Conn),
	}, nil
}

// OpenServer returns a Server using the provided implementation,
// address and root for the global server.
func OpenServer(implementation, serverAddress, root string) (*Server, error) {
	factory, ok := factories[implementation]
	if !ok {
		return nil, ErrNoNode
	}
	return NewWithFactory(factory, serverAddress, root)
}

// Open returns a Server using the command line parameter flags
// for implementation, address and root. It log.Exits out if an error occurs.
func Open() *Server {
	ts, err := OpenServer(*topoImplementation, *topoGlobalServerAddress, *topoGlobalRoot)
	if err != nil {
		log.Exitf("Failed to open topo server (%v,%v,%v): %v", *topoImplementation, *topoGlobalServerAddress, *topoGlobalRoot, err)
	}
	return ts
}

// ConnForCell returns a Conn object for the given cell.
// It caches Conn objects from previously requested cells.
func (ts *Server) ConnForCell(ctx context.Context, cell string) (Conn, error) {
	// Global cell is the easy case.
	if cell == GlobalCell {
		return ts.globalCell, nil
	}

	// Return a cached client if present.
	ts.mu.Lock()
	conn, ok := ts.cells[cell]
	ts.mu.Unlock()
	if ok {
		return conn, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// These can proceed concurrently (we've released the lock).
	// We can use the GlobalReadOnlyCell for this call.
	ci, err := ts.GetCellInfo(ctx, cell, false /*strongRead*/)
	if err != nil {
		return nil, err
	}

	// Connect to the cell topo server, while holding the lock.
	// This ensures only one connection is established at any given time.
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Check if another goroutine beat us to creating a client for
	// this cell.
	if conn, ok = ts.cells[cell]; ok {
		return conn, nil
	}

	// Create the connection.
	conn, err = ts.factory.Create(cell, ci.ServerAddress, ci.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to create topo connection to %v, %v: %v", ci.ServerAddress, ci.Root, err)
	}
	ts.cells[cell] = conn
	return conn, nil
}

// GetRegionByCell returns the region group this `cell` belongs to, if there's none, it returns the `cell` as region.
func GetRegionByCell(ctx context.Context, ts *Server, cell string) string {
	regions.mu.Lock()
	defer regions.mu.Unlock()
	if region, ok := regions.cellsToRegions[cell]; ok {
		return region
	}
	if ts != nil {
		// lazily get the region from cell info if `regions.ts` is available
		info, err := ts.GetCellInfo(ctx, cell, false)
		if err == nil && info.Region != "" {
			regions.cellsToRegions[cell] = info.Region
			return info.Region
		}
	}
	// for backward compatability
	return cell
}

// UpdateCellsToRegionsForTests overwrites the global map built by topo server init, and is meant for testing purpose only.
func UpdateCellsToRegionsForTests(cellsToRegions map[string]string) {
	regions.mu.Lock()
	defer regions.mu.Unlock()
	regions.cellsToRegions = cellsToRegions
}

// Close will close all connections to underlying topo Server.
// It will nil all member variables, so any further access will panic.
func (ts *Server) Close() {
	ts.globalCell.Close()
	if ts.globalReadOnlyCell != ts.globalCell {
		ts.globalReadOnlyCell.Close()
	}
	ts.globalCell = nil
	ts.globalReadOnlyCell = nil
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, conn := range ts.cells {
		conn.Close()
	}
	ts.cells = make(map[string]Conn)
}
