/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
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
	CellInfoFile           = "CellInfo"
	CellsAliasFile         = "CellsAlias"
	KeyspaceFile           = "Keyspace"
	ShardFile              = "Shard"
	VSchemaFile            = "VSchema"
	ShardReplicationFile   = "ShardReplication"
	TabletFile             = "Tablet"
	SrvVSchemaFile         = "SrvVSchema"
	SrvKeyspaceFile        = "SrvKeyspace"
	RoutingRulesFile       = "RoutingRules"
	ExternalClustersFile   = "ExternalClusters"
	ShardRoutingRulesFile  = "ShardRoutingRules"
	CommonRoutingRulesFile = "Rules"
	MirrorRulesFile        = "MirrorRules"
)

// Path for all object types.
const (
	CellsPath                = "cells"
	CellsAliasesPath         = "cells_aliases"
	KeyspacesPath            = "keyspaces"
	ShardsPath               = "shards"
	TabletsPath              = "tablets"
	MetadataPath             = "metadata"
	ExternalClusterVitess    = "vitess"
	RoutingRulesPath         = "routing_rules"
	KeyspaceRoutingRulesPath = "keyspace"
	NamedLocksPath           = "internal/named_locks"
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
//  1. From an implementation, server address, and root path.
//     This uses a plugin mechanism, and we have implementations for
//     etcd, zookeeper and consul.
//  2. Specific implementations may have higher level creation methods
//     (in which case they may provide a more complex Factory).
//     We support memorytopo (for tests and processes that only need an
//     in-memory server), and tee (a helper implementation to transition
//     between one server implementation and another).
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
	// cellConns contains clients configured to talk to a list of
	// topo instances representing local topo clusters. These
	// should be accessed with the ConnForCell() method, which
	// will read the list of addresses for that cell from the
	// global cluster and create clients as needed.
	cellConns map[string]cellConn
	// cellInfoGenerations counts each cell's expireCellInfo calls, so that a ConnForCell whose CellInfo read
	// started before an expiry does not mark its result as fresh.
	cellInfoGenerations map[string]uint64
	// cellInfoCacheTTL is defaultCellInfoCacheTTL; tests change it.
	cellInfoCacheTTL time.Duration
	// cellInfoLatestReads is, for each cell, when the latest CellInfo read that ConnForCell applied started. It
	// orders reads independently of cellConns, whose entries expire and are dropped, so that a read older than
	// one already applied is never applied after it.
	cellInfoLatestReads map[string]time.Time
}

type cellConn struct {
	cellInfo *topodata.CellInfo
	conn     Conn
	// cellInfoReadStart is when the read of cellInfo that conn was last checked against started, which the TTL
	// counts from.
	cellInfoReadStart time.Time
	// expired makes the next ConnForCell read the cell's CellInfo again, regardless of cellInfoReadStart.
	expired bool
}

// defaultCellInfoCacheTTL is how long ConnForCell reuses a cell's cached connection without reading the cell's
// CellInfo from the global topo again. CellInfo only changes when a cell is added, removed, or pointed at a
// different topo server, so re-reading it on every call costs a global read per cell-local operation for a
// change that is almost never there. A cell re-pointed by another process is noticed up to this long later, by
// new ConnForCell calls only; this process notices its own changes immediately.
const defaultCellInfoCacheTTL = 30 * time.Second

type cellsToAliasesMap struct {
	mu sync.Mutex
	// cellsToAliases contains all cell->alias mappings
	cellsToAliases map[string]string
}

var (
	// topoImplementation is the flag for which implementation to use.
	topoImplementation string

	// topoGlobalServerAddress is the address of the global topology
	// server.
	topoGlobalServerAddress string

	// topoGlobalRoot is the root path to use for the global topology
	// server.
	topoGlobalRoot string

	// factories has the factories for the Conn objects.
	factories = make(map[string]Factory)

	cellsAliases = cellsToAliasesMap{
		cellsToAliases: make(map[string]string),
	}

	FlagBinaries = []string{
		"vttablet", "vtctl", "vtctld", "vtcombo", "vtgate",
		"vtorc", "vtbackup",
	}

	// Default read concurrency to use in order to avoid overhwelming the topo server.
	DefaultReadConcurrency int64 = 32
)

func init() {
	for _, cmd := range FlagBinaries {
		servenv.OnParseFor(cmd, registerTopoFlags)
	}
}

func registerTopoFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &topoImplementation, "topo-implementation", topoImplementation, "the topology implementation to use")
	utils.SetFlagStringVar(fs, &topoGlobalServerAddress, "topo-global-server-address", topoGlobalServerAddress, "the address of the global topology server")
	utils.SetFlagStringVar(fs, &topoGlobalRoot, "topo-global-root", topoGlobalRoot, "the path of the global topology data in the global topology server")
	utils.SetFlagInt64Var(fs, &DefaultReadConcurrency, "topo-read-concurrency", DefaultReadConcurrency, "Maximum concurrency of topo reads per global or local cell.")
}

// RegisterFactory registers a Factory for an implementation for a Server.
// If an implementation with that name already exists, it exits the process.
// Call this in the 'init' function in your topology implementation module.
func RegisterFactory(name string, factory Factory) {
	if factories[name] != nil {
		log.Error(fmt.Sprintf("Duplicate topo.Factory registration for %v", name))
		os.Exit(1)
	}
	factories[name] = factory
}

// NewWithFactory creates a new Server based on the given Factory.
// It also opens the global cell connection.
func NewWithFactory(factory Factory, serverAddress, root string) (*Server, error) {
	globalReadSem := semaphore.NewWeighted(DefaultReadConcurrency)
	conn, err := factory.Create(GlobalCell, serverAddress, root)
	if err != nil {
		return nil, err
	}
	conn = NewStatsConn(GlobalCell, conn, globalReadSem)

	var connReadOnly Conn
	if factory.HasGlobalReadOnlyCell(serverAddress, root) {
		connReadOnly, err = factory.Create(GlobalReadOnlyCell, serverAddress, root)
		if err != nil {
			return nil, err
		}
		connReadOnly = NewStatsConn(GlobalReadOnlyCell, connReadOnly, globalReadSem)
	} else {
		connReadOnly = conn
	}

	return &Server{
		globalCell:          conn,
		globalReadOnlyCell:  connReadOnly,
		factory:             factory,
		cellConns:           make(map[string]cellConn),
		cellInfoGenerations: make(map[string]uint64),
		cellInfoCacheTTL:    defaultCellInfoCacheTTL,
		cellInfoLatestReads: make(map[string]time.Time),
	}, nil
}

// OpenServer returns a Server using the provided implementation,
// address and root for the global server.
func OpenServer(implementation, serverAddress, root string) (*Server, error) {
	factory, ok := factories[implementation]
	if !ok {
		return nil, NewError(NoImplementation, implementation)
	}
	return NewWithFactory(factory, serverAddress, root)
}

// Open returns a Server using the command line parameter flags
// for implementation, address and root. It exits the process if an error occurs.
func Open() *Server {
	if topoGlobalServerAddress == "" {
		log.Error("topo-global-server-address must be configured")
		os.Exit(1)
	}
	if topoGlobalRoot == "" {
		log.Error("topo-global-root must be non-empty")
		os.Exit(1)
	}
	ts, err := OpenServer(topoImplementation, topoGlobalServerAddress, topoGlobalRoot)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to open topo server (%v,%v,%v): %v", topoImplementation, topoGlobalServerAddress, topoGlobalRoot, err))
		os.Exit(1)
	}
	return ts
}

// ConnForCell returns a Conn object for the given cell.
// It caches Conn objects from previously requested cells.
func (ts *Server) ConnForCell(ctx context.Context, cell string) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Global cell is the easy case.
	if cell == GlobalCell {
		return ts.globalCell, nil
	}

	for {
		// Reuse the cached connection while the CellInfo it was checked against is recent.
		ts.mu.Lock()
		if cc, ok := ts.cellConns[cell]; ok && !cc.expired && time.Since(cc.cellInfoReadStart) < ts.cellInfoCacheTTL {
			ts.mu.Unlock()
			return cc.conn, nil
		}
		generation := ts.cellInfoGenerations[cell]
		ts.mu.Unlock()

		// Fetch cell cluster addresses from the global cluster. This is a strong read: the result is trusted for
		// cellInfoCacheTTL, so it must not come from a GlobalReadOnlyCell that lags behind.
		readStart := time.Now()
		ci, err := ts.GetCellInfo(ctx, cell, true /*strongRead*/)
		if err != nil {
			return nil, err
		}

		ts.mu.Lock()
		conn, superseded, err := ts.updateCellConnLocked(cell, generation, ci, readStart)
		ts.mu.Unlock()
		if !superseded {
			return conn, err
		}
		// A newer read was applied and left no connection, because connecting to what it read failed. What we
		// read may be older, so read again rather than connect to it.
	}
}

// updateCellConnLocked updates the cell's cached connection from a CellInfo whose read started at readStart, in
// the cell's given expiry generation. It reports the read as superseded, and changes nothing, when a read that
// started later has already been applied and left no connection to return. ts.mu must be held.
func (ts *Server) updateCellConnLocked(cell string, generation uint64, ci *topodata.CellInfo, readStart time.Time) (conn Conn, superseded bool, err error) {
	cc, ok := ts.cellConns[cell]
	// A read that started after ours has already been applied. What we read may be older than what it read, so
	// never apply ours after it: return its connection, or have the caller read again if it left none.
	if ts.cellInfoLatestReads[cell].After(readStart) {
		if ok {
			return cc.conn, false, nil
		}
		return nil, true, nil
	}
	ts.cellInfoLatestReads[cell] = readStart
	// If the cell's CellInfo was expired while we read it, what we read may predate the change, so leave the
	// entry expired for the next call to read again.
	expired := ts.cellInfoGenerations[cell] != generation
	if ok {
		// Client exists in cache.
		// Let's verify that it is the same cell as we are looking for.
		// The cell name can be re-used with a different ServerAddress and/or Root
		// in which case we should get a new connection and update the cache
		if ci.ServerAddress == cc.cellInfo.ServerAddress && ci.Root == cc.cellInfo.Root {
			cc.cellInfoReadStart, cc.expired = readStart, expired
			ts.cellConns[cell] = cc
			return cc.conn, false, nil
		}
		// Close the cached connection, we don't need it anymore
		if cc.conn != nil {
			cc.conn.Close()
		}
		// Drop it from the cache too, so that if connecting below fails, no call can get the closed connection.
		delete(ts.cellConns, cell)
	}

	// Connect to the cell topo server, while holding the lock.
	// This ensures only one connection is established at any given time.
	// Create the connection and cache it
	conn, err = ts.factory.Create(cell, ci.ServerAddress, ci.Root)
	switch {
	case err == nil:
		cellReadSem := semaphore.NewWeighted(DefaultReadConcurrency)
		statsConn := NewStatsConn(cell, conn, cellReadSem)
		// A Server that SetReadOnly made read-only stays read-only for the cells it connects to afterwards.
		if globalCellConn, ok := ts.globalCell.(*StatsConn); ok && globalCellConn.IsReadOnly() {
			statsConn.SetReadOnly(true)
		}
		ts.cellConns[cell] = cellConn{cellInfo: ci, conn: statsConn, cellInfoReadStart: readStart, expired: expired}
		return statsConn, false, nil
	case IsErrType(err, NoNode):
		err = vterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", ci.ServerAddress, ci.Root))
		return nil, false, NewError(NoNode, err.Error())
	default:
		return nil, false, vterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", ci.ServerAddress, ci.Root))
	}
}

// GetAliasByCell returns the alias group this `cell` belongs to, if there's none, it returns the `cell` as alias.
func GetAliasByCell(ctx context.Context, ts *Server, cell string) string {
	cellsAliases.mu.Lock()
	defer cellsAliases.mu.Unlock()
	if region, ok := cellsAliases.cellsToAliases[cell]; ok {
		return region
	}
	if ts != nil {
		// lazily get the region from cell info if `aliases` are available
		cellAliases, err := ts.GetCellsAliases(ctx, false)
		if err != nil {
			// for backward compatibility
			return cell
		}

		for alias, cellsAlias := range cellAliases {
			if slices.Contains(cellsAlias.Cells, cell) {
				cellsAliases.cellsToAliases[cell] = alias
				return alias
			}
		}
	}
	// for backward compatibility
	return cell
}

// Close will close all connections to underlying topo Server.
// It will nil all member variables, so any further access will panic.
func (ts *Server) Close() {
	if ts.globalCell != nil {
		ts.globalCell.Close()
	}
	if ts.globalReadOnlyCell != nil && ts.globalReadOnlyCell != ts.globalCell {
		ts.globalReadOnlyCell.Close()
	}
	ts.globalCell = nil
	ts.globalReadOnlyCell = nil
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, cc := range ts.cellConns {
		cc.conn.Close()
	}
	ts.cellConns = make(map[string]cellConn)
}

// expireCellInfo makes the next ConnForCell for cell read its CellInfo from the global topo again.
func (ts *Server) expireCellInfo(cell string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.cellInfoGenerations[cell]++
	if cc, ok := ts.cellConns[cell]; ok {
		cc.expired = true
		ts.cellConns[cell] = cc
	}
}

func (ts *Server) clearCellAliasesCache() {
	cellsAliases.mu.Lock()
	defer cellsAliases.mu.Unlock()
	cellsAliases.cellsToAliases = make(map[string]string)
}

// OpenExternalVitessClusterServer returns the topo server of the external cluster
func (ts *Server) OpenExternalVitessClusterServer(ctx context.Context, clusterName string) (*Server, error) {
	vc, err := ts.GetExternalVitessCluster(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	if vc == nil {
		return nil, fmt.Errorf("no vitess cluster found with name %s", clusterName)
	}
	var externalTopo *Server
	externalTopo, err = OpenServer(vc.TopoConfig.TopoType, vc.TopoConfig.Server, vc.TopoConfig.Root)
	if err != nil {
		return nil, err
	}
	if externalTopo == nil {
		return nil, fmt.Errorf("unable to open external topo for config %s", clusterName)
	}
	return externalTopo, nil
}

// SetReadOnly is initially ONLY implemented by StatsConn and used in ReadOnlyServer
func (ts *Server) SetReadOnly(readOnly bool) error {
	globalCellConn, ok := ts.globalCell.(*StatsConn)
	if !ok {
		return fmt.Errorf("invalid global cell connection type, expected StatsConn but found: %T", ts.globalCell)
	}
	globalCellConn.SetReadOnly(readOnly)

	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, cc := range ts.cellConns {
		localCellConn, ok := cc.conn.(*StatsConn)
		if !ok {
			return fmt.Errorf("invalid local cell connection type, expected StatsConn but found: %T", cc.conn)
		}
		localCellConn.SetReadOnly(true)
	}

	return nil
}

// IsReadOnly is initially ONLY implemented by StatsConn and used in ReadOnlyServer
func (ts *Server) IsReadOnly() (bool, error) {
	globalCellConn, ok := ts.globalCell.(*StatsConn)
	if !ok {
		return false, fmt.Errorf("invalid global cell connection type, expected StatsConn but found: %T", ts.globalCell)
	}
	if !globalCellConn.IsReadOnly() {
		return false, nil
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, cc := range ts.cellConns {
		localCellConn, ok := cc.conn.(*StatsConn)
		if !ok {
			return false, fmt.Errorf("invalid local cell connection type, expected StatsConn but found: %T", cc.conn)
		}
		if !localCellConn.IsReadOnly() {
			return false, nil
		}
	}

	return true, nil
}

// GetKeyspaceRoutingRulesPath returns the path to the keyspace routing rules file in the topo.
func (ts *Server) GetKeyspaceRoutingRulesPath() string {
	return path.Join(RoutingRulesPath, KeyspaceRoutingRulesPath, CommonRoutingRulesFile)
}
