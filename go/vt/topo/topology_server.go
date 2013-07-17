// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"errors"
	"fmt"
	"os"
	"time"

	"code.google.com/p/vitess/go/relog"
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
)

// topo.Server is the interface used to talk to a persistent
// backend storage server and locking service.
//
// Zookeeper is a good example of this, and zktopo contains the
// implementation for this using zookeeper.
//
// Inside Google, we use Chubby.
type Server interface {
	// topo.Server management interface.
	Close()

	//
	// Cell management, global
	//

	// GetKnownCells returns the list of known cells running our processes.
	// It is possible to find all tablets in the entire system
	// by then calling GetTabletsByCell on every cell, for instance.
	GetKnownCells() ([]string, error)

	//
	// Keyspace management, global.
	//

	// CreateKeyspace creates the given keyspace, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateKeyspace(keyspace string) error

	// GetKeyspaces returns the known keyspaces.
	GetKeyspaces() ([]string, error)

	// DeleteKeyspaceShards deletes all the shards in a keyspace.
	// Use with caution.
	DeleteKeyspaceShards(keyspace string) error

	//
	// Shard management, global.
	//

	// CreateShard creates an empty shard, assuming it doesn't exist
	// yet. The contents of the shard will be an empty Shard{} object.
	// Can return ErrNodeExists if it already exists.
	CreateShard(keyspace, shard string) error

	// UpdateShard unconditionnally updates the shard information
	// pointed at by si.keyspace / si.shard to the *si value.
	// Can return ErrNoNode if the shard doesn't exist yet.
	UpdateShard(si *ShardInfo) error

	// ValidateShard performs routine checks on the shard.
	ValidateShard(keyspace, shard string) error

	// GetShard reads a shard and returns it.
	GetShard(keyspace, shard string) (si *ShardInfo, err error)

	// GetShardNames returns the known shards in a keyspace.
	GetShardNames(keyspace string) ([]string, error)

	//
	// Tablet management, per cell.
	// The tablet string is json-encoded.
	//

	// CreateTablet creates the given tablet, assuming it doesn't exist
	// yet. It does *not* create the tablet replication paths.
	// Can return ErrNodeExists if it already exists.
	CreateTablet(tablet *Tablet) error

	// UpdateTablet updates a given tablet. The version is used
	// for atomic updates (use -1 to overwrite any version).
	// Can return ErrNoNode if the tablet doesn't exist.
	// Can return ErrBadVersion if the version has changed.
	UpdateTablet(tablet *TabletInfo, existingVersion int) (newVersion int, err error)

	// UpdateTabletFields updates the current tablet record
	// with new values, independently of the version
	// Can return ErrNoNode if the tablet doesn't exist.
	UpdateTabletFields(tabletAlias TabletAlias, update func(*Tablet) error) error

	// DeleteTablet removes a tablet from the system.
	// We assume no RPC is currently running to it.
	// TODO(alainjobart) verify this assumption, link with RPC code.
	// Can return ErrNoNode if the tablet doesn't exist.
	DeleteTablet(alias TabletAlias) error

	// ValidateTablet performs routine checks on the tablet.
	ValidateTablet(alias TabletAlias) error

	// GetTablet returns the tablet data (includes the current version).
	// Can return ErrNoNode if the tablet doesn't exist.
	GetTablet(alias TabletAlias) (*TabletInfo, error)

	// GetTabletsByCell returns all the tablets in the given cell.
	// Can return ErrNoNode if no tablet was ever created in that cell.
	GetTabletsByCell(cell string) ([]TabletAlias, error)

	//
	// Replication graph management, global.
	//
	// Uses a path for replication, use "" to get the masters,
	// /master to get the slaves.
	//

	// GetReplicationPaths returns the replication paths for the parent path
	// - get the master(s): GetReplicationPaths(..., "")
	// - get the slaves: GetReplicationPaths(..., "/nyc-00020100")
	GetReplicationPaths(keyspace, shard, repPath string) ([]TabletAlias, error)

	// CreateReplicationPath creates a replication path.
	// Can return ErrNodeExists if it already exists.
	CreateReplicationPath(keyspace, shard, repPath string) error

	// DeleteReplicationPath removes a replication path.
	// Can returnErrNoNode if it doesn't exist.
	DeleteReplicationPath(keyspace, shard, repPath string) error

	//
	// Serving Graph management, per cell.
	//

	// GetSrvTabletTypesPerShard returns the existing serving types
	// for a shard.
	// Can return ErrNoNode.
	GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]TabletType, error)

	// UpdateSrvTabletType updates the serving records for a cell,
	// keyspace, shard, tabletType.
	UpdateSrvTabletType(cell, keyspace, shard string, tabletType TabletType, addrs *VtnsAddrs) error

	// GetSrvTabletType returns the VtnsAddrs list of serving addresses
	// for a TabletType inside a shard.
	// Can return ErrNoNode.
	GetSrvTabletType(cell, keyspace, shard string, tabletType TabletType) (*VtnsAddrs, error)

	// DeleteSrvTabletType deletes the serving records for a cell,
	// keyspace, shard, tabletType.
	DeleteSrvTabletType(cell, keyspace, shard string, tabletType TabletType) error

	// UpdateSrvShard updates the serving records for a cell,
	// keyspace, shard.
	UpdateSrvShard(cell, keyspace, shard string, srvShard *SrvShard) error

	// GetSrvShard reads a SrvShard record.
	// Can return ErrNoNode.
	GetSrvShard(cell, keyspace, shard string) (*SrvShard, error)

	// UpdateSrvKeyspace updates the serving records for a cell, keyspace.
	UpdateSrvKeyspace(cell, keyspace string, srvKeyspace *SrvKeyspace) error

	// GetSrvKeyspace reads a SrvKeyspace record.
	// Can return ErrNoNode.
	GetSrvKeyspace(cell, keyspace string) (*SrvKeyspace, error)

	// UpdateTabletEndpoint updates a single tablet record in the
	// already computed serving graph. The update has to be somewhat
	// atomic, so it requires Server intrisic knowledge.
	UpdateTabletEndpoint(cell, keyspace, shard string, tabletType TabletType, addr *VtnsAddr) error

	//
	// Keyspace and Shard locks for actions, global.
	//

	// LockKeyspaceForAction locks the keyspace in order to
	// perform the action described by contents. It will wait for
	// the lock for at most duration. The wait can be interrupted
	// if the interrupted channel is closed. It returns the lock
	// path.
	// Can return ErrTimeout or ErrInterrupted
	LockKeyspaceForAction(keyspace, contents string, timeout time.Duration, interrupted chan struct{}) (string, error)

	// UnlockKeyspaceForAction unlocks a keyspace.
	UnlockKeyspaceForAction(keyspace, lockPath, results string) error

	// LockShardForAction locks the shard in order to
	// perform the action described by contents. It will wait for
	// the lock for at most duration. The wait can be interrupted
	// if the interrupted channel is closed. It returns the lock
	// path.
	// Can return ErrTimeout or ErrInterrupted
	LockShardForAction(keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error)

	// UnlockShardForAction unlocks a shard.
	UnlockShardForAction(keyspace, shard, lockPath, results string) error

	//
	// Remote Tablet Actions, local cell.
	//

	// WriteTabletAction initiates a remote action on the tablet.
	// Actions are queued up, and executed sequentially.  An
	// action is identified by the returned string, actionPath.
	WriteTabletAction(tabletAlias TabletAlias, contents string) (string, error)

	// WaitForTabletAction waits for a tablet action to complete. It
	// will wait for the result for at most duration. The wait can
	// be interrupted if the interrupted channel is closed.
	// Can return ErrTimeout or ErrInterrupted
	WaitForTabletAction(actionPath string, waitTime time.Duration, interrupted chan struct{}) (string, error)

	// PurgeTabletActions removes all queued actions for a tablet.
	// This might break the locking mechanism of the remote action
	// queue, used with caution.
	PurgeTabletActions(tabletAlias TabletAlias, canBePurged func(data string) bool) error

	//
	// Supporting the local agent process, local cell.
	//

	// ValidateTabletActions checks a tablet can execute remote
	// actions.
	ValidateTabletActions(tabletAlias TabletAlias) error

	// CreateTabletPidNode will keep a PID node up to date with
	// this tablet's current PID, until 'done' is closed.
	CreateTabletPidNode(tabletAlias TabletAlias, done chan struct{}) error

	// ValidateTabletPidNode makes sure a PID file exists for the tablet
	ValidateTabletPidNode(tabletAlias TabletAlias) error

	// GetSubprocessFlags returns the flags required to run a
	// subprocess that uses the same Server parameters as
	// this process.
	GetSubprocessFlags() []string

	// ActionEventLoop is the main loop for the action processing engine.
	// It will feed events to the dispatchAction callback.
	// If dispatchAction returns an error, we'll wait a bit before trying
	// again.
	// If 'done' is closed, the loop returns.
	ActionEventLoop(tabletAlias TabletAlias, dispatchAction func(actionPath, data string) error, done chan struct{})

	// ReadTabletActionPath reads the actionPath and returns the
	// associated TabletAlias, the data (originally written by
	// WriteTabletAction), and its version
	ReadTabletActionPath(actionPath string) (TabletAlias, string, int, error)

	// UpdateTabletAction updates the actionPath with the new data.
	// version is the current version we're expecting. Use -1 to set
	// any version.
	// Can return ErrBadVersion.
	UpdateTabletAction(actionPath, data string, version int) error

	// StoreTabletActionResponse stores the data for the response.
	// This will not unblock the caller yet.
	StoreTabletActionResponse(actionPath, data string) error

	// UnblockTabletAction will let the client continue.
	// StoreTabletActionResponse must have been called already.
	UnblockTabletAction(actionPath string) error
}

// Registry for Server implementations.
var serverImpls map[string]Server = make(map[string]Server)

// RegisterServer adds an implementation for a Server.
// If an implementation with that name already exists, panics.
// Call this in the 'init' function in your module.
func RegisterServer(name string, ts Server) {
	if serverImpls[name] != nil {
		panic(fmt.Errorf("Duplicate topo.Server registration for %v", name))
	}
	serverImpls[name] = ts
}

// Returns a specific Server by name, or nil.
func GetServerByName(name string) Server {
	return serverImpls[name]
}

// Returns 'our' Server:
// - If only one is registered, that's the one.
// - If more than one are registered, use the 'VT_TOPOLOGY_SERVER'
//   environment variable.
// - Then defaults to 'zookeeper'.
// - Then panics.
func GetServer() Server {
	if len(serverImpls) == 1 {
		for name, ts := range serverImpls {
			relog.Debug("Using only topo.Server: %v", name)
			return ts
		}
	}

	name := os.Getenv("VT_TOPOLOGY_SERVER")
	if name == "" {
		name = "zookeeper"
	}
	result := serverImpls[name]
	if result == nil {
		panic(fmt.Errorf("No topo.Server named %v", name))
	}
	relog.Debug("Using topo.Server: %v", name)
	return result
}

// Close all registered Server.
func CloseServers() {
	for name, ts := range serverImpls {
		relog.Debug("Closing topo.Server: %v", name)
		ts.Close()
	}
}
