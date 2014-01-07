// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"errors"
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
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
	// They shall be sorted.
	GetKnownCells() ([]string, error)

	//
	// Keyspace management, global.
	//

	// CreateKeyspace creates the given keyspace, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateKeyspace(keyspace string) error

	// GetKeyspaces returns the known keyspaces. They shall be sorted.
	GetKeyspaces() ([]string, error)

	// DeleteKeyspaceShards deletes all the shards in a keyspace.
	// Use with caution.
	DeleteKeyspaceShards(keyspace string) error

	//
	// Shard management, global.
	//

	// CreateShard creates an empty shard, assuming it doesn't exist
	// yet. The contents of the shard will be a new Shard{} object,
	// with KeyRange populated by the result of ValidateShardName().
	// Can return ErrNodeExists if it already exists.
	CreateShard(keyspace, shard string, value *Shard) error

	// UpdateShard unconditionnally updates the shard information
	// pointed at by si.keyspace / si.shard to the *si value.
	// This will only be called with a lock on the shard.
	// Can return ErrNoNode if the shard doesn't exist yet.
	UpdateShard(si *ShardInfo) error

	// ValidateShard performs routine checks on the shard.
	ValidateShard(keyspace, shard string) error

	// GetShard reads a shard and returns it. This returns an
	// object stored in the global cell, and a topology
	// implementation may choose to return a value from some sort
	// of cache. If you need stronger consistency guarantees,
	// please use GetShardCritical.
	//
	// Can return ErrNoNode
	GetShard(keyspace, shard string) (si *ShardInfo, err error)

	// GetShardCritical is like GetShard, but it always returns
	// consistent data.
	GetShardCritical(keyspace, shard string) (si *ShardInfo, err error)

	// GetShardNames returns the known shards in a keyspace.
	// Can return ErrNoNode if the keyspace wasn't created,
	// or if DeleteKeyspaceShards was called. They shall be sorted.
	GetShardNames(keyspace string) ([]string, error)

	//
	// Tablet management, per cell.
	//

	// CreateTablet creates the given tablet, assuming it doesn't exist
	// yet. It does *not* create the tablet replication paths.
	// Can return ErrNodeExists if it already exists.
	CreateTablet(tablet *Tablet) error

	// UpdateTablet updates a given tablet. The version is used
	// for atomic updates. UpdateTablet will return ErrNoNode if
	// the tablet doesn't exist and ErrBadVersion if the version
	// has changed.
	UpdateTablet(tablet *TabletInfo, existingVersion int64) (newVersion int64, err error)

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
	// Replication graph management, per cell.
	//

	// CreateShardReplication creates the ShardReplication object,
	// assuming it doesn't exist yet.
	// Can return ErrNodeExists if it already exists.
	CreateShardReplication(cell, keyspace, shard string, sr *ShardReplication) error

	// UpdateShardReplicationFields updates the current
	// ShardReplication record with new values
	// Can return ErrNoNode if the object doesn't exist.
	UpdateShardReplicationFields(cell, keyspace, shard string, update func(*ShardReplication) error) error

	// GetShardReplication returns the replication data.
	// Can return ErrNoNode if the object doesn't exist.
	GetShardReplication(cell, keyspace, shard string) (*ShardReplicationInfo, error)

	// DeleteShardReplication deletes the replication data.
	// Can return ErrNoNode if the object doesn't exist.
	DeleteShardReplication(cell, keyspace, shard string) error

	//
	// Serving Graph management, per cell.
	//

	// GetSrvTabletTypesPerShard returns the existing serving types
	// for a shard.
	// Can return ErrNoNode.
	GetSrvTabletTypesPerShard(cell, keyspace, shard string) ([]TabletType, error)

	// UpdateEndPoints updates the serving records for a cell,
	// keyspace, shard, tabletType.
	UpdateEndPoints(cell, keyspace, shard string, tabletType TabletType, addrs *EndPoints) error

	// GetEndPoints returns the EndPoints list of serving addresses
	// for a TabletType inside a shard.
	// Can return ErrNoNode.
	GetEndPoints(cell, keyspace, shard string, tabletType TabletType) (*EndPoints, error)

	// DeleteSrvTabletType deletes the serving records for a cell,
	// keyspace, shard, tabletType.
	// Can return ErrNoNode.
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

	// GetSrvKeyspaceNames returns the list of visible Keyspaces
	// in this cell. They shall be sorted.
	GetSrvKeyspaceNames(cell string) ([]string, error)

	// UpdateTabletEndpoint updates a single tablet record in the
	// already computed serving graph. The update has to be somewhat
	// atomic, so it requires Server intrisic knowledge.
	// If the node doesn't exist, it is not updated, this is not an error.
	UpdateTabletEndpoint(cell, keyspace, shard string, tabletType TabletType, addr *EndPoint) error

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
	CreateTabletPidNode(tabletAlias TabletAlias, contents string, done chan struct{}) error

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
	ReadTabletActionPath(actionPath string) (TabletAlias, string, int64, error)

	// UpdateTabletAction updates the actionPath with the new data.
	// version is the current version we're expecting. Use -1 to set
	// any version.
	// Can return ErrBadVersion.
	UpdateTabletAction(actionPath, data string, version int64) error

	// StoreTabletActionResponse stores the data for the response.
	// This will not unblock the caller yet.
	StoreTabletActionResponse(actionPath, data string) error

	// UnblockTabletAction will let the client continue.
	// StoreTabletActionResponse must have been called already.
	UnblockTabletAction(actionPath string) error
}

// Registry for Server implementations.
var serverImpls map[string]Server = make(map[string]Server)

// Which implementation to use
var topoImplementation = flag.String("topo_implementation", "zookeeper", "the topology implementation to use")

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

// GetServer returns 'our' Server, going down this list:
// - If only one is registered, that's the one.
// - If more than one are registered, use the 'topo_implementation' flag
//   (which defaults to zookeeper).
// - Then panics.
func GetServer() Server {
	if len(serverImpls) == 1 {
		for name, ts := range serverImpls {
			log.V(6).Infof("Using only topo.Server: %v", name)
			return ts
		}
	}

	result := serverImpls[*topoImplementation]
	if result == nil {
		panic(fmt.Errorf("No topo.Server named %v", *topoImplementation))
	}
	log.V(6).Infof("Using topo.Server: %v", *topoImplementation)
	return result
}

// Close all registered Server.
func CloseServers() {
	for name, ts := range serverImpls {
		log.V(6).Infof("Closing topo.Server: %v", name)
		ts.Close()
	}
}

// GetSubprocessFlags returns all the flags required to launch a subprocess
// with the exact same topology server as the current process.
func GetSubprocessFlags() []string {
	result := []string{
		"-topo_implementation", *topoImplementation,
	}
	return append(result, GetServer().GetSubprocessFlags()...)
}
