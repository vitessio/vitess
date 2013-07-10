// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package naming

import (
	"errors"
	"fmt"
	"os"
	"time"

	"code.google.com/p/vitess/go/relog"
)

var (
	// This error is returned by functions to specify the
	// requested resource already exists.
	ErrNodeExists = errors.New("Node already exists")

	// This error is returned by functions to specify the requested
	// resource does not exist.
	ErrNoNode = errors.New("Node doesn't exist")

	// This error is returned by functions to specify a child of the
	// resource is still present and prevents the action from completing
	ErrNotEmpty = errors.New("Node not empty")
)

// TopologyServer is the interface used to talk to a persistent
// backend storage server and locking service.
//
// Zookeeper is a good example of this, and zktopo contains the
// implementation for this using zookeeper.
//
// Inside Google, we use Chubby.
type TopologyServer interface {
	// TopologyServer management interface
	Close()

	//
	// Keyspace management, global
	//

	// CreateKeyspace creates the given keyspace, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateKeyspace(keyspace string) error

	// GetKeyspaces returns the known keyspaces
	GetKeyspaces() ([]string, error)

	//
	// Shard management, global
	//

	// CreateShard creates the given shard, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateShard(keyspace, shard, contents string) error

	// UpdateShard unconditionnally updates the shard information
	// Can return ErrNoNode if the shard doesn't exist yet
	UpdateShard(keyspace, shard, contents string) error

	// ValidateShard performs routine checks on the shard
	ValidateShard(keyspace, shard string) error

	// GetShard reads a shard and returns it
	GetShard(keyspace, shard string) (contents string, err error)

	// GetShardNames returns the known shards in a keyspace
	GetShardNames(keyspace string) ([]string, error)

	//
	// Tablet management, per cell.
	// The tablet string is json-encoded.
	//

	// CreateTablet creates the given tablet, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateTablet(alias TabletAlias, contents string) error

	// UpdateTablet updates a given tablet. The version is used
	// for atomic updates (use -1 to overwrite any version)
	UpdateTablet(alias TabletAlias, contents string, existingVersion int) (newVersion int, err error)

	// ValidateTablet performs routine checks on the tablet
	ValidateTablet(alias TabletAlias) error

	// GetTablet returns the tablet contents, and the current version
	GetTablet(alias TabletAlias) (contents string, version int, err error)

	// GetTabletsByCell returns all the tablets in the given cell
	GetTabletsByCell(cell string) ([]TabletAlias, error)

	//
	// Replication graph management, global
	//
	// Uses a path for replication, use "" to get the masters,
	// /master to get the slaves
	//

	// GetReplicationPaths returns the replication paths for the parent path
	// - get the master(s): GetReplicationPaths(..., "")
	// - get the slaves: GetReplicationPaths(..., "/nyc-00020100")
	GetReplicationPaths(keyspace, shard, repPath string) ([]TabletAlias, error)

	// CreateReplicationPath creates a replication path
	// Can return ErrNodeExists if it already exists.
	CreateReplicationPath(keyspace, shard, repPath string) error

	// DeleteReplicationPath removes a replication path
	// Can returnErrNoNode if it doesn't exist
	DeleteReplicationPath(keyspace, shard, repPath string) error

	//
	// Keyspace and Shard locks for actions, global
	//

	// LockKeyspaceForAction locks the keyspace in order to
	// perform the action described by contents. It will wait for
	// the lock for at most duration. The wait can be interrupted
	// if the interrupted channel is closed. It returns the lock
	// path.
	LockKeyspaceForAction(keyspace, contents string, timeout time.Duration, interrupted chan struct{}) (string, error)

	// UnlockKeyspaceForAction unlocks a keyspace.
	UnlockKeyspaceForAction(keyspace, lockPath, results string) error

	// LockShardForAction locks the shard in order to
	// perform the action described by contents. It will wait for
	// the lock for at most duration. The wait can be interrupted
	// if the interrupted channel is closed. It returns the lock
	// path.
	LockShardForAction(keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error)

	// UnlockShardForAction unlocks a shard.
	UnlockShardForAction(keyspace, shard, lockPath, results string) error
}

// Registry for TopologyServer implementations
var topologyServerImpls map[string]TopologyServer = make(map[string]TopologyServer)

// RegisterTopologyServer adds an implementation for a TopologyServer.
// If an implementation with that name already exists, panics.
// Call this in the 'init' function in your module.
func RegisterTopologyServer(name string, ts TopologyServer) {
	if topologyServerImpls[name] != nil {
		panic(fmt.Errorf("Duplicate TopologyServer registration for %v", name))
	}
	topologyServerImpls[name] = ts
}

// Returns a specific TopologyServer by name, or nil
func GetTopologyServerByName(name string) TopologyServer {
	return topologyServerImpls[name]
}

// Returns 'our' TopologyServer:
// - If only one is registered, that's the one.
// - If more than one are registered, use the 'VT_TOPOLOGY_SERVER'
//   environment variable.
// - Then defaults to 'zookeeper'.
// - Then panics.
func GetTopologyServer() TopologyServer {
	if len(topologyServerImpls) == 1 {
		for name, ts := range topologyServerImpls {
			relog.Debug("Using only TopologyServer: %v", name)
			return ts
		}
	}

	name := os.Getenv("VT_TOPOLOGY_SERVER")
	if name == "" {
		name = "zookeeper"
	}
	result := topologyServerImpls[name]
	if result == nil {
		panic(fmt.Errorf("No TopologyServer named %v", name))
	}
	relog.Debug("Using TopologyServer: %v", name)
	return result
}

// Close all registered TopologyServer
func CloseTopologyServers() {
	for name, ts := range topologyServerImpls {
		relog.Debug("Closing TopologyServer: %v", name)
		ts.Close()
	}
}
