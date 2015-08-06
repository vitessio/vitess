// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"errors"
	"flag"
	"fmt"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
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

// Server is the interface used to talk to a persistent
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
	GetKnownCells(ctx context.Context) ([]string, error)

	//
	// Keyspace management, global.
	//

	// CreateKeyspace creates the given keyspace, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateKeyspace(ctx context.Context, keyspace string, value *pb.Keyspace) error

	// UpdateKeyspace updates the keyspace information
	// pointed at by ki.keyspace to the *ki value.
	// This will only be called with a lock on the keyspace.
	// Can return ErrNoNode if the keyspace doesn't exist yet,
	// or ErrBadVersion if the version has changed.
	//
	// Do not use directly, but instead use topo.UpdateKeyspace.
	UpdateKeyspace(ctx context.Context, ki *KeyspaceInfo, existingVersion int64) (newVersion int64, err error)

	// DeleteKeyspace deletes the specified keyspace.
	// Can return ErrNoNode if the keyspace doesn't exist.
	DeleteKeyspace(ctx context.Context, keyspace string) error

	// GetKeyspace reads a keyspace and returns it.
	// Can return ErrNoNode
	GetKeyspace(ctx context.Context, keyspace string) (*KeyspaceInfo, error)

	// GetKeyspaces returns the known keyspace names. They shall be sorted.
	GetKeyspaces(ctx context.Context) ([]string, error)

	// DeleteKeyspaceShards deletes all the shards in a keyspace.
	// Use with caution.
	DeleteKeyspaceShards(ctx context.Context, keyspace string) error

	//
	// Shard management, global.
	//

	// CreateShard creates an empty shard, assuming it doesn't exist
	// yet. The contents of the shard will be a new Shard{} object,
	// with KeyRange populated by the result of ValidateShardName().
	// Can return ErrNodeExists if it already exists.
	CreateShard(ctx context.Context, keyspace, shard string, value *pb.Shard) error

	// UpdateShard updates the shard information
	// pointed at by si.keyspace / si.shard to the *si value.
	// This will only be called with a lock on the shard.
	// Can return ErrNoNode if the shard doesn't exist yet,
	// or ErrBadVersion if the version has changed.
	//
	// Do not use directly, but instead use topo.UpdateShard.
	UpdateShard(ctx context.Context, si *ShardInfo, existingVersion int64) (newVersion int64, err error)

	// ValidateShard performs routine checks on the shard.
	ValidateShard(ctx context.Context, keyspace, shard string) error

	// GetShard reads a shard and returns it.
	// Can return ErrNoNode
	GetShard(ctx context.Context, keyspace, shard string) (*ShardInfo, error)

	// GetShardNames returns the known shards in a keyspace.
	// Can return ErrNoNode if the keyspace wasn't created,
	// or if DeleteKeyspaceShards was called. They shall be sorted.
	GetShardNames(ctx context.Context, keyspace string) ([]string, error)

	// DeleteShard deletes the provided shard.
	// Can return ErrNoNode if the shard doesn't exist.
	DeleteShard(ctx context.Context, keyspace, shard string) error

	//
	// Tablet management, per cell.
	//

	// CreateTablet creates the given tablet, assuming it doesn't exist
	// yet. It does *not* create the tablet replication paths.
	// Can return ErrNodeExists if it already exists.
	CreateTablet(ctx context.Context, tablet *Tablet) error

	// UpdateTablet updates a given tablet. The version is used
	// for atomic updates. UpdateTablet will return ErrNoNode if
	// the tablet doesn't exist and ErrBadVersion if the version
	// has changed.
	//
	// Do not use directly, but instead use topo.UpdateTablet.
	UpdateTablet(ctx context.Context, tablet *TabletInfo, existingVersion int64) (newVersion int64, err error)

	// UpdateTabletFields updates the current tablet record
	// with new values, independently of the version
	// Can return ErrNoNode if the tablet doesn't exist.
	UpdateTabletFields(ctx context.Context, tabletAlias TabletAlias, update func(*Tablet) error) error

	// DeleteTablet removes a tablet from the system.
	// We assume no RPC is currently running to it.
	// TODO(alainjobart) verify this assumption, link with RPC code.
	// Can return ErrNoNode if the tablet doesn't exist.
	DeleteTablet(ctx context.Context, alias TabletAlias) error

	// GetTablet returns the tablet data (includes the current version).
	// Can return ErrNoNode if the tablet doesn't exist.
	GetTablet(ctx context.Context, alias TabletAlias) (*TabletInfo, error)

	// GetTabletsByCell returns all the tablets in the given cell.
	// Can return ErrNoNode if no tablet was ever created in that cell.
	GetTabletsByCell(ctx context.Context, cell string) ([]TabletAlias, error)

	//
	// Replication graph management, per cell.
	//

	// UpdateShardReplicationFields updates the current
	// ShardReplication record with new values. If the
	// ShardReplication object does not exist, an empty one will
	// be passed to the update function. All necessary directories
	// need to be created by this method, if applicable.
	UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*pb.ShardReplication) error) error

	// GetShardReplication returns the replication data.
	// Can return ErrNoNode if the object doesn't exist.
	GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*ShardReplicationInfo, error)

	// DeleteShardReplication deletes the replication data.
	// Can return ErrNoNode if the object doesn't exist.
	DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error

	// DeleteKeyspaceReplication deletes the replication data for all shards.
	// Can return ErrNoNode if the object doesn't exist.
	DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error

	//
	// Serving Graph management, per cell.
	//

	// LockSrvShardForAction locks the serving shard in order to
	// perform the action described by contents. It will wait for
	// the lock until at most ctx.Done(). The wait can be interrupted
	// by cancelling the context. It returns the lock path.
	//
	// Can return ErrTimeout or ErrInterrupted.
	LockSrvShardForAction(ctx context.Context, cell, keyspace, shard, contents string) (string, error)

	// UnlockSrvShardForAction unlocks a serving shard.
	UnlockSrvShardForAction(ctx context.Context, cell, keyspace, shard, lockPath, results string) error

	// GetSrvTabletTypesPerShard returns the existing serving types
	// for a shard.
	// Can return ErrNoNode.
	GetSrvTabletTypesPerShard(ctx context.Context, cell, keyspace, shard string) ([]TabletType, error)

	// CreateEndPoints creates and sets the serving records for a cell,
	// keyspace, shard, tabletType.
	// It returns ErrNodeExists if the record already exists.
	CreateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType TabletType, addrs *pb.EndPoints) error

	// UpdateEndPoints updates the serving records for a cell,
	// keyspace, shard, tabletType.
	// If existingVersion is -1, it will set the value unconditionally,
	// creating it if necessary.
	// Otherwise, it will Compare-And-Set only if the version matches.
	// Can return ErrBadVersion.
	// Can return ErrNoNode only if existingVersion is not -1.
	UpdateEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType TabletType, addrs *pb.EndPoints, existingVersion int64) error

	// GetEndPoints returns the EndPoints list of serving addresses
	// for a TabletType inside a shard, as well as the node version.
	// Can return ErrNoNode.
	GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType TabletType) (ep *pb.EndPoints, version int64, err error)

	// DeleteEndPoints deletes the serving records for a cell,
	// keyspace, shard, tabletType.
	// If existingVersion is -1, it will delete the records unconditionally.
	// Otherwise, it will Compare-And-Delete only if the version matches.
	// Can return ErrNoNode or ErrBadVersion.
	DeleteEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType TabletType, existingVersion int64) error

	// WatchEndPoints returns a channel that receives notifications
	// every time EndPoints for the given type changes.
	// It should receive a notification with the initial value fairly
	// quickly after this is set. A value of nil means the Endpoints
	// object doesn't exist or is empty. To stop watching this
	// EndPoints object, close the stopWatching channel.
	// If the underlying topo.Server encounters an error watching the node,
	// it should retry on a regular basis until it can succeed.
	// The initial error returned by this method is meant to catch
	// the obvious bad cases (invalid cell, invalid tabletType, ...)
	// that are never going to work. Mutiple notifications with the
	// same contents may be sent (for instance when the serving graph
	// is rebuilt, but the content hasn't changed).
	WatchEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType TabletType) (notifications <-chan *pb.EndPoints, stopWatching chan<- struct{}, err error)

	// UpdateSrvShard updates the serving records for a cell,
	// keyspace, shard.
	UpdateSrvShard(ctx context.Context, cell, keyspace, shard string, srvShard *pb.SrvShard) error

	// GetSrvShard reads a SrvShard record.
	// Can return ErrNoNode.
	GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*pb.SrvShard, error)

	// DeleteSrvShard deletes a SrvShard record.
	// Can return ErrNoNode.
	DeleteSrvShard(ctx context.Context, cell, keyspace, shard string) error

	// UpdateSrvKeyspace updates the serving records for a cell, keyspace.
	UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *SrvKeyspace) error

	// DeleteSrvKeyspace deletes the cell-local serving records for a keyspace.
	// Can return ErrNoNode.
	DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error

	// GetSrvKeyspace reads a SrvKeyspace record.
	// Can return ErrNoNode.
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*SrvKeyspace, error)

	// GetSrvKeyspaceNames returns the list of visible Keyspaces
	// in this cell. They shall be sorted.
	GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error)

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
}

// Schemafier is a temporary interface for supporting vschema
// reads and writes. It will eventually be merged into Server.
type Schemafier interface {
	SaveVSchema(context.Context, string) error
	GetVSchema(ctx context.Context) (string, error)
}

// Registry for Server implementations.
var serverImpls = make(map[string]Server)

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

// GetServerByName returns a specific Server by name, or nil.
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

// CloseServers closes all registered Server.
func CloseServers() {
	for name, ts := range serverImpls {
		log.V(6).Infof("Closing topo.Server: %v", name)
		ts.Close()
	}
}
