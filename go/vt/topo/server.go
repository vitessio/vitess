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

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
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
// Zookeeper is a good example of this, and zktopo contains the
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
	// Keyspace management, global.
	//

	// CreateKeyspace creates the given keyspace, assuming it doesn't exist
	// yet. Can return ErrNodeExists if it already exists.
	CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error

	// UpdateKeyspace updates the keyspace information
	// pointed at by ki.keyspace to the *ki value.
	// This will only be called with a lock on the keyspace.
	// Can return ErrNoNode if the keyspace doesn't exist yet,
	// or ErrBadVersion if the version has changed.
	//
	// Do not use directly, but instead use Server.UpdateKeyspace.
	UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (newVersion int64, err error)

	// DeleteKeyspace deletes the specified keyspace.
	// Can return ErrNoNode if the keyspace doesn't exist.
	DeleteKeyspace(ctx context.Context, keyspace string) error

	// GetKeyspace reads a keyspace and returns it, along with its version.
	// Can return ErrNoNode
	GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error)

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
	CreateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard) error

	// UpdateShard updates the shard information
	// pointed at by si.keyspace / si.shard to the *si value.
	// Can return ErrNoNode if the shard doesn't exist yet,
	// or ErrBadVersion if the version has changed.
	//
	// Do not use directly, but instead use topo.UpdateShardFields.
	UpdateShard(ctx context.Context, keyspace, shard string, value *topodatapb.Shard, existingVersion int64) (newVersion int64, err error)

	// ValidateShard performs routine checks on the shard.
	ValidateShard(ctx context.Context, keyspace, shard string) error

	// GetShard reads a shard and returns it, along with its version.
	// Can return ErrNoNode
	GetShard(ctx context.Context, keyspace, shard string) (*topodatapb.Shard, int64, error)

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
	CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error

	// UpdateTablet updates a given tablet. The version is used
	// for atomic updates. UpdateTablet will return ErrNoNode if
	// the tablet doesn't exist and ErrBadVersion if the version
	// has changed.
	//
	// Do not use directly, but instead use topo.UpdateTablet.
	UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (newVersion int64, err error)

	// DeleteTablet removes a tablet from the system.
	// We assume no RPC is currently running to it.
	// TODO(alainjobart) verify this assumption, link with RPC code.
	// Can return ErrNoNode if the tablet doesn't exist.
	DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error

	// GetTablet returns the tablet data (includes the current version).
	// Can return ErrNoNode if the tablet doesn't exist.
	GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error)

	// GetTabletsByCell returns all the tablets in the given cell.
	// Can return ErrNoNode if no tablet was ever created in that cell.
	GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error)

	//
	// Replication graph management, per cell.
	//

	// UpdateShardReplicationFields updates the current
	// ShardReplication record with new values. If the
	// ShardReplication object does not exist, an empty one will
	// be passed to the update function. All necessary directories
	// need to be created by this method, if applicable.
	UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error

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

	//
	// Master election methods. This is meant to have a small
	// number of processes elect a master within a group. The
	// backend storage for this can either be the global topo
	// server, or a resilient quorum of individual cells, to
	// reduce the load / dependency on the global topo server.
	//

	// NewMasterParticipation creates a MasterParticipation
	// object, used to become the Master in an election for the
	// provided group name.  Id is the name of the local process,
	// passing in the hostname:port of the current process as id
	// is the common usage. Id must be unique for each process
	// calling this, for a given name. Calling this function does
	// not make the current process a candidate for the election.
	NewMasterParticipation(name, id string) (MasterParticipation, error)
}

// Server is a wrapper type that can have extra methods.
// Outside modules should just use the Server object.
type Server struct {
	Impl
}

// MasterParticipation is the object returned by NewMasterParticipation.
// Sample usage:
//
// mp := server.NewMasterParticipation("vtctld", "hostname:8080")
// job := NewJob()
// go func() {
//   for {
//     ctx, err := mp.WaitForMastership()
//     switch err {
//     case nil:
//       job.RunUntilContextDone(ctx)
//     case topo.ErrInterrupted:
//       return
//     default:
//       log.Errorf("Got error while waiting for master, will retry in 5s: %v", err)
//       time.Sleep(5 * time.Second)
//     }
//   }
// }()
//
// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
//   if job.Running() {
//     job.WriteStatus(w, r)
//   } else {
//     http.Redirect(w, r, mp.GetCurrentMasterID(), http.StatusFound)
//   }
// })
//
// servenv.OnTermSync(func() {
//   mp.Shutdown()
// })
type MasterParticipation interface {
	// WaitForMastership makes the current process a candidate
	// for election, and waits until this process is the master.
	// After we become the master, we may lose mastership. In that case,
	// the returned context will be canceled. If Stop was called,
	// WaitForMastership will return nil, ErrInterrupted.
	WaitForMastership() (context.Context, error)

	// Stop is called when we don't want to participate in the
	// master election any more. Typically, that is when the
	// hosting process is terminating.  We will relinquish
	// mastership at that point, if we had it. Stop should
	// not return until everything has been done.
	// The MasterParticipation object should be discarded
	// after Stop has been called. Any call to WaitForMastership
	// after Stop() will return nil, ErrInterrupted.
	// If WaitForMastership() was running, it will return
	// nil, ErrInterrupted as soon as possible.
	Stop()

	// GetCurrentMasterID returns the current master id.
	// This may not work after Stop has been called.
	GetCurrentMasterID() (string, error)
}

// SrvTopoServer is a subset of the Server API that only contains the serving
// graph read-only calls used by clients to resolve serving addresses,
// and how to get VSchema. It is mostly used by our discovery modules,
// and by vtgate.
type SrvTopoServer interface {
	GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error)
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)
	WatchSrvVSchema(ctx context.Context, cell string) (*WatchSrvVSchemaData, <-chan *WatchSrvVSchemaData, func())
}

// Registry for Server implementations.
var serverImpls = make(map[string]Impl)

// Which implementation to use
var topoImplementation = flag.String("topo_implementation", "zookeeper", "the topology implementation to use")

// RegisterServer adds an implementation for a Server.
// If an implementation with that name already exists, panics.
// Call this in the 'init' function in your module.
func RegisterServer(name string, ts Impl) {
	if serverImpls[name] != nil {
		panic(fmt.Errorf("Duplicate topo.Server registration for %v", name))
	}
	serverImpls[name] = ts
}

// GetServerByName returns a specific Server by name, or nil.
func GetServerByName(name string) Server {
	return Server{Impl: serverImpls[name]}
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
			return Server{Impl: ts}
		}
	}

	result := serverImpls[*topoImplementation]
	if result == nil {
		panic(fmt.Errorf("No topo.Server named %v", *topoImplementation))
	}
	log.V(6).Infof("Using topo.Server: %v", *topoImplementation)
	return Server{Impl: result}
}

// CloseServers closes all registered Server.
func CloseServers() {
	for name, ts := range serverImpls {
		log.V(6).Infof("Closing topo.Server: %v", name)
		ts.Close()
	}
}
