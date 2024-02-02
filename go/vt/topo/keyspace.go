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

package topo

import (
	"context"
	"path"
	"sort"
	"sync"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/events"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file contains keyspace utility functions.

// Default concurrency to use in order to avoid overhwelming the topo server.
var DefaultConcurrency = 32

// shardKeySuffix is the suffix of a shard key.
// The full key looks like this:
// /vitess/global/keyspaces/customer/shards/80-/Shard
const shardKeySuffix = "Shard"

func registerFlags(fs *pflag.FlagSet) {
	fs.IntVar(&DefaultConcurrency, "topo_read_concurrency", DefaultConcurrency, "Concurrency of topo reads.")
}

func init() {
	servenv.OnParseFor("vtcombo", registerFlags)
	servenv.OnParseFor("vtctld", registerFlags)
	servenv.OnParseFor("vtgate", registerFlags)
}

// KeyspaceInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a keyspace.
type KeyspaceInfo struct {
	keyspace string
	version  Version
	*topodatapb.Keyspace
}

// KeyspaceName returns the keyspace name
func (ki *KeyspaceInfo) KeyspaceName() string {
	return ki.keyspace
}

// SetKeyspaceName sets the keyspace name
func (ki *KeyspaceInfo) SetKeyspaceName(name string) {
	ki.keyspace = name
}

// ValidateKeyspaceName checks if the provided name is a valid name for a
// keyspace.
func ValidateKeyspaceName(name string) error {
	return validateObjectName(name)
}

// CreateKeyspace wraps the underlying Conn.Create
// and dispatches the event.
func (ts *Server) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	if err := ValidateKeyspaceName(keyspace); err != nil {
		return vterrors.Wrapf(err, "CreateKeyspace: %s", err)
	}

	data, err := value.MarshalVT()
	if err != nil {
		return err
	}

	keyspacePath := path.Join(KeyspacesPath, keyspace, KeyspaceFile)
	if _, err := ts.globalCell.Create(ctx, keyspacePath, data); err != nil {
		return err
	}

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceName: keyspace,
		Keyspace:     value,
		Status:       "created",
	})
	return nil
}

// GetKeyspace reads the given keyspace and returns it
func (ts *Server) GetKeyspace(ctx context.Context, keyspace string) (*KeyspaceInfo, error) {
	if err := ValidateKeyspaceName(keyspace); err != nil {
		return nil, vterrors.Wrapf(err, "GetKeyspace: %s", err)
	}

	keyspacePath := path.Join(KeyspacesPath, keyspace, KeyspaceFile)
	data, version, err := ts.globalCell.Get(ctx, keyspacePath)
	if err != nil {
		return nil, err
	}

	k := &topodatapb.Keyspace{}
	if err = k.UnmarshalVT(data); err != nil {
		return nil, vterrors.Wrap(err, "bad keyspace data")
	}

	return &KeyspaceInfo{
		keyspace: keyspace,
		version:  version,
		Keyspace: k,
	}, nil
}

// GetKeyspaceDurability reads the given keyspace and returns its durabilty policy
func (ts *Server) GetKeyspaceDurability(ctx context.Context, keyspace string) (string, error) {
	keyspaceInfo, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return "", err
	}
	// Get the durability policy from the keyspace information
	// If it is unspecified, use the default durability which is "none" for backward compatibility
	if keyspaceInfo.GetDurabilityPolicy() != "" {
		return keyspaceInfo.GetDurabilityPolicy(), nil
	}
	return "none", nil
}

func (ts *Server) GetSidecarDBName(ctx context.Context, keyspace string) (string, error) {
	keyspaceInfo, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return "", err
	}
	if keyspaceInfo.SidecarDbName != "" {
		return keyspaceInfo.SidecarDbName, nil
	}
	return sidecar.DefaultName, nil
}

func (ts *Server) GetThrottlerConfig(ctx context.Context, keyspace string) (*topodatapb.ThrottlerConfig, error) {
	keyspaceInfo, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	return keyspaceInfo.ThrottlerConfig, nil
}

// UpdateKeyspace updates the keyspace data. It checks the keyspace is locked.
func (ts *Server) UpdateKeyspace(ctx context.Context, ki *KeyspaceInfo) error {
	// make sure it is locked first
	if err := CheckKeyspaceLocked(ctx, ki.keyspace); err != nil {
		return err
	}

	data, err := ki.Keyspace.MarshalVT()
	if err != nil {
		return err
	}
	keyspacePath := path.Join(KeyspacesPath, ki.keyspace, KeyspaceFile)
	version, err := ts.globalCell.Update(ctx, keyspacePath, data, ki.version)
	if err != nil {
		return err
	}
	ki.version = version

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceName: ki.keyspace,
		Keyspace:     ki.Keyspace,
		Status:       "updated",
	})
	return nil
}

// FindAllShardsInKeyspaceOptions controls the behavior of
// Server.FindAllShardsInKeyspace.
type FindAllShardsInKeyspaceOptions struct {
	// Concurrency controls the maximum number of concurrent calls to GetShard.
	// If <= 0, Concurrency is set to 1.
	Concurrency int
}

// FindAllShardsInKeyspace reads and returns all the existing shards in a
// keyspace. It doesn't take any lock.
//
// If opt is non-nil, it is used to configure the method's behavior. Otherwise,
// the default options are used.
func (ts *Server) FindAllShardsInKeyspace(ctx context.Context, keyspace string, opt *FindAllShardsInKeyspaceOptions) (map[string]*ShardInfo, error) {
	// Apply any necessary defaults.
	if opt == nil {
		opt = &FindAllShardsInKeyspaceOptions{}
	}
	if opt.Concurrency <= 0 {
		opt.Concurrency = DefaultConcurrency
	}

	// First try to get all shards using List if we can.
	buildResultFromList := func(kvpairs []KVInfo) (map[string]*ShardInfo, error) {
		result := make(map[string]*ShardInfo, len(kvpairs))
		for _, entry := range kvpairs {
			// The shard key looks like this: /vitess/global/keyspaces/commerce/shards/-80/Shard
			shardKey := string(entry.Key)
			// We don't want keys that aren't Shards. For example:
			// /vitess/global/keyspaces/commerce/shards/0/locks/7587876423742065323
			// This example key can happen with Shards because you can get a shard
			// lock in the topo via TopoServer.LockShard().
			if path.Base(shardKey) != shardKeySuffix {
				continue
			}
			shardName := path.Base(path.Dir(shardKey)) // The base part of the dir is "-80"
			// Validate the extracted shard name.
			if _, _, err := ValidateShardName(shardName); err != nil {
				return nil, vterrors.Wrapf(err, "FindAllShardsInKeyspace(%s): unexpected shard key/path %q contains invalid shard name/range %q",
					keyspace, shardKey, shardName)
			}
			shard := &topodatapb.Shard{}
			if err := shard.UnmarshalVT(entry.Value); err != nil {
				return nil, vterrors.Wrapf(err, "FindAllShardsInKeyspace(%s): invalid data found for shard %q in %q",
					keyspace, shardName, shardKey)
			}
			result[shardName] = &ShardInfo{
				keyspace:  keyspace,
				shardName: shardName,
				version:   entry.Version,
				Shard:     shard,
			}
		}
		return result, nil
	}
	shardsPath := path.Join(KeyspacesPath, keyspace, ShardsPath)
	listRes, err := ts.globalCell.List(ctx, shardsPath)
	if err == nil { // We have everything we need to build the result
		return buildResultFromList(listRes)
	}
	if IsErrType(err, NoNode) {
		// The path doesn't exist, let's see if the keyspace exists.
		if _, kerr := ts.GetKeyspace(ctx, keyspace); kerr != nil {
			return nil, vterrors.Wrapf(err, "FindAllShardsInKeyspace(%s): List", keyspace)
		}
		// We simply have no shards.
		return make(map[string]*ShardInfo, 0), nil
	}
	// Currently the ZooKeeper implementation does not support index prefix
	// scans so we fall back to concurrently fetching the shards one by one.
	// It is also possible that the response containing all shards is too
	// large in which case we also fall back to the one by one fetch.
	if !IsErrType(err, NoImplementation) && !IsErrType(err, ResourceExhausted) {
		return nil, vterrors.Wrapf(err, "FindAllShardsInKeyspace(%s): List", keyspace)
	}

	// Fall back to the shard by shard method.
	shards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to get list of shard names for keyspace '%s'", keyspace)
	}

	// Keyspaces with a large number of shards and geographically distributed
	// topo instances may experience significant latency fetching shard records.
	//
	// A prior version of this logic used unbounded concurrency to fetch shard
	// records which resulted in overwhelming topo server instances:
	// https://github.com/vitessio/vitess/pull/5436.
	//
	// However, removing the concurrency altogether can cause large operations
	// to fail due to timeout. The caller chooses the appropriate concurrency
	// level so that certain paths can be optimized (such as vtctld
	// RebuildKeyspace calls, which do not run on every vttablet).
	var (
		mu     sync.Mutex
		result = make(map[string]*ShardInfo, len(shards))
	)

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(int(opt.Concurrency))

	for _, shard := range shards {
		shard := shard

		eg.Go(func() error {
			si, err := ts.GetShard(ctx, keyspace, shard)
			switch {
			case IsErrType(err, NoNode):
				log.Warningf("GetShard(%s, %s) returned ErrNoNode, consider checking the topology.", keyspace, shard)
				return nil
			case err == nil:
				mu.Lock()
				result[shard] = si
				mu.Unlock()

				return nil
			default:
				return vterrors.Wrapf(err, "GetShard(%s, %s) failed", keyspace, shard)
			}
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}

// GetServingShards returns all shards where the primary is serving.
func (ts *Server) GetServingShards(ctx context.Context, keyspace string) ([]*ShardInfo, error) {
	shards, err := ts.FindAllShardsInKeyspace(ctx, keyspace, nil)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to get list of shards for keyspace '%v'", keyspace)
	}

	result := make([]*ShardInfo, 0, len(shards))
	for _, shard := range shards {
		if !shard.IsPrimaryServing {
			continue
		}
		result = append(result, shard)
	}
	if len(result) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%v has no serving shards", keyspace)
	}
	// Sort the shards by KeyRange for deterministic results.
	sort.Slice(result, func(i, j int) bool {
		return key.KeyRangeLess(result[i].KeyRange, result[j].KeyRange)
	})

	return result, nil
}

// GetOnlyShard returns the single ShardInfo of an unsharded keyspace.
func (ts *Server) GetOnlyShard(ctx context.Context, keyspace string) (*ShardInfo, error) {
	allShards, err := ts.FindAllShardsInKeyspace(ctx, keyspace, nil)
	if err != nil {
		return nil, err
	}
	if len(allShards) == 1 {
		for _, s := range allShards {
			return s, nil
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace %s must have one and only one shard: %v", keyspace, allShards)
}

// DeleteKeyspace wraps the underlying Conn.Delete
// and dispatches the event.
func (ts *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	keyspacePath := path.Join(KeyspacesPath, keyspace, KeyspaceFile)
	if err := ts.globalCell.Delete(ctx, keyspacePath, nil); err != nil {
		return err
	}

	// Delete the cell-global VSchema path
	// If not remove this, vtctld web page Dashboard will Display Error
	if err := ts.DeleteVSchema(ctx, keyspace); err != nil && !IsErrType(err, NoNode) {
		return err
	}

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceName: keyspace,
		Keyspace:     nil,
		Status:       "deleted",
	})
	return nil
}

// GetKeyspaces returns the list of keyspaces in the topology.
func (ts *Server) GetKeyspaces(ctx context.Context) ([]string, error) {
	children, err := ts.globalCell.ListDir(ctx, KeyspacesPath, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return nil, nil
	default:
		return nil, err
	}
}

// GetShardNames returns the list of shards in a keyspace.
func (ts *Server) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	shardsPath := path.Join(KeyspacesPath, keyspace, ShardsPath)
	children, err := ts.globalCell.ListDir(ctx, shardsPath, false /*full*/)
	if IsErrType(err, NoNode) {
		// The directory doesn't exist, let's see if the keyspace
		// is here or not.
		_, kerr := ts.GetKeyspace(ctx, keyspace)
		if kerr == nil {
			// Keyspace is here, means no shards.
			return nil, nil
		}
		return nil, err
	}
	return DirEntriesToStringArray(children), err
}
