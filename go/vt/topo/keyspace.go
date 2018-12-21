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
	"fmt"
	"path"
	"sync"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/events"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains keyspace utility functions

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

// GetServedFrom returns a Keyspace_ServedFrom record if it exists.
func (ki *KeyspaceInfo) GetServedFrom(tabletType topodatapb.TabletType) *topodatapb.Keyspace_ServedFrom {
	for _, ksf := range ki.ServedFroms {
		if ksf.TabletType == tabletType {
			return ksf
		}
	}
	return nil
}

// CheckServedFromMigration makes sure a requested migration is safe
func (ki *KeyspaceInfo) CheckServedFromMigration(tabletType topodatapb.TabletType, keyspace string, remove bool) error {
	// master is a special case with a few extra checks
	if tabletType == topodatapb.TabletType_MASTER {
		if !remove {
			return fmt.Errorf("Cannot add master back to %v", ki.keyspace)
		}
		if len(ki.ServedFroms) > 1 {
			return fmt.Errorf("Cannot migrate master into %v until everything else is migrated", ki.keyspace)
		}
	}

	// we can't remove a type we don't have
	if ki.GetServedFrom(tabletType) == nil && remove {
		return fmt.Errorf("Supplied type cannot be migrated")
	}

	// check the keyspace is consistent in any case
	for _, ksf := range ki.ServedFroms {
		if ksf.Keyspace != keyspace {
			return fmt.Errorf("Inconsistent keypace specified in migration: %v != %v for type %v", keyspace, ksf.Keyspace, ksf.TabletType)
		}
	}

	return nil
}

// UpdateServedFromMap handles ServedFromMap. It can add or remove
// records, cells, ...
func (ki *KeyspaceInfo) UpdateServedFromMap(tabletType topodatapb.TabletType, keyspace string, remove bool) error {
	// check parameters to be sure
	if err := ki.CheckServedFromMigration(tabletType, keyspace, remove); err != nil {
		return err
	}

	ksf := ki.GetServedFrom(tabletType)
	if ksf == nil {
		// the record doesn't exist
		if remove {
			if len(ki.ServedFroms) == 0 {
				ki.ServedFroms = nil
			}
			log.Warningf("Trying to remove KeyspaceServedFrom for missing type %v in keyspace %v", tabletType, ki.keyspace)
		} else {
			ki.ServedFroms = append(ki.ServedFroms, &topodatapb.Keyspace_ServedFrom{
				TabletType: tabletType,
				Keyspace:   keyspace,
			})
		}
		return nil
	}

	if remove {
		var newServedFroms []*topodatapb.Keyspace_ServedFrom
		for _, k := range ki.ServedFroms {
			if k != ksf {
				newServedFroms = append(newServedFroms, k)
			}
		}
		ki.ServedFroms = newServedFroms
	} else {
		if ksf.Keyspace != keyspace {
			return fmt.Errorf("cannot UpdateServedFromMap on existing record for keyspace %v, different keyspace: %v != %v", ki.keyspace, ksf.Keyspace, keyspace)
		}
	}
	return nil
}

// ComputeCellServedFrom returns the ServedFrom list for a cell
func (ki *KeyspaceInfo) ComputeCellServedFrom(cell string) []*topodatapb.SrvKeyspace_ServedFrom {
	var result []*topodatapb.SrvKeyspace_ServedFrom
	for _, ksf := range ki.ServedFroms {
		result = append(result, &topodatapb.SrvKeyspace_ServedFrom{
			TabletType: ksf.TabletType,
			Keyspace:   ksf.Keyspace,
		})
	}
	return result
}

// CreateKeyspace wraps the underlying Conn.Create
// and dispatches the event.
func (ts *Server) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	data, err := proto.Marshal(value)
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
	keyspacePath := path.Join(KeyspacesPath, keyspace, KeyspaceFile)
	data, version, err := ts.globalCell.Get(ctx, keyspacePath)
	if err != nil {
		return nil, err
	}

	k := &topodatapb.Keyspace{}
	if err = proto.Unmarshal(data, k); err != nil {
		return nil, fmt.Errorf("bad keyspace data %v", err)
	}

	return &KeyspaceInfo{
		keyspace: keyspace,
		version:  version,
		Keyspace: k,
	}, nil
}

// UpdateKeyspace updates the keyspace data. It checks the keyspace is locked.
func (ts *Server) UpdateKeyspace(ctx context.Context, ki *KeyspaceInfo) error {
	// make sure it is locked first
	if err := CheckKeyspaceLocked(ctx, ki.keyspace); err != nil {
		return err
	}

	data, err := proto.Marshal(ki.Keyspace)
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

// FindAllShardsInKeyspace reads and returns all the existing shards in
// a keyspace. It doesn't take any lock.
func (ts *Server) FindAllShardsInKeyspace(ctx context.Context, keyspace string) (map[string]*ShardInfo, error) {
	shards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return nil, fmt.Errorf("failed to get list of shards for keyspace '%v': %v", keyspace, err)
	}

	result := make(map[string]*ShardInfo, len(shards))
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	rec := concurrency.FirstErrorRecorder{}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			si, err := ts.GetShard(ctx, keyspace, shard)
			if err != nil {
				if IsErrType(err, NoNode) {
					log.Warningf("GetShard(%v, %v) returned ErrNoNode, consider checking the topology.", keyspace, shard)
				} else {
					rec.RecordError(fmt.Errorf("GetShard(%v, %v) failed: %v", keyspace, shard, err))
				}
				return
			}
			mu.Lock()
			result[shard] = si
			mu.Unlock()
		}(shard)
	}
	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}
	return result, nil
}

// DeleteKeyspace wraps the underlying Conn.Delete
// and dispatches the event.
func (ts *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	keyspacePath := path.Join(KeyspacesPath, keyspace, KeyspaceFile)
	if err := ts.globalCell.Delete(ctx, keyspacePath, nil); err != nil {
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
