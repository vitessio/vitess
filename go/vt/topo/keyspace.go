// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo/events"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains keyspace utility functions

// KeyspaceInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a keyspace.
type KeyspaceInfo struct {
	keyspace string
	version  int64
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
func (ki *KeyspaceInfo) CheckServedFromMigration(tabletType topodatapb.TabletType, cells []string, keyspace string, remove bool) error {
	// master is a special case with a few extra checks
	if tabletType == topodatapb.TabletType_MASTER {
		if !remove {
			return fmt.Errorf("Cannot add master back to %v", ki.keyspace)
		}
		if len(cells) > 0 {
			return fmt.Errorf("Cannot migrate only some cells for master removal in keyspace %v", ki.keyspace)
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
func (ki *KeyspaceInfo) UpdateServedFromMap(tabletType topodatapb.TabletType, cells []string, keyspace string, remove bool, allCells []string) error {
	// check parameters to be sure
	if err := ki.CheckServedFromMigration(tabletType, cells, keyspace, remove); err != nil {
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
				Cells:      cells,
				Keyspace:   keyspace,
			})
		}
		return nil
	}

	if remove {
		result, emptyList := removeCells(ksf.Cells, cells, allCells)
		if emptyList {
			// we don't have any cell left, we need to clear this record
			var newServedFroms []*topodatapb.Keyspace_ServedFrom
			for _, k := range ki.ServedFroms {
				if k != ksf {
					newServedFroms = append(newServedFroms, k)
				}
			}
			ki.ServedFroms = newServedFroms
		} else {
			ksf.Cells = result
		}
	} else {
		if ksf.Keyspace != keyspace {
			return fmt.Errorf("cannot UpdateServedFromMap on existing record for keyspace %v, different keyspace: %v != %v", ki.keyspace, ksf.Keyspace, keyspace)
		}
		ksf.Cells = addCells(ksf.Cells, cells)
	}
	return nil
}

// ComputeCellServedFrom returns the ServedFrom list for a cell
func (ki *KeyspaceInfo) ComputeCellServedFrom(cell string) []*topodatapb.SrvKeyspace_ServedFrom {
	var result []*topodatapb.SrvKeyspace_ServedFrom
	for _, ksf := range ki.ServedFroms {
		if InCellList(cell, ksf.Cells) {
			result = append(result, &topodatapb.SrvKeyspace_ServedFrom{
				TabletType: ksf.TabletType,
				Keyspace:   ksf.Keyspace,
			})
		}
	}
	return result
}

// CreateKeyspace wraps the underlying Impl.CreateKeyspace
// and dispatches the event.
func (ts Server) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	if err := ts.Impl.CreateKeyspace(ctx, keyspace, value); err != nil {
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
func (ts Server) GetKeyspace(ctx context.Context, keyspace string) (*KeyspaceInfo, error) {
	value, version, err := ts.Impl.GetKeyspace(ctx, keyspace)
	if err != nil {
		return nil, err
	}

	return &KeyspaceInfo{
		keyspace: keyspace,
		version:  version,
		Keyspace: value,
	}, nil
}

// UpdateKeyspace updates the keyspace data. It checks the keyspace is locked.
func (ts Server) UpdateKeyspace(ctx context.Context, ki *KeyspaceInfo) error {
	// make sure it is locked first
	if err := CheckKeyspaceLocked(ctx, ki.keyspace); err != nil {
		return err
	}

	// call the Impl's version
	newVersion, err := ts.Impl.UpdateKeyspace(ctx, ki.keyspace, ki.Keyspace, ki.version)
	if err != nil {
		return err
	}
	ki.version = newVersion

	event.Dispatch(&events.KeyspaceChange{
		KeyspaceName: ki.keyspace,
		Keyspace:     ki.Keyspace,
		Status:       "updated",
	})
	return nil
}

// FindAllShardsInKeyspace reads and returns all the existing shards in
// a keyspace. It doesn't take any lock.
func (ts Server) FindAllShardsInKeyspace(ctx context.Context, keyspace string) (map[string]*ShardInfo, error) {
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
				if err == ErrNoNode {
					log.Warningf("GetShard(%v,%v) returned ErrNoNode, consider checking the topology.", keyspace, shard)
				} else {
					rec.RecordError(fmt.Errorf("GetShard(%v,%v) failed: %v", keyspace, shard, err))
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

// DeleteKeyspace wraps the underlying Impl.DeleteKeyspace
// and dispatches the event.
func (ts Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	if err := ts.Impl.DeleteKeyspace(ctx, keyspace); err != nil {
		return err
	}
	event.Dispatch(&events.KeyspaceChange{
		KeyspaceName: keyspace,
		Keyspace:     nil,
		Status:       "deleted",
	})
	return nil
}
