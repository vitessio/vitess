// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
)

// This file contains keyspace utility functions

// KeyspaceServedFrom is a per-cell record to redirect traffic to another
// keyspace. Used for vertical splits.
type KeyspaceServedFrom struct {
	// who is targeted
	Cells []string // nil means all cells

	// where to redirect
	Keyspace string
}

// Keyspace is the data structure that has data about the Keyspaces in
// the topology. Most fields are optional.
type Keyspace struct {
	// name of the column used for sharding
	// empty if the keyspace is not sharded
	ShardingColumnName string

	// type of the column used for sharding
	// KIT_UNSET if the keyspace is not sharded
	ShardingColumnType key.KeyspaceIdType

	// ServedFromMap will redirect the appropriate traffic to
	// another keyspace
	ServedFromMap map[TabletType]*KeyspaceServedFrom

	// Number of shards to use for batch job / mapreduce jobs
	// that need to split a given keyspace into multiple shards.
	// The value N used should be big enough that all possible shards
	// cover 1/Nth of the entire space or more.
	// It is usually the number of shards in the system. If a keyspace
	// is being resharded from M to P shards, it should be max(M, P).
	// That way we can guarantee a query that is targeted to 1/N of the
	// keyspace will land on just one shard.
	SplitShardCount int32
}

// KeyspaceInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a keyspace.
type KeyspaceInfo struct {
	keyspace string
	version  int64
	*Keyspace
}

// KeyspaceName returns the keyspace name
func (ki *KeyspaceInfo) KeyspaceName() string {
	return ki.keyspace
}

// Version returns the keyspace version from last time it was read or updated.
func (ki *KeyspaceInfo) Version() int64 {
	return ki.version
}

// NewKeyspaceInfo returns a KeyspaceInfo basing on keyspace with the
// keyspace. This function should be only used by Server
// implementations.
func NewKeyspaceInfo(keyspace string, value *Keyspace, version int64) *KeyspaceInfo {
	return &KeyspaceInfo{
		keyspace: keyspace,
		version:  version,
		Keyspace: value,
	}
}

// CheckServedFromMigration makes sure a requested migration is safe
func (ki *KeyspaceInfo) CheckServedFromMigration(tabletType TabletType, cells []string, keyspace string, remove bool) error {
	// master is a special case with a few extra checks
	if tabletType == TYPE_MASTER {
		if !remove {
			return fmt.Errorf("Cannot add master back to %v", ki.keyspace)
		}
		if len(cells) > 0 {
			return fmt.Errorf("Cannot migrate only some cells for master removal in keyspace %v", ki.keyspace)
		}
		if len(ki.ServedFromMap) > 1 {
			return fmt.Errorf("Cannot migrate master into %v until everything else is migrated", ki.keyspace)
		}
	}

	// we can't remove a type we don't have
	if _, ok := ki.ServedFromMap[tabletType]; !ok && remove {
		return fmt.Errorf("Supplied type cannot be migrated")
	}

	// check the keyspace is consistent in any case
	for tt, ksf := range ki.ServedFromMap {
		if ksf.Keyspace != keyspace {
			return fmt.Errorf("Inconsistent keypace specified in migration: %v != %v for type %v", keyspace, ksf.Keyspace, tt)
		}
	}

	return nil
}

// UpdateServedFromMap handles ServedFromMap. It can add or remove
// records, cells, ...
func (ki *KeyspaceInfo) UpdateServedFromMap(tabletType TabletType, cells []string, keyspace string, remove bool, allCells []string) error {
	// check parameters to be sure
	if err := ki.CheckServedFromMigration(tabletType, cells, keyspace, remove); err != nil {
		return err
	}

	if ki.ServedFromMap == nil {
		ki.ServedFromMap = make(map[TabletType]*KeyspaceServedFrom)
	}
	ksf, ok := ki.ServedFromMap[tabletType]
	if !ok {
		// the record doesn't exist
		if remove {
			if len(ki.ServedFromMap) == 0 {
				ki.ServedFromMap = nil
			}
			log.Warningf("Trying to remove KeyspaceServedFrom for missing type %v in keyspace %v", tabletType, ki.keyspace)
		} else {
			ki.ServedFromMap[tabletType] = &KeyspaceServedFrom{
				Cells:    cells,
				Keyspace: keyspace,
			}
		}
		return nil
	}

	if remove {
		result, emptyList := removeCells(ksf.Cells, cells, allCells)
		if emptyList {
			// we don't have any cell left, we need to clear this record
			delete(ki.ServedFromMap, tabletType)
			if len(ki.ServedFromMap) == 0 {
				ki.ServedFromMap = nil
			}
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

// ComputeCellServedFrom returns the ServedFrom map for a cell
func (ki *KeyspaceInfo) ComputeCellServedFrom(cell string) map[TabletType]string {
	result := make(map[TabletType]string)
	for tabletType, ksf := range ki.ServedFromMap {
		if InCellList(cell, ksf.Cells) {
			result[tabletType] = ksf.Keyspace
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// UpdateKeyspace updates the keyspace data, with the right version
func UpdateKeyspace(ts Server, ki *KeyspaceInfo) error {
	var version int64 = -1
	if ki.version != 0 {
		version = ki.version
	}

	newVersion, err := ts.UpdateKeyspace(ki, version)
	if err == nil {
		ki.version = newVersion
	}
	return err
}

// FindAllShardsInKeyspace reads and returns all the existing shards in
// a keyspace. It doesn't take any lock.
func FindAllShardsInKeyspace(ts Server, keyspace string) (map[string]*ShardInfo, error) {
	shards, err := ts.GetShardNames(keyspace)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*ShardInfo, len(shards))
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	rec := concurrency.FirstErrorRecorder{}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			si, err := ts.GetShard(keyspace, shard)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShard(%v,%v) failed: %v", keyspace, shard, err))
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
