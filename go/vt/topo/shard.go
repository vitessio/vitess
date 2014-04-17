// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"strings"
	"sync"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
)

// Functions for dealing with shard representations in topology.

// SourceShard represents a data source for filtered replication
// accross shards. When this is used in a destination shard, the master
// of that shard will run filtered replication.
type SourceShard struct {
	// Uid is the unique ID for this SourceShard object.
	// It is for instance used as a unique index in blp_checkpoint
	// when storing the position. It should be unique whithin a
	// destination Shard, but not globally unique.
	Uid uint32

	// the source keyspace
	Keyspace string

	// the source shard
	Shard string

	// The source shard keyrange
	// If partial, len(Tables) has to be zero
	KeyRange key.KeyRange

	// The source table list to replicate
	// If non-empty, KeyRange must not be partial (must be KeyRange{})
	Tables []string
}

func (source *SourceShard) String() string {
	return fmt.Sprintf("SourceShard(%v,%v/%v)", source.Uid, source.Keyspace, source.Shard)
}

// A pure data struct for information stored in topology server.  This
// node is used to present a controlled view of the shard, unaware of
// every management action. It also contains configuration data for a
// shard.
type Shard struct {
	// There can be only at most one master, but there may be none. (0)
	MasterAlias TabletAlias

	// This must match the shard name based on our other conventions, but
	// helpful to have it decomposed here.
	KeyRange key.KeyRange

	// ServedTypes is a list of all the tablet types this shard will
	// serve. This is usually used with overlapping shards during
	// data shuffles like shard splitting.
	ServedTypes []TabletType

	// SourceShards is the list of shards we're replicating from,
	// using filtered replication.
	SourceShards []SourceShard

	// Cells is the list of cells that have tablets for this shard.
	// It is populated at InitTablet time when a tabelt is added
	// in a cell that is not in the list yet.
	Cells []string
}

func newShard() *Shard {
	return &Shard{}
}

// ValidateShardName takes a shard name and sanitizes it, and also returns
// the KeyRange.
func ValidateShardName(shard string) (string, key.KeyRange, error) {
	if !strings.Contains(shard, "-") {
		return shard, key.KeyRange{}, nil
	}

	parts := strings.Split(shard, "-")
	if len(parts) != 2 {
		return "", key.KeyRange{}, fmt.Errorf("Invalid shardId, can only contain one '-': %v", shard)
	}

	keyRange, err := key.ParseKeyRangeParts(parts[0], parts[1])
	if err != nil {
		return "", key.KeyRange{}, err
	}

	if keyRange.End != key.MaxKey && keyRange.Start >= keyRange.End {
		return "", key.KeyRange{}, fmt.Errorf("Out of order keys: %v is not strictly smaller than %v", keyRange.Start.Hex(), keyRange.End.Hex())
	}

	return strings.ToUpper(shard), keyRange, nil
}

// HasCell returns true if the cell is listed in the Cells for the shard.
func (shard *Shard) HasCell(cell string) bool {
	for _, c := range shard.Cells {
		if c == cell {
			return true
		}
	}
	return false
}

// ShardInfo is a meta struct that contains metadata to give the data
// more context and convenience. This is the main way we interact with a shard.
type ShardInfo struct {
	keyspace  string
	shardName string
	*Shard
}

// Keyspace returns the keyspace a shard belongs to
func (si *ShardInfo) Keyspace() string {
	return si.keyspace
}

// ShardName returns the shard name for a shard
func (si *ShardInfo) ShardName() string {
	return si.shardName
}

// NewShardInfo returns a ShardInfo basing on shard with the
// keyspace / shard. This function should be only used by Server
// implementations.
func NewShardInfo(keyspace, shard string, value *Shard) *ShardInfo {
	return &ShardInfo{
		keyspace:  keyspace,
		shardName: shard,
		Shard:     value,
	}
}

// CreateShard creates a new shard and tries to fill in the right information.
func CreateShard(ts Server, keyspace, shard string) error {

	name, keyRange, err := ValidateShardName(shard)
	if err != nil {
		return err
	}
	s := &Shard{KeyRange: keyRange}

	// start the shard with all serving types. If it overlaps with
	// other shards for some serving types, remove them.
	servingTypes := map[TabletType]bool{
		TYPE_MASTER:  true,
		TYPE_REPLICA: true,
		TYPE_RDONLY:  true,
	}
	sis, err := FindAllShardsInKeyspace(ts, keyspace)
	if err != nil && err != ErrNoNode {
		return err
	}
	for _, si := range sis {
		if key.KeyRangesIntersect(si.KeyRange, keyRange) {
			for _, t := range si.ServedTypes {
				delete(servingTypes, t)
			}
		}
	}
	s.ServedTypes = make([]TabletType, 0, len(servingTypes))
	for st := range servingTypes {
		s.ServedTypes = append(s.ServedTypes, st)
	}

	return ts.CreateShard(keyspace, name, s)
}

// InCellList returns true if the cell list is empty,
// or if the passed cell is in the cell list.
func InCellList(cell string, cells []string) bool {
	if len(cells) == 0 {
		return true
	}
	for _, c := range cells {
		if c == cell {
			return true
		}
	}
	return false
}

// FindAllTabletAliasesInShard uses the replication graph to find all the
// tablet aliases in the given shard.
// It can return ErrPartialResult if some cells were not fetched,
// in which case the result only contains the cells that were fetched.
func FindAllTabletAliasesInShard(ts Server, keyspace, shard string) ([]TabletAlias, error) {
	return FindAllTabletAliasesInShardByCell(ts, keyspace, shard, nil)
}

// FindAllTabletAliasesInShard uses the replication graph to find all the
// tablet aliases in the given shard.
// It can return ErrPartialResult if some cells were not fetched,
// in which case the result only contains the cells that were fetched.
func FindAllTabletAliasesInShardByCell(ts Server, keyspace, shard string, cells []string) ([]TabletAlias, error) {
	// read the shard information to find the cells
	si, err := ts.GetShard(keyspace, shard)
	if err != nil {
		return nil, err
	}

	resultAsMap := make(map[TabletAlias]bool)
	if !si.MasterAlias.IsZero() {
		if InCellList(si.MasterAlias.Cell, cells) {
			resultAsMap[si.MasterAlias] = true
		}
	}

	// read the replication graph in each cell and add all found tablets
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range si.Cells {
		if !InCellList(cell, cells) {
			continue
		}
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			sri, err := ts.GetShardReplication(cell, keyspace, shard)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err))
				return
			}

			mutex.Lock()
			for _, rl := range sri.ReplicationLinks {
				resultAsMap[rl.TabletAlias] = true
				if !rl.Parent.IsZero() && InCellList(rl.Parent.Cell, cells) {
					resultAsMap[rl.Parent] = true
				}
			}
			mutex.Unlock()
		}(cell)
	}
	wg.Wait()
	err = nil
	if rec.HasErrors() {
		log.Warningf("FindAllTabletAliasesInShard(%v,%v): got partial result: %v", keyspace, shard, rec.Error())
		err = ErrPartialResult
	}

	result := make([]TabletAlias, 0, len(resultAsMap))
	for a := range resultAsMap {
		result = append(result, a)
	}
	return result, err
}

// GetTabletMapForShard returns the tablets for a shard. It can return
// ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
func GetTabletMapForShard(ts Server, keyspace, shard string) (map[TabletAlias]*TabletInfo, error) {
	return GetTabletMapForShardByCell(ts, keyspace, shard, nil)
}

// GetTabletMapForShardByCell returns the tablets for a shard. It can return
// ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
func GetTabletMapForShardByCell(ts Server, keyspace, shard string, cells []string) (map[TabletAlias]*TabletInfo, error) {
	// if we get a partial result, we keep going. It most likely means
	// a cell is out of commission.
	aliases, err := FindAllTabletAliasesInShardByCell(ts, keyspace, shard, cells)
	if err != nil && err != ErrPartialResult {
		return nil, err
	}

	// get the tablets for the cells we were able to reach, forward
	// ErrPartialResult from FindAllTabletAliasesInShard
	result, gerr := GetTabletMap(ts, aliases)
	if gerr == nil && err != nil {
		gerr = err
	}
	return result, gerr
}
