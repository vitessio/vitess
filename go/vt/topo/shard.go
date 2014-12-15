// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"html/template"
	"reflect"
	"strings"
	"sync"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
)

// Functions for dealing with shard representations in topology.

// addCells will merge both cells list, settling on nil if either list is empty
func addCells(left, right []string) []string {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}

	for _, cell := range right {
		if !InCellList(cell, left) {
			left = append(left, cell)
		}
	}
	return left
}

// removeCells will remove the cells from the provided list. It returns
// the new list, and a boolean that indicates the returned list is empty.
func removeCells(cells, toRemove, fullList []string) ([]string, bool) {
	// The assumption here is we already migrated something,
	// and we're reverting that part. So we're gonna remove
	// records only.
	leftoverCells := make([]string, 0, len(cells))
	if len(cells) == 0 {
		// we migrated all the cells already, take the full list
		// and remove all the ones we're not reverting
		for _, cell := range fullList {
			if !InCellList(cell, toRemove) {
				leftoverCells = append(leftoverCells, cell)
			}
		}
	} else {
		// we migrated a subset of the cells,
		// remove the ones we're reverting
		for _, cell := range cells {
			if !InCellList(cell, toRemove) {
				leftoverCells = append(leftoverCells, cell)
			}
		}
	}

	if len(leftoverCells) == 0 {
		// we don't have any cell left, we need to clear this record
		return nil, true
	}

	return leftoverCells, false
}

// SourceShard represents a data source for filtered replication
// across shards. When this is used in a destination shard, the master
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

// String returns a printable view of a SourceShard.
func (source *SourceShard) String() string {
	return fmt.Sprintf("SourceShard(%v,%v/%v)", source.Uid, source.Keyspace, source.Shard)
}

// AsHTML returns a HTML version of the object.
func (source *SourceShard) AsHTML() template.HTML {
	result := fmt.Sprintf("<b>Uid</b>: %v</br>\n<b>Source</b>: %v/%v</br>\n", source.Uid, source.Keyspace, source.Shard)
	if source.KeyRange.IsPartial() {
		result += fmt.Sprintf("<b>KeyRange</b>: %v-%v</br>\n",
			source.KeyRange.Start.Hex(), source.KeyRange.End.Hex())
	}
	if len(source.Tables) > 0 {
		result += fmt.Sprintf("<b>Tables</b>: %v</br>\n",
			strings.Join(source.Tables, " "))
	}
	return template.HTML(result)
}

// TabletControl describes the parameters used by the vttablet processes
// to know what specific configurations they should be using.
type TabletControl struct {
	// How to match the tablets
	Cells []string // nil means all cells

	// What specific action to take
	DisableQueryService bool
	BlacklistedTables   []string // only used if DisableQueryService==false
}

// ShardServedType describes the cells where the given shard is serving.
type ShardServedType struct {
	Cells []string // nil means all cells
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

	// ServedTypesMap is a map of all the tablet types this shard
	// will serve, to the cells that serve this type. This is
	// usually used with overlapping shards during data shuffles
	// like shard splitting. Note the master record will always
	// list all the cells.
	ServedTypesMap map[TabletType]*ShardServedType

	// SourceShards is the list of shards we're replicating from,
	// using filtered replication.
	SourceShards []SourceShard

	// Cells is the list of cells that have tablets for this shard.
	// It is populated at InitTablet time when a tablet is added
	// in a cell that is not in the list yet.
	Cells []string

	// TabletControlMap is a map of TabletControl to apply specific
	// configurations to tablets by type.
	TabletControlMap map[TabletType]*TabletControl
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
		return "", key.KeyRange{}, fmt.Errorf("invalid shardId, can only contain one '-': %v", shard)
	}

	keyRange, err := key.ParseKeyRangeParts(parts[0], parts[1])
	if err != nil {
		return "", key.KeyRange{}, err
	}

	if keyRange.End != key.MaxKey && keyRange.Start >= keyRange.End {
		return "", key.KeyRange{}, fmt.Errorf("out of order keys: %v is not strictly smaller than %v", keyRange.Start.Hex(), keyRange.End.Hex())
	}

	return strings.ToLower(shard), keyRange, nil
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
	version   int64
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

// Version returns the shard version from last time it was read or updated.
func (si *ShardInfo) Version() int64 {
	return si.version
}

// NewShardInfo returns a ShardInfo basing on shard with the
// keyspace / shard. This function should be only used by Server
// implementations.
func NewShardInfo(keyspace, shard string, value *Shard, version int64) *ShardInfo {
	return &ShardInfo{
		keyspace:  keyspace,
		shardName: shard,
		version:   version,
		Shard:     value,
	}
}

// UpdateShard updates the shard data, with the right version
func UpdateShard(ctx context.Context, ts Server, si *ShardInfo) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateShard")
	span.Annotate("keyspace", si.Keyspace())
	span.Annotate("shard", si.ShardName())
	defer span.Finish()

	var version int64 = -1
	if si.version != 0 {
		version = si.version
	}

	newVersion, err := ts.UpdateShard(si, version)
	if err == nil {
		si.version = newVersion
	}
	return err
}

// CreateShard creates a new shard and tries to fill in the right information.
func CreateShard(ts Server, keyspace, shard string) error {

	name, keyRange, err := ValidateShardName(shard)
	if err != nil {
		return err
	}

	// start the shard with all serving types. If it overlaps with
	// other shards for some serving types, remove them.
	s := &Shard{
		KeyRange: keyRange,
		ServedTypesMap: map[TabletType]*ShardServedType{
			TYPE_MASTER:  &ShardServedType{},
			TYPE_REPLICA: &ShardServedType{},
			TYPE_RDONLY:  &ShardServedType{},
		},
	}

	sis, err := FindAllShardsInKeyspace(ts, keyspace)
	if err != nil && err != ErrNoNode {
		return err
	}
	for _, si := range sis {
		if key.KeyRangesIntersect(si.KeyRange, keyRange) {
			for t, _ := range si.ServedTypesMap {
				delete(s.ServedTypesMap, t)
			}
		}
	}
	if len(s.ServedTypesMap) == 0 {
		s.ServedTypesMap = nil
	}

	return ts.CreateShard(keyspace, name, s)
}

// UpdateSourceBlacklistedTables will add or remove the listed tables
// in the shard record's TabletControl structures. Note we don't
// support a lot of the corner cases:
// - only support one table list per shard. If we encounter a different
//   table list that the provided one, we error out.
// - we don't support DisableQueryService at the same time as BlacklistedTables,
//   because it's not used in the same context (vertical vs horizontal sharding)
func (si *ShardInfo) UpdateSourceBlacklistedTables(tabletType TabletType, cells []string, remove bool, tables []string) error {
	if si.TabletControlMap == nil {
		si.TabletControlMap = make(map[TabletType]*TabletControl)
	}
	tc, ok := si.TabletControlMap[tabletType]
	if !ok {
		// handle the case where the TabletControl object is new
		if remove {
			if len(si.TabletControlMap) == 0 {
				si.TabletControlMap = nil
			}
			// we try to remove from something that doesn't exist,
			// log, but we're done.
			log.Warningf("Trying to remove TabletControl.BlacklistedTables for missing type %v in shard %v/%v", tabletType, si.keyspace, si.shardName)
			return nil
		}

		// trying to add more constraints with no existing record
		si.TabletControlMap[tabletType] = &TabletControl{
			Cells:               cells,
			DisableQueryService: false,
			BlacklistedTables:   tables,
		}
		return nil
	}

	// we have an existing record, check table lists matches and
	// DisableQueryService is not set
	if tc.DisableQueryService {
		return fmt.Errorf("cannot safely alter BlacklistedTables as DisableQueryService is set for shard %v/%v", si.keyspace, si.shardName)
	}

	if remove {
		si.removeCellsFromTabletControl(tc, tabletType, cells)
	} else {
		if !reflect.DeepEqual(tc.BlacklistedTables, tables) {
			return fmt.Errorf("trying to use two different sets of blacklisted tables for shard %v/%v: %v and %v", si.keyspace, si.shardName, tc.BlacklistedTables, tables)
		}

		tc.Cells = addCells(tc.Cells, cells)
	}
	return nil
}

// UpdateDisableQueryService will make sure the disableQueryService is
// set appropriately in the shard record. Note we don't support a lot
// of the corner cases:
// - we don't support DisableQueryService at the same time as BlacklistedTables,
//   because it's not used in the same context (vertical vs horizontal sharding)
func (si *ShardInfo) UpdateDisableQueryService(tabletType TabletType, cells []string, disableQueryService bool) error {
	if si.TabletControlMap == nil {
		si.TabletControlMap = make(map[TabletType]*TabletControl)
	}
	tc, ok := si.TabletControlMap[tabletType]
	if !ok {
		// handle the case where the TabletControl object is new
		if disableQueryService {
			si.TabletControlMap[tabletType] = &TabletControl{
				Cells:               cells,
				DisableQueryService: true,
				BlacklistedTables:   nil,
			}
		} else {
			if len(si.TabletControlMap) == 0 {
				si.TabletControlMap = nil
			}
			log.Warningf("Trying to remove TabletControl.DisableQueryService for missing type: %v", tabletType)
		}
		return nil
	}

	// we have an existing record, check table list is empty and
	// DisableQueryService is set
	if len(tc.BlacklistedTables) > 0 {
		return fmt.Errorf("cannot safely alter DisableQueryService as BlacklistedTables is set")
	}
	if !tc.DisableQueryService {
		return fmt.Errorf("cannot safely alter DisableQueryService as DisableQueryService is not set, this record should not be there")
	}

	if disableQueryService {
		tc.Cells = addCells(tc.Cells, cells)
	} else {
		si.removeCellsFromTabletControl(tc, tabletType, cells)
	}
	return nil
}

func (si *ShardInfo) removeCellsFromTabletControl(tc *TabletControl, tabletType TabletType, cells []string) {
	result, emptyList := removeCells(tc.Cells, cells, si.Cells)
	if emptyList {
		// we don't have any cell left, we need to clear this record
		delete(si.TabletControlMap, tabletType)
		if len(si.TabletControlMap) == 0 {
			si.TabletControlMap = nil
		}
	} else {
		tc.Cells = result
	}
}

// GetServedTypesPerCell returns the list of types this shard is serving
// in the provided cell.
func (si *ShardInfo) GetServedTypesPerCell(cell string) []TabletType {
	result := make([]TabletType, 0, len(si.ServedTypesMap))
	for tt, sst := range si.ServedTypesMap {
		if InCellList(cell, sst.Cells) {
			result = append(result, tt)
		}
	}
	return result
}

// CheckServedTypesMigration makes sure the provided migration is possible
func (si *ShardInfo) CheckServedTypesMigration(tabletType TabletType, cells []string, remove bool) error {
	// master is a special case with a few extra checks
	if tabletType == TYPE_MASTER {
		if len(cells) > 0 {
			return fmt.Errorf("cannot migrate only some cells for master in shard %v/%v", si.keyspace, si.shardName)
		}
		if remove && len(si.ServedTypesMap) > 1 {
			return fmt.Errorf("cannot migrate master away from %v/%v until everything else is migrated", si.keyspace, si.shardName)
		}
	}

	// we can't remove a type we don't have
	if _, ok := si.ServedTypesMap[tabletType]; !ok && remove {
		return fmt.Errorf("supplied type cannot be migrated")
	}

	return nil
}

// UpdateServedTypesMap handles ServedTypesMap. It can add or remove
// records, cells, ...
func (si *ShardInfo) UpdateServedTypesMap(tabletType TabletType, cells []string, remove bool) error {
	// check parameters to be sure
	if err := si.CheckServedTypesMigration(tabletType, cells, remove); err != nil {
		return err
	}

	if si.ServedTypesMap == nil {
		si.ServedTypesMap = make(map[TabletType]*ShardServedType)
	}
	sst, ok := si.ServedTypesMap[tabletType]
	if !ok {
		// the record doesn't exist
		if remove {
			if len(si.ServedTypesMap) == 0 {
				si.ServedTypesMap = nil
			}
			log.Warningf("Trying to remove ShardServedType for missing type %v in shard %v/%v", tabletType, si.keyspace, si.shardName)
		} else {
			si.ServedTypesMap[tabletType] = &ShardServedType{
				Cells: cells,
			}
		}
		return nil
	}

	if remove {
		result, emptyList := removeCells(sst.Cells, cells, si.Cells)
		if emptyList {
			// we don't have any cell left, we need to clear this record
			delete(si.ServedTypesMap, tabletType)
			if len(si.ServedTypesMap) == 0 {
				si.ServedTypesMap = nil
			}
		} else {
			sst.Cells = result
		}
	} else {
		sst.Cells = addCells(sst.Cells, cells)
	}
	return nil
}

//
// Utility functions for shards
//

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
func FindAllTabletAliasesInShard(ctx context.Context, ts Server, keyspace, shard string) ([]TabletAlias, error) {
	return FindAllTabletAliasesInShardByCell(ctx, ts, keyspace, shard, nil)
}

// FindAllTabletAliasesInShard uses the replication graph to find all the
// tablet aliases in the given shard.
// It can return ErrPartialResult if some cells were not fetched,
// in which case the result only contains the cells that were fetched.
func FindAllTabletAliasesInShardByCell(ctx context.Context, ts Server, keyspace, shard string, cells []string) ([]TabletAlias, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topo.FindAllTabletAliasesInShardbyCell")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("num_cells", len(cells))
	defer span.Finish()

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
func GetTabletMapForShard(ctx context.Context, ts Server, keyspace, shard string) (map[TabletAlias]*TabletInfo, error) {
	return GetTabletMapForShardByCell(ctx, ts, keyspace, shard, nil)
}

// GetTabletMapForShardByCell returns the tablets for a shard. It can return
// ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
func GetTabletMapForShardByCell(ctx context.Context, ts Server, keyspace, shard string, cells []string) (map[TabletAlias]*TabletInfo, error) {
	// if we get a partial result, we keep going. It most likely means
	// a cell is out of commission.
	aliases, err := FindAllTabletAliasesInShardByCell(ctx, ts, keyspace, shard, cells)
	if err != nil && err != ErrPartialResult {
		return nil, err
	}

	// get the tablets for the cells we were able to reach, forward
	// ErrPartialResult from FindAllTabletAliasesInShard
	result, gerr := GetTabletMap(ctx, ts, aliases)
	if gerr == nil && err != nil {
		gerr = err
	}
	return result, gerr
}
