// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"encoding/hex"
	"fmt"
	"html/template"
	"reflect"
	"sort"
	"strings"
	"sync"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
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

// ParseKeyspaceShardString parse a "keyspace/shard" string and extract
// both keyspace and shard. It also returns empty keyspace and shard if
// input param looks like a old zk path
func ParseKeyspaceShardString(param string) (string, string, error) {
	if param[0] == '/' {
		return "", "", fmt.Errorf("Invalid keyspace/shard: %v, Note: old style zk path is no longer supported, please use a keyspace/shard instead", param)
	}
	keySpaceShard := strings.Split(param, "/")
	if len(keySpaceShard) != 2 {
		return "", "", fmt.Errorf("Invalid shard path: %v", param)
	}
	return keySpaceShard[0], keySpaceShard[1], nil
}

// SourceShardString returns a printable view of a SourceShard.
func SourceShardString(source *pb.Shard_SourceShard) string {
	return fmt.Sprintf("SourceShard(%v,%v/%v)", source.Uid, source.Keyspace, source.Shard)
}

// SourceShardAsHTML returns a HTML version of the object.
func SourceShardAsHTML(source *pb.Shard_SourceShard) template.HTML {
	result := fmt.Sprintf("<b>Uid</b>: %v</br>\n<b>Source</b>: %v/%v</br>\n", source.Uid, source.Keyspace, source.Shard)
	if key.KeyRangeIsPartial(source.KeyRange) {
		result += fmt.Sprintf("<b>KeyRange</b>: %v-%v</br>\n",
			hex.EncodeToString(source.KeyRange.Start),
			hex.EncodeToString(source.KeyRange.End))
	}
	if len(source.Tables) > 0 {
		result += fmt.Sprintf("<b>Tables</b>: %v</br>\n",
			strings.Join(source.Tables, " "))
	}
	return template.HTML(result)
}

// IsShardUsingRangeBasedSharding returns true if the shard name
// implies it is using range based sharding.
func IsShardUsingRangeBasedSharding(shard string) bool {
	return strings.Contains(shard, "-")
}

// ValidateShardName takes a shard name and sanitizes it, and also returns
// the KeyRange.
func ValidateShardName(shard string) (string, *pb.KeyRange, error) {
	if !IsShardUsingRangeBasedSharding(shard) {
		return shard, nil, nil
	}

	parts := strings.Split(shard, "-")
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid shardId, can only contain one '-': %v", shard)
	}

	keyRange, err := key.ParseKeyRangeParts3(parts[0], parts[1])
	if err != nil {
		return "", nil, err
	}

	if len(keyRange.End) > 0 && string(keyRange.Start) >= string(keyRange.End) {
		return "", nil, fmt.Errorf("out of order keys: %v is not strictly smaller than %v", hex.EncodeToString(keyRange.Start), hex.EncodeToString(keyRange.End))
	}

	return strings.ToLower(shard), keyRange, nil
}

// ShardInfo is a meta struct that contains metadata to give the data
// more context and convenience. This is the main way we interact with a shard.
type ShardInfo struct {
	keyspace  string
	shardName string
	version   int64
	*pb.Shard
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
func NewShardInfo(keyspace, shard string, value *pb.Shard, version int64) *ShardInfo {
	return &ShardInfo{
		keyspace:  keyspace,
		shardName: shard,
		version:   version,
		Shard:     value,
	}
}

// HasCell returns true if the cell is listed in the Cells for the shard.
func (si *ShardInfo) HasCell(cell string) bool {
	for _, c := range si.Cells {
		if c == cell {
			return true
		}
	}
	return false
}

// GetShard is a high level function to read shard data.
// It generates trace spans.
func GetShard(ctx context.Context, ts Server, keyspace, shard string) (*ShardInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.GetShard")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	return ts.GetShard(ctx, keyspace, shard)
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

	newVersion, err := ts.UpdateShard(ctx, si, version)
	if err == nil {
		si.version = newVersion
	}
	return err
}

// UpdateShardFields is a high level helper to read a shard record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated ShardInfo.
func UpdateShardFields(ctx context.Context, ts Server, keyspace, shard string, update func(*pb.Shard) error) (*ShardInfo, error) {
	for {
		si, err := GetShard(ctx, ts, keyspace, shard)
		if err != nil {
			return nil, err
		}
		if err = update(si.Shard); err != nil {
			return nil, err
		}
		if err = UpdateShard(ctx, ts, si); err != ErrBadVersion {
			return si, err
		}
	}
}

// CreateShard creates a new shard and tries to fill in the right information.
// This should be called while holding the keyspace lock for the shard.
// (call topotools.CreateShard to do that for you).
// In unit tests (that are not parallel), this function can be called directly.
func CreateShard(ctx context.Context, ts Server, keyspace, shard string) error {
	name, keyRange, err := ValidateShardName(shard)
	if err != nil {
		return err
	}

	// start the shard with all serving types. If it overlaps with
	// other shards for some serving types, remove them.
	servedTypes := map[pb.TabletType]bool{
		pb.TabletType_MASTER:  true,
		pb.TabletType_REPLICA: true,
		pb.TabletType_RDONLY:  true,
	}
	s := &pb.Shard{
		KeyRange: keyRange,
	}

	if IsShardUsingRangeBasedSharding(name) {
		// if we are using range-based sharding, we don't want
		// overlapping shards to all serve and confuse the clients.
		sis, err := FindAllShardsInKeyspace(ctx, ts, keyspace)
		if err != nil && err != ErrNoNode {
			return err
		}
		for _, si := range sis {
			if si.KeyRange == nil || key.KeyRangesIntersect3(si.KeyRange, keyRange) {
				for _, st := range si.ServedTypes {
					delete(servedTypes, st.TabletType)
				}
			}
		}
	}

	for st, _ := range servedTypes {
		s.ServedTypes = append(s.ServedTypes, &pb.Shard_ServedType{
			TabletType: st,
		})
	}

	return ts.CreateShard(ctx, keyspace, name, s)
}

// GetTabletControl returns the Shard_TabletControl for the given tablet type,
// or nil if it is not in the map.
func (si *ShardInfo) GetTabletControl(tabletType pb.TabletType) *pb.Shard_TabletControl {
	for _, tc := range si.TabletControls {
		if tc.TabletType == tabletType {
			return tc
		}
	}
	return nil
}

// UpdateSourceBlacklistedTables will add or remove the listed tables
// in the shard record's TabletControl structures. Note we don't
// support a lot of the corner cases:
// - only support one table list per shard. If we encounter a different
//   table list that the provided one, we error out.
// - we don't support DisableQueryService at the same time as BlacklistedTables,
//   because it's not used in the same context (vertical vs horizontal sharding)
func (si *ShardInfo) UpdateSourceBlacklistedTables(tabletType pb.TabletType, cells []string, remove bool, tables []string) error {
	tc := si.GetTabletControl(tabletType)
	if tc == nil {
		// handle the case where the TabletControl object is new
		if remove {
			// we try to remove from something that doesn't exist,
			// log, but we're done.
			log.Warningf("Trying to remove TabletControl.BlacklistedTables for missing type %v in shard %v/%v", tabletType, si.keyspace, si.shardName)
			return nil
		}

		// trying to add more constraints with no existing record
		si.TabletControls = append(si.TabletControls, &pb.Shard_TabletControl{
			TabletType:          tabletType,
			Cells:               cells,
			DisableQueryService: false,
			BlacklistedTables:   tables,
		})
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
func (si *ShardInfo) UpdateDisableQueryService(tabletType pb.TabletType, cells []string, disableQueryService bool) error {
	tc := si.GetTabletControl(tabletType)
	if tc == nil {
		// handle the case where the TabletControl object is new
		if disableQueryService {
			si.TabletControls = append(si.TabletControls, &pb.Shard_TabletControl{
				TabletType:          tabletType,
				Cells:               cells,
				DisableQueryService: true,
				BlacklistedTables:   nil,
			})
		} else {
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

func (si *ShardInfo) removeCellsFromTabletControl(tc *pb.Shard_TabletControl, tabletType pb.TabletType, cells []string) {
	result, emptyList := removeCells(tc.Cells, cells, si.Cells)
	if emptyList {
		// we don't have any cell left, we need to clear this record
		var tabletControls []*pb.Shard_TabletControl
		for _, tc := range si.TabletControls {
			if tc.TabletType != tabletType {
				tabletControls = append(tabletControls, tc)
			}
		}
		si.TabletControls = tabletControls
	} else {
		tc.Cells = result
	}
}

// GetServedType returns the Shard_ServedType for a TabletType, or nil
func (si *ShardInfo) GetServedType(tabletType pb.TabletType) *pb.Shard_ServedType {
	for _, st := range si.ServedTypes {
		if st.TabletType == tabletType {
			return st
		}
	}
	return nil
}

// GetServedTypesPerCell returns the list of types this shard is serving
// in the provided cell.
func (si *ShardInfo) GetServedTypesPerCell(cell string) []TabletType {
	result := make([]TabletType, 0, len(si.ServedTypes))
	for _, st := range si.ServedTypes {
		if InCellList(cell, st.Cells) {
			result = append(result, ProtoToTabletType(st.TabletType))
		}
	}
	return result
}

// CheckServedTypesMigration makes sure the provided migration is possible
func (si *ShardInfo) CheckServedTypesMigration(tabletType pb.TabletType, cells []string, remove bool) error {
	// master is a special case with a few extra checks
	if tabletType == pb.TabletType_MASTER {
		if len(cells) > 0 {
			return fmt.Errorf("cannot migrate only some cells for master in shard %v/%v", si.keyspace, si.shardName)
		}
		if remove && len(si.ServedTypes) > 1 {
			return fmt.Errorf("cannot migrate master away from %v/%v until everything else is migrated", si.keyspace, si.shardName)
		}
	}

	// we can't remove a type we don't have
	if si.GetServedType(tabletType) == nil && remove {
		return fmt.Errorf("supplied type %v cannot be migrated out of %#v", tabletType, si)
	}

	return nil
}

// UpdateServedTypesMap handles ServedTypesMap. It can add or remove
// records, cells, ...
func (si *ShardInfo) UpdateServedTypesMap(tabletType pb.TabletType, cells []string, remove bool) error {
	// check parameters to be sure
	if err := si.CheckServedTypesMigration(tabletType, cells, remove); err != nil {
		return err
	}

	sst := si.GetServedType(tabletType)
	if sst == nil {
		// the record doesn't exist
		if remove {
			log.Warningf("Trying to remove ShardServedType for missing type %v in shard %v/%v", tabletType, si.keyspace, si.shardName)
		} else {
			si.ServedTypes = append(si.ServedTypes, &pb.Shard_ServedType{
				TabletType: tabletType,
				Cells:      cells,
			})
		}
		return nil
	}

	if remove {
		result, emptyList := removeCells(sst.Cells, cells, si.Cells)
		if emptyList {
			// we don't have any cell left, we need to clear this record
			var servedTypes []*pb.Shard_ServedType
			for _, st := range si.ServedTypes {
				if st.TabletType != tabletType {
					servedTypes = append(servedTypes, st)
				}
			}
			si.ServedTypes = servedTypes
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
//
// It can return ErrPartialResult if some cells were not fetched,
// in which case the result only contains the cells that were fetched.
//
// The tablet aliases are sorted by cell, then by UID.
func FindAllTabletAliasesInShard(ctx context.Context, ts Server, keyspace, shard string) ([]TabletAlias, error) {
	return FindAllTabletAliasesInShardByCell(ctx, ts, keyspace, shard, nil)
}

// FindAllTabletAliasesInShardByCell uses the replication graph to find all the
// tablet aliases in the given shard.
//
// It can return ErrPartialResult if some cells were not fetched,
// in which case the result only contains the cells that were fetched.
//
// The tablet aliases are sorted by cell, then by UID.
func FindAllTabletAliasesInShardByCell(ctx context.Context, ts Server, keyspace, shard string, cells []string) ([]TabletAlias, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topo.FindAllTabletAliasesInShardbyCell")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("num_cells", len(cells))
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// read the shard information to find the cells
	si, err := GetShard(ctx, ts, keyspace, shard)
	if err != nil {
		return nil, err
	}

	resultAsMap := make(map[TabletAlias]bool)
	if si.MasterAlias != nil && !TabletAliasIsZero(si.MasterAlias) {
		if InCellList(si.MasterAlias.Cell, cells) {
			resultAsMap[ProtoToTabletAlias(si.MasterAlias)] = true
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
			sri, err := ts.GetShardReplication(ctx, cell, keyspace, shard)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err))
				return
			}

			mutex.Lock()
			for _, node := range sri.Nodes {
				resultAsMap[ProtoToTabletAlias(node.TabletAlias)] = true
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
	sort.Sort(TabletAliasList(result))
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
