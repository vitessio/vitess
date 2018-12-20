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
	"encoding/hex"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/events"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

// IsShardUsingRangeBasedSharding returns true if the shard name
// implies it is using range based sharding.
func IsShardUsingRangeBasedSharding(shard string) bool {
	return strings.Contains(shard, "-")
}

// ValidateShardName takes a shard name and sanitizes it, and also returns
// the KeyRange.
func ValidateShardName(shard string) (string, *topodatapb.KeyRange, error) {
	if !IsShardUsingRangeBasedSharding(shard) {
		return shard, nil, nil
	}

	parts := strings.Split(shard, "-")
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid shardId, can only contain one '-': %v", shard)
	}

	keyRange, err := key.ParseKeyRangeParts(parts[0], parts[1])
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
	version   Version
	*topodatapb.Shard
}

// NewShardInfo returns a ShardInfo basing on shard with the
// keyspace / shard. This function should be only used by Server
// implementations.
func NewShardInfo(keyspace, shard string, value *topodatapb.Shard, version Version) *ShardInfo {
	return &ShardInfo{
		keyspace:  keyspace,
		shardName: shard,
		version:   version,
		Shard:     value,
	}
}

// Keyspace returns the keyspace a shard belongs to.
func (si *ShardInfo) Keyspace() string {
	return si.keyspace
}

// ShardName returns the shard name for a shard.
func (si *ShardInfo) ShardName() string {
	return si.shardName
}

// Version returns the shard version from last time it was read or updated.
func (si *ShardInfo) Version() Version {
	return si.version
}

// HasMaster returns true if the Shard has an assigned Master.
func (si *ShardInfo) HasMaster() bool {
	return !topoproto.TabletAliasIsZero(si.Shard.MasterAlias)
}

// HasCell returns true if the cell is listed in the Cells for the shard.
func (si *ShardInfo) HasCell(cell string) bool {
	return topoproto.ShardHasCell(si.Shard, cell)
}

// GetShard is a high level function to read shard data.
// It generates trace spans.
func (ts *Server) GetShard(ctx context.Context, keyspace, shard string) (*ShardInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.GetShard")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	defer span.Finish()

	shardPath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardFile)
	data, version, err := ts.globalCell.Get(ctx, shardPath)
	if err != nil {
		return nil, err
	}

	value := &topodatapb.Shard{}
	if err = proto.Unmarshal(data, value); err != nil {
		return nil, fmt.Errorf("GetShard(%v,%v): bad shard data: %v", keyspace, shard, err)
	}
	return &ShardInfo{
		keyspace:  keyspace,
		shardName: shard,
		version:   version,
		Shard:     value,
	}, nil
}

// updateShard updates the shard data, with the right version.
// It also creates a span, and dispatches the event.
func (ts *Server) updateShard(ctx context.Context, si *ShardInfo) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateShard")
	span.Annotate("keyspace", si.keyspace)
	span.Annotate("shard", si.shardName)
	defer span.Finish()

	data, err := proto.Marshal(si.Shard)
	if err != nil {
		return err
	}
	shardPath := path.Join(KeyspacesPath, si.keyspace, ShardsPath, si.shardName, ShardFile)
	newVersion, err := ts.globalCell.Update(ctx, shardPath, data, si.version)
	if err != nil {
		return err
	}
	si.version = newVersion

	event.Dispatch(&events.ShardChange{
		KeyspaceName: si.Keyspace(),
		ShardName:    si.ShardName(),
		Shard:        si.Shard,
		Status:       "updated",
	})
	return nil
}

// UpdateShardFields is a high level helper to read a shard record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated ShardInfo.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
//
// Note the callback method takes a ShardInfo, so it can get the
// keyspace and shard from it, or use all the ShardInfo methods.
func (ts *Server) UpdateShardFields(ctx context.Context, keyspace, shard string, update func(*ShardInfo) error) (*ShardInfo, error) {
	for {
		si, err := ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, err
		}
		if err = update(si); err != nil {
			if IsErrType(err, NoUpdateNeeded) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.updateShard(ctx, si); !IsErrType(err, BadVersion) {
			return si, err
		}
	}
}

// CreateShard creates a new shard and tries to fill in the right information.
// This will lock the Keyspace, as we may be looking at other shard servedTypes.
// Using GetOrCreateShard is probably a better idea for most use cases.
func (ts *Server) CreateShard(ctx context.Context, keyspace, shard string) (err error) {
	// Lock the keyspace, because we'll be looking at ServedTypes.
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, keyspace, "CreateShard")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// validate parameters
	name, keyRange, err := ValidateShardName(shard)
	if err != nil {
		return err
	}

	// start the shard with all serving types. If it overlaps with
	// other shards for some serving types, remove them.
	servedTypes := map[topodatapb.TabletType]bool{
		topodatapb.TabletType_MASTER:  true,
		topodatapb.TabletType_REPLICA: true,
		topodatapb.TabletType_RDONLY:  true,
	}
	value := &topodatapb.Shard{
		KeyRange: keyRange,
	}

	if IsShardUsingRangeBasedSharding(name) {
		// if we are using range-based sharding, we don't want
		// overlapping shards to all serve and confuse the clients.
		sis, err := ts.FindAllShardsInKeyspace(ctx, keyspace)
		if err != nil && !IsErrType(err, NoNode) {
			return err
		}
		for _, si := range sis {
			if si.KeyRange == nil || key.KeyRangesIntersect(si.KeyRange, keyRange) {
				for _, st := range si.ServedTypes {
					delete(servedTypes, st.TabletType)
				}
			}
		}
	}

	for st := range servedTypes {
		value.ServedTypes = append(value.ServedTypes, &topodatapb.Shard_ServedType{
			TabletType: st,
		})
	}

	// Marshal and save.
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	shardPath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardFile)
	if _, err := ts.globalCell.Create(ctx, shardPath, data); err != nil {
		// Return error as is, we need to propagate
		// ErrNodeExists for instance.
		return err
	}

	event.Dispatch(&events.ShardChange{
		KeyspaceName: keyspace,
		ShardName:    shard,
		Shard:        value,
		Status:       "created",
	})
	return nil
}

// GetOrCreateShard will return the shard object, or create one if it doesn't
// already exist. Note the shard creation is protected by a keyspace Lock.
func (ts *Server) GetOrCreateShard(ctx context.Context, keyspace, shard string) (si *ShardInfo, err error) {
	si, err = ts.GetShard(ctx, keyspace, shard)
	if !IsErrType(err, NoNode) {
		return
	}

	// create the keyspace, maybe it already exists
	if err = ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil && !IsErrType(err, NodeExists) {
		return nil, fmt.Errorf("CreateKeyspace(%v) failed: %v", keyspace, err)
	}

	// now try to create with the lock, may already exist
	if err = ts.CreateShard(ctx, keyspace, shard); err != nil && !IsErrType(err, NodeExists) {
		return nil, fmt.Errorf("CreateShard(%v/%v) failed: %v", keyspace, shard, err)
	}

	// try to read the shard again, maybe someone created it
	// in between the original GetShard and the LockKeyspace
	return ts.GetShard(ctx, keyspace, shard)
}

// DeleteShard wraps the underlying conn.Delete
// and dispatches the event.
func (ts *Server) DeleteShard(ctx context.Context, keyspace, shard string) error {
	shardPath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardFile)
	if err := ts.globalCell.Delete(ctx, shardPath, nil); err != nil {
		return err
	}
	event.Dispatch(&events.ShardChange{
		KeyspaceName: keyspace,
		ShardName:    shard,
		Shard:        nil,
		Status:       "deleted",
	})
	return nil
}

// GetTabletControl returns the Shard_TabletControl for the given tablet type,
// or nil if it is not in the map.
func (si *ShardInfo) GetTabletControl(tabletType topodatapb.TabletType) *topodatapb.Shard_TabletControl {
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
//
// This function should be called while holding the keyspace lock.
func (si *ShardInfo) UpdateSourceBlacklistedTables(ctx context.Context, tabletType topodatapb.TabletType, cells []string, remove bool, tables []string) error {
	if err := CheckKeyspaceLocked(ctx, si.keyspace); err != nil {
		return err
	}
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
		si.TabletControls = append(si.TabletControls, &topodatapb.Shard_TabletControl{
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
// This function should be called while holding the keyspace lock.
func (si *ShardInfo) UpdateDisableQueryService(ctx context.Context, tabletType topodatapb.TabletType, cells []string, disableQueryService bool) error {
	if err := CheckKeyspaceLocked(ctx, si.keyspace); err != nil {
		return err
	}
	tc := si.GetTabletControl(tabletType)
	if tc == nil {
		// handle the case where the TabletControl object is new
		if disableQueryService {
			si.TabletControls = append(si.TabletControls, &topodatapb.Shard_TabletControl{
				TabletType:          tabletType,
				Cells:               cells,
				DisableQueryService: true,
				BlacklistedTables:   nil,
			})
		}
		return nil
	}

	// we have an existing record, check table list is empty and
	// DisableQueryService is set
	if len(tc.BlacklistedTables) > 0 {
		return fmt.Errorf("cannot safely alter DisableQueryService as BlacklistedTables is set")
	}
	if !tc.DisableQueryService {
		// This code is unreachable because we always delete the control record when we enable QueryService.
		return fmt.Errorf("cannot safely alter DisableQueryService as DisableQueryService is not set, this record should not be there for shard %v/%v", si.keyspace, si.shardName)
	}

	if disableQueryService {
		tc.Cells = addCells(tc.Cells, cells)
	} else {
		if tc.Frozen {
			return fmt.Errorf("migrate has gone past the point of no return, cannot re-enable serving for %v/%v", si.keyspace, si.shardName)
		}
		si.removeCellsFromTabletControl(tc, tabletType, cells)
	}
	return nil
}

func (si *ShardInfo) removeCellsFromTabletControl(tc *topodatapb.Shard_TabletControl, tabletType topodatapb.TabletType, cells []string) {
	result, emptyList := removeCells(tc.Cells, cells, si.Cells)
	if emptyList {
		// we don't have any cell left, we need to clear this record
		var tabletControls []*topodatapb.Shard_TabletControl
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
func (si *ShardInfo) GetServedType(tabletType topodatapb.TabletType) *topodatapb.Shard_ServedType {
	for _, st := range si.ServedTypes {
		if st.TabletType == tabletType {
			return st
		}
	}
	return nil
}

// GetServedTypesPerCell returns the list of types this shard is serving
// in the provided cell.
func (si *ShardInfo) GetServedTypesPerCell(cell string) []topodatapb.TabletType {
	result := make([]topodatapb.TabletType, 0, len(si.ServedTypes))
	for _, st := range si.ServedTypes {
		result = append(result, st.TabletType)
	}
	return result
}

// UpdateServedTypesMap handles ServedTypesMap. It can add or remove cells.
func (si *ShardInfo) UpdateServedTypesMap(tabletType topodatapb.TabletType, cells []string, remove bool) error {
	sst := si.GetServedType(tabletType)

	if remove {
		if sst == nil {
			// nothing to remove
			return nil
		}
		result, emptyList := removeCells(sst.Cells, cells, si.Cells)
		if emptyList {
			// we don't have any cell left, we need to clear this record
			var servedTypes []*topodatapb.Shard_ServedType
			for _, st := range si.ServedTypes {
				if st.TabletType != tabletType {
					servedTypes = append(servedTypes, st)
				}
			}
			si.ServedTypes = servedTypes
			return nil
		}
		sst.Cells = result
		return nil
	}

	// add
	if sst == nil {
		si.ServedTypes = append(si.ServedTypes, &topodatapb.Shard_ServedType{
			TabletType: tabletType,
			Cells:      cells,
		})
		return nil
	}
	sst.Cells = addCells(sst.Cells, cells)
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
func (ts *Server) FindAllTabletAliasesInShard(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error) {
	return ts.FindAllTabletAliasesInShardByCell(ctx, keyspace, shard, nil)
}

// FindAllTabletAliasesInShardByCell uses the replication graph to find all the
// tablet aliases in the given shard.
//
// It can return ErrPartialResult if some cells were not fetched,
// in which case the result only contains the cells that were fetched.
//
// The tablet aliases are sorted by cell, then by UID.
func (ts *Server) FindAllTabletAliasesInShardByCell(ctx context.Context, keyspace, shard string, cells []string) ([]*topodatapb.TabletAlias, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topo.FindAllTabletAliasesInShardbyCell")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("num_cells", len(cells))
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// read the shard information to find the cells
	si, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return nil, err
	}

	resultAsMap := make(map[string]*topodatapb.TabletAlias)
	if si.HasMaster() {
		if InCellList(si.MasterAlias.Cell, cells) {
			resultAsMap[topoproto.TabletAliasString(si.MasterAlias)] = si.MasterAlias
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
				resultAsMap[topoproto.TabletAliasString(node.TabletAlias)] = node.TabletAlias
			}
			mutex.Unlock()
		}(cell)
	}
	wg.Wait()
	err = nil
	if rec.HasErrors() {
		log.Warningf("FindAllTabletAliasesInShard(%v,%v): got partial result: %v", keyspace, shard, rec.Error())
		err = NewError(PartialResult, shard)
	}

	result := make([]*topodatapb.TabletAlias, 0, len(resultAsMap))
	for _, a := range resultAsMap {
		v := *a
		result = append(result, &v)
	}
	sort.Sort(topoproto.TabletAliasList(result))
	return result, err
}

// GetTabletMapForShard returns the tablets for a shard. It can return
// ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
// The map is indexed by topoproto.TabletAliasString(tablet alias).
func (ts *Server) GetTabletMapForShard(ctx context.Context, keyspace, shard string) (map[string]*TabletInfo, error) {
	return ts.GetTabletMapForShardByCell(ctx, keyspace, shard, nil)
}

// GetTabletMapForShardByCell returns the tablets for a shard. It can return
// ErrPartialResult if it couldn't read all the cells, or all
// the individual tablets, in which case the map is valid, but partial.
// The map is indexed by topoproto.TabletAliasString(tablet alias).
func (ts *Server) GetTabletMapForShardByCell(ctx context.Context, keyspace, shard string, cells []string) (map[string]*TabletInfo, error) {
	// if we get a partial result, we keep going. It most likely means
	// a cell is out of commission.
	aliases, err := ts.FindAllTabletAliasesInShardByCell(ctx, keyspace, shard, cells)
	if err != nil && !IsErrType(err, PartialResult) {
		return nil, err
	}

	// get the tablets for the cells we were able to reach, forward
	// ErrPartialResult from FindAllTabletAliasesInShard
	result, gerr := ts.GetTabletMap(ctx, aliases)
	if gerr == nil && err != nil {
		gerr = err
	}
	return result, gerr
}
