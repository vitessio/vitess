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
	"encoding/hex"
	"fmt"
	"path"
	"sync"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the utility methods to manage SrvKeyspace objects.

func srvKeyspaceFileName(keyspace string) string {
	return path.Join(KeyspacesPath, keyspace, SrvKeyspaceFile)
}

// WatchSrvKeyspaceData is returned / streamed by WatchSrvKeyspace.
// The WatchSrvKeyspace API guarantees exactly one of Value or Err will be set.
type WatchSrvKeyspaceData struct {
	Value *topodatapb.SrvKeyspace
	Err   error
}

// WatchSrvKeyspace will set a watch on the SrvKeyspace object.
// It has the same contract as Conn.Watch, but it also unpacks the
// contents into a SrvKeyspace object.
func (ts *Server) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (*WatchSrvKeyspaceData, <-chan *WatchSrvKeyspaceData, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return &WatchSrvKeyspaceData{Err: err}, nil, nil
	}

	filePath := srvKeyspaceFileName(keyspace)
	ctx, cancel := context.WithCancel(ctx)
	current, wdChannel, err := conn.Watch(ctx, filePath)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	value := &topodatapb.SrvKeyspace{}
	if err := value.UnmarshalVT(current.Contents); err != nil {
		// Cancel the watch, drain channel.
		cancel()
		for range wdChannel {
		}
		return nil, nil, vterrors.Wrapf(err, "error unpacking initial SrvKeyspace object")
	}

	changes := make(chan *WatchSrvKeyspaceData, 10)

	// The background routine reads any event from the watch channel,
	// translates it, and sends it to the caller.
	// If cancel() is called, the underlying Watch() code will
	// send an ErrInterrupted and then close the channel. We'll
	// just propagate that back to our caller.
	go func() {
		defer cancel()
		defer close(changes)

		for wd := range wdChannel {
			if wd.Err != nil {
				// Last error value, we're done.
				// wdChannel will be closed right after
				// this, no need to do anything.
				changes <- &WatchSrvKeyspaceData{Err: wd.Err}
				return
			}

			value := &topodatapb.SrvKeyspace{}
			if err := value.UnmarshalVT(wd.Contents); err != nil {
				cancel()
				for range wdChannel {
				}
				changes <- &WatchSrvKeyspaceData{Err: vterrors.Wrapf(err, "error unpacking SrvKeyspace object")}
				return
			}

			changes <- &WatchSrvKeyspaceData{Value: value}
		}
	}()

	return &WatchSrvKeyspaceData{Value: value}, changes, nil
}

// GetSrvKeyspaceNames returns the SrvKeyspace objects for a cell.
func (ts *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	children, err := conn.ListDir(ctx, KeyspacesPath, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return nil, nil
	default:
		return nil, err
	}
}

// GetShardServingCells returns cells where this shard is serving
func (ts *Server) GetShardServingCells(ctx context.Context, si *ShardInfo) (servingCells []string, err error) {
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	servingCells = make([]string, 0)
	var mu sync.Mutex
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, si.keyspace)
			switch {
			case err == nil:
				for _, partition := range srvKeyspace.GetPartitions() {
					for _, shardReference := range partition.ShardReferences {
						if shardReference.GetName() == si.ShardName() {
							func() {
								mu.Lock()
								defer mu.Unlock()
								// Check that this cell hasn't been added already
								for _, servingCell := range servingCells {
									if servingCell == cell {
										return
									}
								}
								servingCells = append(servingCells, cell)
							}()
						}
					}
				}
			case IsErrType(err, NoNode):
				// NOOP
				return
			default:
				rec.RecordError(err)
				return
			}
		}(cell, si.Keyspace())
	}
	wg.Wait()
	if rec.HasErrors() {
		return nil, NewError(PartialResult, rec.Error().Error())
	}
	return servingCells, nil
}

// GetShardServingTypes returns served types for given shard across all cells
func (ts *Server) GetShardServingTypes(ctx context.Context, si *ShardInfo) (servingTypes []topodatapb.TabletType, err error) {
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	servingTypes = make([]topodatapb.TabletType, 0)
	var mu sync.Mutex
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, si.keyspace)
			switch {
			case err == nil:
				func() {
					mu.Lock()
					defer mu.Unlock()
					for _, partition := range srvKeyspace.GetPartitions() {
						partitionAlreadyAdded := false
						for _, servingType := range servingTypes {
							if servingType == partition.ServedType {
								partitionAlreadyAdded = true
								break
							}
						}

						if !partitionAlreadyAdded {
							for _, shardReference := range partition.ShardReferences {
								if shardReference.GetName() == si.ShardName() {
									servingTypes = append(servingTypes, partition.ServedType)
									break
								}
							}
						}

					}
				}()
			case IsErrType(err, NoNode):
				// NOOP
				return
			default:
				rec.RecordError(err)
				return
			}
		}(cell, si.Keyspace())
	}
	wg.Wait()
	if rec.HasErrors() {
		return nil, NewError(PartialResult, rec.Error().Error())
	}
	return servingTypes, nil
}

// AddSrvKeyspacePartitions adds partitions to srvKeyspace
func (ts *Server) AddSrvKeyspacePartitions(ctx context.Context, keyspace string, shards []*ShardInfo, tabletType topodatapb.TabletType, cells []string) (err error) {
	if err = CheckKeyspaceLocked(ctx, keyspace); err != nil {
		return err
	}

	// The caller intents to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			switch {
			case err == nil:
				partitionFound := false

				for _, partition := range srvKeyspace.GetPartitions() {
					if partition.GetServedType() != tabletType {
						continue
					}
					partitionFound = true

					for _, si := range shards {
						found := false
						for _, shardReference := range partition.GetShardReferences() {
							if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
								found = true
							}
						}

						if !found {
							shardReference := &topodatapb.ShardReference{
								Name:     si.ShardName(),
								KeyRange: si.KeyRange,
							}
							partition.ShardReferences = append(partition.GetShardReferences(), shardReference)
						}
					}
				}

				// Partition does not exist at all, we need to create it
				if !partitionFound {

					partition := &topodatapb.SrvKeyspace_KeyspacePartition{
						ServedType: tabletType,
					}

					shardReferences := make([]*topodatapb.ShardReference, 0)
					for _, si := range shards {
						shardReference := &topodatapb.ShardReference{
							Name:     si.ShardName(),
							KeyRange: si.KeyRange,
						}
						shardReferences = append(shardReferences, shardReference)
					}

					partition.ShardReferences = shardReferences

					srvKeyspace.Partitions = append(srvKeyspace.GetPartitions(), partition)
				}

				err = ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
				if err != nil {
					rec.RecordError(err)
					return
				}
			case IsErrType(err, NoNode):
				// NOOP
			default:
				rec.RecordError(err)
				return
			}
		}(cell)
	}
	wg.Wait()
	if rec.HasErrors() {
		return NewError(PartialResult, rec.Error().Error())
	}
	return nil
}

// DeleteSrvKeyspacePartitions deletes shards from srvKeyspace partitions
func (ts *Server) DeleteSrvKeyspacePartitions(ctx context.Context, keyspace string, shards []*ShardInfo, tabletType topodatapb.TabletType, cells []string) (err error) {
	if err = CheckKeyspaceLocked(ctx, keyspace); err != nil {
		return err
	}

	// The caller intents to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			switch {
			case err == nil:
				for _, partition := range srvKeyspace.GetPartitions() {
					if partition.GetServedType() != tabletType {
						continue
					}

					for _, si := range shards {
						found := false
						for _, shardReference := range partition.GetShardReferences() {
							// Use shard name rather than key range so it works
							// for both range-based and non-range-based shards.
							if shardReference.GetName() == si.ShardName() {
								found = true
							}
						}

						if found {
							shardReferences := make([]*topodatapb.ShardReference, 0)
							for _, shardReference := range partition.GetShardReferences() {
								// Use shard name rather than key range so it works
								// for both range-based and non-range-based shards.
								if shardReference.GetName() != si.ShardName() {
									shardReferences = append(shardReferences, shardReference)
								}
							}
							partition.ShardReferences = shardReferences
						}
					}
				}

				err = ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
				if err != nil {
					rec.RecordError(err)
					return
				}
			case IsErrType(err, NoNode):
				// NOOP
			default:
				rec.RecordError(err)
				return
			}
		}(cell)
	}
	wg.Wait()
	if rec.HasErrors() {
		return NewError(PartialResult, rec.Error().Error())
	}
	return nil
}

// UpdateSrvKeyspaceThrottlerConfig updates existing throttler configuration
func (ts *Server) UpdateSrvKeyspaceThrottlerConfig(ctx context.Context, keyspace string, cells []string, update func(throttlerConfig *topodatapb.ThrottlerConfig) *topodatapb.ThrottlerConfig) (updatedCells []string, err error) {
	if err = CheckKeyspaceLocked(ctx, keyspace); err != nil {
		return updatedCells, err
	}

	// The caller intends to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return updatedCells, err
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			switch {
			case err == nil:
				srvKeyspace.ThrottlerConfig = update(srvKeyspace.ThrottlerConfig)
				if err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace); err != nil {
					rec.RecordError(err)
					return
				}
				updatedCells = append(updatedCells, cell)
				return
			case IsErrType(err, NoNode):
				// NOOP as not every cell will contain a serving tablet in the keyspace
			default:
				rec.RecordError(err)
				return
			}
		}(cell)
	}
	wg.Wait()
	if rec.HasErrors() {
		return updatedCells, NewError(PartialResult, rec.Error().Error())
	}
	return updatedCells, nil
}

// UpdateDisableQueryService will make sure the disableQueryService is
// set appropriately in tablet controls in srvKeyspace.
func (ts *Server) UpdateDisableQueryService(ctx context.Context, keyspace string, shards []*ShardInfo, tabletType topodatapb.TabletType, cells []string, disableQueryService bool) (err error) {
	if err = CheckKeyspaceLocked(ctx, keyspace); err != nil {
		return err
	}

	// The caller intends to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	for _, shard := range shards {
		for _, tc := range shard.TabletControls {
			if len(tc.DeniedTables) > 0 {
				return fmt.Errorf("cannot safely alter DisableQueryService as DeniedTables is set for shard %v", shard)
			}
		}
	}

	if !disableQueryService {
		for _, si := range shards {
			tc := si.GetTabletControl(tabletType)
			if tc == nil {
				continue
			}
			if tc.Frozen {
				return fmt.Errorf("migrate has gone past the point of no return, cannot re-enable serving for %v/%v", si.keyspace, si.shardName)
			}
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			switch {
			case err == nil:
				for _, partition := range srvKeyspace.GetPartitions() {
					if partition.GetServedType() != tabletType {
						continue
					}

					for _, si := range shards {
						found := false
						for _, tabletControl := range partition.GetShardTabletControls() {
							if key.KeyRangeEqual(tabletControl.GetKeyRange(), si.GetKeyRange()) {
								found = true
								tabletControl.QueryServiceDisabled = disableQueryService
							}
						}

						if !found {
							shardTabletControl := &topodatapb.ShardTabletControl{
								Name:                 si.ShardName(),
								KeyRange:             si.KeyRange,
								QueryServiceDisabled: disableQueryService,
							}
							partition.ShardTabletControls = append(partition.GetShardTabletControls(), shardTabletControl)
						}
					}
				}

				err = ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
				if err != nil {
					rec.RecordError(err)
					return
				}
			case IsErrType(err, NoNode):
				// NOOP
			default:
				rec.RecordError(err)
				return
			}
		}(cell)
	}
	wg.Wait()
	if rec.HasErrors() {
		return NewError(PartialResult, rec.Error().Error())
	}
	return nil
}

// MigrateServedType removes/adds shards from srvKeyspace when migrating a served type.
func (ts *Server) MigrateServedType(ctx context.Context, keyspace string, shardsToAdd, shardsToRemove []*ShardInfo, tabletType topodatapb.TabletType, cells []string) (err error) {
	if err = CheckKeyspaceLocked(ctx, keyspace); err != nil {
		return err
	}

	// The caller intents to update all cells in this case
	if len(cells) == 0 {
		cells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return err
		}
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell, keyspace string) {
			defer wg.Done()
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			switch {
			case err == nil:
				for _, partition := range srvKeyspace.GetPartitions() {

					// We are finishing the migration, cleaning up tablet controls from the srvKeyspace
					if tabletType == topodatapb.TabletType_PRIMARY {
						partition.ShardTabletControls = nil
					}

					if partition.GetServedType() != tabletType {
						continue
					}

					shardReferences := make([]*topodatapb.ShardReference, 0)

					for _, shardReference := range partition.GetShardReferences() {
						inShardsToRemove := false
						for _, si := range shardsToRemove {
							if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
								inShardsToRemove = true
								break
							}
						}

						if !inShardsToRemove {
							shardReferences = append(shardReferences, shardReference)
						}
					}

					for _, si := range shardsToAdd {
						alreadyAdded := false
						for _, shardReference := range partition.GetShardReferences() {
							if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
								alreadyAdded = true
								break
							}
						}

						if !alreadyAdded {
							shardReference := &topodatapb.ShardReference{
								Name:     si.ShardName(),
								KeyRange: si.KeyRange,
							}
							shardReferences = append(shardReferences, shardReference)
						}
					}

					partition.ShardReferences = shardReferences
				}

				if err := OrderAndCheckPartitions(cell, srvKeyspace); err != nil {
					rec.RecordError(err)
					return
				}

				err = ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
				if err != nil {
					rec.RecordError(err)
					return
				}

			case IsErrType(err, NoNode):
				// Assuming this cell is not active, nothing to do.
			default:
				if err != nil {
					rec.RecordError(err)
					return
				}
			}
		}(cell, keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		return NewError(PartialResult, rec.Error().Error())
	}
	return nil
}

// UpdateSrvKeyspace saves a new SrvKeyspace. It is a blind write.
func (ts *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	data, err := srvKeyspace.MarshalVT()
	if err != nil {
		return err
	}
	_, err = conn.Update(ctx, nodePath, data, nil)
	return err
}

// DeleteSrvKeyspace deletes a SrvKeyspace.
func (ts *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	return conn.Delete(ctx, nodePath, nil)
}

// GetSrvKeyspaceAllCells returns the SrvKeyspace for all cells
func (ts *Server) GetSrvKeyspaceAllCells(ctx context.Context, keyspace string) ([]*topodatapb.SrvKeyspace, error) {
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, err
	}

	srvKeyspaces := make([]*topodatapb.SrvKeyspace, len(cells))
	for _, cell := range cells {
		srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
		switch {
		case err == nil:
			srvKeyspaces = append(srvKeyspaces, srvKeyspace)
		case IsErrType(err, NoNode):
			// NOOP
		default:
			return srvKeyspaces, err
		}
	}
	return srvKeyspaces, nil
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (ts *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := srvKeyspaceFileName(keyspace)
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := srvKeyspace.UnmarshalVT(data); err != nil {
		return nil, vterrors.Wrapf(err, "SrvKeyspace unmarshal failed: %v", data)
	}
	return srvKeyspace, nil
}

// OrderAndCheckPartitions will re-order the partition list, and check
// it's correct.
func OrderAndCheckPartitions(cell string, srvKeyspace *topodatapb.SrvKeyspace) error {
	// now check them all
	for _, partition := range srvKeyspace.Partitions {
		tabletType := partition.ServedType
		topoproto.ShardReferenceArray(partition.ShardReferences).Sort()

		// check the first Start is MinKey, the last End is MaxKey,
		// and the values in between match: End[i] == Start[i+1]
		first := partition.ShardReferences[0]
		if first.KeyRange != nil && len(first.KeyRange.Start) != 0 {
			return fmt.Errorf("keyspace partition for %v in cell %v does not start with min key", tabletType, cell)
		}
		last := partition.ShardReferences[len(partition.ShardReferences)-1]
		if last.KeyRange != nil && len(last.KeyRange.End) != 0 {
			return fmt.Errorf("keyspace partition for %v in cell %v does not end with max key", tabletType, cell)
		}
		for i := range partition.ShardReferences[0 : len(partition.ShardReferences)-1] {
			currShard := partition.ShardReferences[i]
			nextShard := partition.ShardReferences[i+1]
			currHasKeyRange := currShard.KeyRange != nil
			nextHasKeyRange := nextShard.KeyRange != nil
			if currHasKeyRange != nextHasKeyRange {
				return fmt.Errorf("shards with inconsistent KeyRanges for %v in cell %v. shards: %v, %v", tabletType, cell, currShard, nextShard)
			}
			if !currHasKeyRange {
				// this is the custom sharding case, all KeyRanges must be nil
				continue
			}
			if !key.KeyRangeContiguous(currShard.KeyRange, nextShard.KeyRange) {
				return fmt.Errorf("non-contiguous KeyRange values for %v in cell %v at shard %v to %v: %v != %v", tabletType, cell, i, i+1, hex.EncodeToString(currShard.KeyRange.End), hex.EncodeToString(nextShard.KeyRange.Start))
			}
		}
	}

	return nil
}

// ValidateSrvKeyspace validates that the SrvKeyspace for given keyspace in the provided cells is not corrupted
func (ts *Server) ValidateSrvKeyspace(ctx context.Context, keyspace, cells string) error {
	cellsToValidate, err := ts.ExpandCells(ctx, cells)
	if err != nil {
		return err
	}
	for _, cell := range cellsToValidate {
		srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
		if err != nil {
			return err
		}
		err = OrderAndCheckPartitions(cell, srvKeyspace)
		if err != nil {
			return err
		}
	}
	return nil
}
