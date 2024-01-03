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

package vindexes

import (
	"context"
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	lookupHashParamWriteOnly = "write_only"
)

var (
	_ SingleColumn    = (*LookupHash)(nil)
	_ Lookup          = (*LookupHash)(nil)
	_ LookupPlanable  = (*LookupHash)(nil)
	_ ParamValidating = (*LookupHash)(nil)
	_ SingleColumn    = (*LookupHashUnique)(nil)
	_ Lookup          = (*LookupHashUnique)(nil)
	_ LookupPlanable  = (*LookupHashUnique)(nil)
	_ ParamValidating = (*LookupHashUnique)(nil)

	lookupHashParams = append(
		append(make([]string, 0), lookupCommonParams...),
		lookupHashParamWriteOnly,
	)
)

func init() {
	Register("lookup_hash", newLookupHash)
	Register("lookup_hash_unique", newLookupHashUnique)
}

// ====================================================================

// LookupHash defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup.
// Warning: This Vindex is being deprecated in favor of Lookup
type LookupHash struct {
	name          string
	writeOnly     bool
	lkp           lookupInternal
	unknownParams []string
}

// newLookupHash creates a LookupHash vindex.
// The supplied map has the following required fields:
//
//	table: name of the backing table. It can be qualified by the keyspace.
//	from: list of columns in the table that have the 'from' values of the lookup vindex.
//	to: The 'to' column name of the table.
//
// The following fields are optional:
//
//	autocommit: setting this to "true" will cause inserts to upsert and deletes to be ignored.
//	write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func newLookupHash(name string, m map[string]string) (Vindex, error) {
	lh := &LookupHash{
		name:          name,
		unknownParams: FindUnknownParams(m, lookupHashParams),
	}

	cc, err := parseCommonConfig(m)
	if err != nil {
		return nil, err
	}
	lh.writeOnly, err = boolFromMap(m, lookupHashParamWriteOnly)
	if err != nil {
		return nil, err
	}

	// if autocommit is on for non-unique lookup, upsert should also be on.
	upsert := cc.autocommit || cc.multiShardAutocommit
	if err := lh.lkp.Init(m, cc.autocommit, upsert, cc.multiShardAutocommit); err != nil {
		return nil, err
	}
	return lh, nil
}

// String returns the name of the vindex.
func (lh *LookupHash) String() string {
	return lh.name
}

// Cost returns the cost of this vindex as 20.
func (lh *LookupHash) Cost() int {
	return 20
}

// IsUnique returns false since the Vindex is not unique.
func (lh *LookupHash) IsUnique() bool {
	return false
}

// NeedsVCursor satisfies the Vindex interface.
func (lh *LookupHash) NeedsVCursor() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (lh *LookupHash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lh.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	// if ignore_nulls is set and the query is about single null value, then fallback to all shards
	if len(ids) == 1 && ids[0].IsNull() && lh.lkp.IgnoreNulls {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	results, err := lh.lkp.Lookup(ctx, vcursor, ids, vtgatepb.CommitOrder_NORMAL)
	if err != nil {
		return nil, err
	}
	return lh.MapResult(ids, results)
}

// MapResult implements the LookupPlanable interface
func (lh *LookupHash) MapResult(ids []sqltypes.Value, results []*sqltypes.Result) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lh.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	for _, result := range results {
		if len(result.Rows) == 0 {
			out = append(out, key.DestinationNone{})
			continue
		}
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			num, err := row[0].ToCastUint64()
			if err != nil {
				// A failure to convert is equivalent to not being
				// able to map.
				continue
			}
			ksids = append(ksids, vhash(num))
		}
		out = append(out, key.DestinationKeyspaceIDs(ksids))
	}
	return out, nil
}

// Query implements the LookupPlanable interface
func (lh *LookupHash) Query() (selQuery string, arguments []string) {
	return lh.lkp.query()
}

// AllowBatch implements the LookupPlanable interface
func (lh *LookupHash) AllowBatch() bool {
	return lh.lkp.BatchLookup
}

func (lh *LookupHash) AutoCommitEnabled() bool {
	return lh.lkp.Autocommit
}

// GetCommitOrder implements the LookupPlanable interface
func (lh *LookupHash) GetCommitOrder() vtgatepb.CommitOrder {
	return vtgatepb.CommitOrder_NORMAL
}

// Verify returns true if ids maps to ksids.
func (lh *LookupHash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lh.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}

	values, err := unhashList(ksids)
	if err != nil {
		return nil, vterrors.Wrap(err, "lookup.Verify.vunhash")
	}
	return lh.lkp.Verify(ctx, vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lh *LookupHash) Create(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return vterrors.Wrap(err, "lookup.Create.vunhash")
	}
	return lh.lkp.Create(ctx, vcursor, rowsColValues, values, ignoreMode)
}

// Update updates the entry in the vindex table.
func (lh *LookupHash) Update(ctx context.Context, vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	v, err := vunhash(ksid)
	if err != nil {
		return vterrors.Wrap(err, "lookup.Update.vunhash")
	}
	return lh.lkp.Update(ctx, vcursor, oldValues, ksid, sqltypes.NewUint64(v), newValues)
}

// Delete deletes the entry from the vindex table.
func (lh *LookupHash) Delete(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return vterrors.Wrap(err, "lookup.Delete.vunhash")
	}
	return lh.lkp.Delete(ctx, vcursor, rowsColValues, sqltypes.NewUint64(v), vtgatepb.CommitOrder_NORMAL)
}

// MarshalJSON returns a JSON representation of LookupHash.
func (lh *LookupHash) MarshalJSON() ([]byte, error) {
	return json.Marshal(lh.lkp)
}

// UnknownParams satisfies the ParamValidating interface.
func (lh *LookupHash) UnknownParams() []string {
	return lh.unknownParams
}

// unhashList unhashes a list of keyspace ids into []sqltypes.Value.
func unhashList(ksids [][]byte) ([]sqltypes.Value, error) {
	values := make([]sqltypes.Value, 0, len(ksids))
	for _, ksid := range ksids {
		v, err := vunhash(ksid)
		if err != nil {
			return nil, err
		}
		values = append(values, sqltypes.NewUint64(v))
	}
	return values, nil
}

// ====================================================================

// LookupHashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
// Warning: This Vindex is being deprecated in favor of LookupUnique
type LookupHashUnique struct {
	name          string
	writeOnly     bool
	lkp           lookupInternal
	unknownParams []string
}

var _ LookupPlanable = (*LookupHashUnique)(nil)

// newLookupHashUnique creates a LookupHashUnique vindex.
// The supplied map has the following required fields:
//
//	table: name of the backing table. It can be qualified by the keyspace.
//	from: list of columns in the table that have the 'from' values of the lookup vindex.
//	to: The 'to' column name of the table.
//
// The following fields are optional:
//
//	autocommit: setting this to "true" will cause deletes to be ignored.
//	write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func newLookupHashUnique(name string, m map[string]string) (Vindex, error) {
	lhu := &LookupHashUnique{
		name:          name,
		unknownParams: FindUnknownParams(m, lookupHashParams),
	}

	cc, err := parseCommonConfig(m)
	if err != nil {
		return nil, err
	}
	lhu.writeOnly, err = boolFromMap(m, lookupHashParamWriteOnly)
	if err != nil {
		return nil, err
	}

	// Don't allow upserts for unique vindexes.
	if err := lhu.lkp.Init(m, cc.autocommit, false /* upsert */, cc.multiShardAutocommit); err != nil {
		return nil, err
	}
	return lhu, nil
}

// String returns the name of the vindex.
func (lhu *LookupHashUnique) String() string {
	return lhu.name
}

// Cost returns the cost of this vindex as 10.
func (lhu *LookupHashUnique) Cost() int {
	return 10
}

// IsUnique returns true since the Vindex is unique.
func (lhu *LookupHashUnique) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (lhu *LookupHashUnique) NeedsVCursor() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (lhu *LookupHashUnique) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	if lhu.writeOnly {
		out := make([]key.Destination, 0, len(ids))
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	results, err := lhu.lkp.Lookup(ctx, vcursor, ids, vtgatepb.CommitOrder_NORMAL)
	if err != nil {
		return nil, err
	}
	return lhu.MapResult(ids, results)
}

func (lhu *LookupHashUnique) MapResult(ids []sqltypes.Value, results []*sqltypes.Result) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lhu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}
	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, key.DestinationNone{})
		case 1:
			num, err := result.Rows[0][0].ToCastUint64()
			if err != nil {
				out = append(out, key.DestinationNone{})
				continue
			}
			out = append(out, key.DestinationKeyspaceID(vhash(num)))
		default:
			return nil, fmt.Errorf("LookupHash.Map: unexpected multiple results from vindex %s: %v", lhu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lhu *LookupHashUnique) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lhu.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}

	values, err := unhashList(ksids)
	if err != nil {
		return nil, vterrors.Wrap(err, "lookup.Verify.vunhash")
	}
	return lhu.lkp.Verify(ctx, vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lhu *LookupHashUnique) Create(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return vterrors.Wrap(err, "lookup.Create.vunhash")
	}
	return lhu.lkp.Create(ctx, vcursor, rowsColValues, values, ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lhu *LookupHashUnique) Delete(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return vterrors.Wrap(err, "lookup.Delete.vunhash")
	}
	return lhu.lkp.Delete(ctx, vcursor, rowsColValues, sqltypes.NewUint64(v), vtgatepb.CommitOrder_NORMAL)
}

// Update updates the entry in the vindex table.
func (lhu *LookupHashUnique) Update(ctx context.Context, vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	v, err := vunhash(ksid)
	if err != nil {
		return vterrors.Wrap(err, "lookup.Update.vunhash")
	}
	return lhu.lkp.Update(ctx, vcursor, oldValues, ksid, sqltypes.NewUint64(v), newValues)
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (lhu *LookupHashUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(lhu.lkp)
}

// IsBackfilling implements the LookupBackfill interface
func (lhu *LookupHashUnique) IsBackfilling() bool {
	return lhu.writeOnly
}

func (lhu *LookupHashUnique) AllowBatch() bool {
	return lhu.lkp.BatchLookup
}

func (lhu *LookupHashUnique) AutoCommitEnabled() bool {
	return lhu.lkp.Autocommit
}

func (lhu *LookupHashUnique) Query() (selQuery string, arguments []string) {
	return lhu.lkp.query()
}

// GetCommitOrder implements the LookupPlanable interface
func (lhu *LookupHashUnique) GetCommitOrder() vtgatepb.CommitOrder {
	return vtgatepb.CommitOrder_NORMAL
}

// UnknownParams implements the ParamValidating interface.
func (lhu *LookupHashUnique) UnknownParams() []string {
	return lhu.unknownParams
}
