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

package vindexes

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	_ Vindex = (*LookupUnicodeLooseMD5Hash)(nil)
	_ Lookup = (*LookupUnicodeLooseMD5Hash)(nil)
	_ Vindex = (*LookupUnicodeLooseMD5HashUnique)(nil)
	_ Lookup = (*LookupUnicodeLooseMD5HashUnique)(nil)
)

func init() {
	Register("lookup_unicodeloosemd5_hash", NewLookupUnicodeLooseMD5Hash)
	Register("lookup_unicodeloosemd5_hash_unique", NewLookupUnicodeLooseMD5HashUnique)
}

//====================================================================

// LookupUnicodeLooseMD5Hash defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup and stores the from value in a hashed form.
// Warning: This Vindex is being depcreated in favor of Lookup
type LookupUnicodeLooseMD5Hash struct {
	name      string
	writeOnly bool
	lkp       lookupInternal
}

// NewLookupUnicodeLooseMD5Hash creates a LookupUnicodeLooseMD5Hash vindex.
// The supplied map has the following required fields:
//   table: name of the backing table. It can be qualified by the keyspace.
//   from: list of columns in the table that have the 'from' values of the lookup vindex.
//   to: The 'to' column name of the table.
//
// The following fields are optional:
//   autocommit: setting this to "true" will cause inserts to upsert and deletes to be ignored.
//   write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func NewLookupUnicodeLooseMD5Hash(name string, m map[string]string) (Vindex, error) {
	lh := &LookupUnicodeLooseMD5Hash{name: name}

	autocommit, err := boolFromMap(m, "autocommit")
	if err != nil {
		return nil, err
	}
	lh.writeOnly, err = boolFromMap(m, "write_only")
	if err != nil {
		return nil, err
	}

	// if autocommit is on for non-unique lookup, upsert should also be on.
	if err := lh.lkp.Init(m, autocommit, autocommit /* upsert */); err != nil {
		return nil, err
	}
	return lh, nil
}

// String returns the name of the vindex.
func (lh *LookupUnicodeLooseMD5Hash) String() string {
	return lh.name
}

// Cost returns the cost of this vindex as 20.
func (lh *LookupUnicodeLooseMD5Hash) Cost() int {
	return 20
}

// IsUnique returns false since the Vindex is not unique.
func (lh *LookupUnicodeLooseMD5Hash) IsUnique() bool {
	return false
}

// IsFunctional returns false since the Vindex is not functional.
func (lh *LookupUnicodeLooseMD5Hash) IsFunctional() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (lh *LookupUnicodeLooseMD5Hash) Map(vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lh.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	ids, err := convertIds(ids)
	if err != nil {
		return nil, err
	}
	results, err := lh.lkp.Lookup(vcursor, ids)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if len(result.Rows) == 0 {
			out = append(out, key.DestinationNone{})
			continue
		}
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			num, err := sqltypes.ToUint64(row[0])
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

// Verify returns true if ids maps to ksids.
func (lh *LookupUnicodeLooseMD5Hash) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lh.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}

	values, err := unhashList(ksids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	ids, err = convertIds(ids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	return lh.lkp.Verify(vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lh *LookupUnicodeLooseMD5Hash) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return fmt.Errorf("lookup.Create.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Create.convert: %v", err)
	}
	return lh.lkp.Create(vcursor, rowsColValues, values, ignoreMode)
}

// Update updates the entry in the vindex table.
func (lh *LookupUnicodeLooseMD5Hash) Update(vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Update.vunhash: %v", err)
	}
	newValues, err = convertIds(newValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	oldValues, err = convertIds(oldValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	return lh.lkp.Update(vcursor, oldValues, sqltypes.NewUint64(v), newValues)
}

// Delete deletes the entry from the vindex table.
func (lh *LookupUnicodeLooseMD5Hash) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Delete.convert: %v", err)
	}
	return lh.lkp.Delete(vcursor, rowsColValues, sqltypes.NewUint64(v), vtgatepb.CommitOrder_NORMAL)
}

// MarshalJSON returns a JSON representation of LookupHash.
func (lh *LookupUnicodeLooseMD5Hash) MarshalJSON() ([]byte, error) {
	return json.Marshal(lh.lkp)
}

//====================================================================

// LookupUnicodeLooseMD5HashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup and will store the from value in a hashed format.
// Warning: This Vindex is being depcreated in favor of LookupUnique
type LookupUnicodeLooseMD5HashUnique struct {
	name      string
	writeOnly bool
	lkp       lookupInternal
}

// NewLookupUnicodeLooseMD5HashUnique creates a LookupUnicodeLooseMD5HashUnique vindex.
// The supplied map has the following required fields:
//   table: name of the backing table. It can be qualified by the keyspace.
//   from: list of columns in the table that have the 'from' values of the lookup vindex.
//   to: The 'to' column name of the table.
//
// The following fields are optional:
//   autocommit: setting this to "true" will cause deletes to be ignored.
//   write_only: in this mode, Map functions return the full keyrange causing a full scatter.
func NewLookupUnicodeLooseMD5HashUnique(name string, m map[string]string) (Vindex, error) {
	lhu := &LookupUnicodeLooseMD5HashUnique{name: name}

	autocommit, err := boolFromMap(m, "autocommit")
	if err != nil {
		return nil, err
	}
	lhu.writeOnly, err = boolFromMap(m, "write_only")
	if err != nil {
		return nil, err
	}

	// Don't allow upserts for unique vindexes.
	if err := lhu.lkp.Init(m, autocommit, false /* upsert */); err != nil {
		return nil, err
	}
	return lhu, nil
}

// String returns the name of the vindex.
func (lhu *LookupUnicodeLooseMD5HashUnique) String() string {
	return lhu.name
}

// Cost returns the cost of this vindex as 10.
func (lhu *LookupUnicodeLooseMD5HashUnique) Cost() int {
	return 10
}

// IsUnique returns true since the Vindex is unique.
func (lhu *LookupUnicodeLooseMD5HashUnique) IsUnique() bool {
	return true
}

// IsFunctional returns false since the Vindex is not functional.
func (lhu *LookupUnicodeLooseMD5HashUnique) IsFunctional() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (lhu *LookupUnicodeLooseMD5HashUnique) Map(vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lhu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	ids, err := convertIds(ids)
	if err != nil {
		return nil, err
	}
	results, err := lhu.lkp.Lookup(vcursor, ids)
	if err != nil {
		return nil, err
	}
	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, key.DestinationNone{})
		case 1:
			num, err := sqltypes.ToUint64(result.Rows[0][0])
			if err != nil {
				out = append(out, key.DestinationNone{})
				continue
			}
			out = append(out, key.DestinationKeyspaceID(vhash(num)))
		default:
			return nil, fmt.Errorf("LookupUnicodeLooseMD5HashUnique.Map: unexpected multiple results from vindex %s: %v", lhu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lhu *LookupUnicodeLooseMD5HashUnique) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lhu.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}

	values, err := unhashList(ksids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	ids, err = convertIds(ids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	return lhu.lkp.Verify(vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lhu *LookupUnicodeLooseMD5HashUnique) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return fmt.Errorf("lookup.Create.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Create.convert: %v", err)
	}
	return lhu.lkp.Create(vcursor, rowsColValues, values, ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lhu *LookupUnicodeLooseMD5HashUnique) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete.vunhash: %v", err)
	}
	rowsColValues, err = convertRows(rowsColValues)
	if err != nil {
		return fmt.Errorf("lookup.Delete.convert: %v", err)
	}
	return lhu.lkp.Delete(vcursor, rowsColValues, sqltypes.NewUint64(v), vtgatepb.CommitOrder_NORMAL)
}

// Update updates the entry in the vindex table.
func (lhu *LookupUnicodeLooseMD5HashUnique) Update(vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Update.vunhash: %v", err)
	}
	newValues, err = convertIds(newValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	oldValues, err = convertIds(oldValues)
	if err != nil {
		return fmt.Errorf("lookup.Update.convert: %v", err)
	}
	return lhu.lkp.Update(vcursor, oldValues, sqltypes.NewUint64(v), newValues)
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (lhu *LookupUnicodeLooseMD5HashUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(lhu.lkp)
}

func unicodeHashValue(value sqltypes.Value) (sqltypes.Value, error) {
	hash, err := unicodeHash(value)
	if err != nil {
		return sqltypes.NULL, err
	}

	return sqltypes.NewUint64(binary.BigEndian.Uint64(hash[:8])), nil
}

func convertIds(ids []sqltypes.Value) ([]sqltypes.Value, error) {
	converted := make([]sqltypes.Value, 0, len(ids))
	for _, id := range ids {
		idVal, err := unicodeHashValue(id)
		if err != nil {
			return nil, err
		}
		converted = append(converted, idVal)
	}
	return converted, nil
}

func convertRows(rows [][]sqltypes.Value) ([][]sqltypes.Value, error) {
	converted := make([][]sqltypes.Value, 0, len(rows))
	for _, row := range rows {
		row, err := convertIds(row)
		if err != nil {
			return nil, err
		}
		converted = append(converted, row)
	}
	return converted, nil
}
