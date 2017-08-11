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
	"encoding/json"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ NonUnique = (*LookupHash)(nil)
	_ Lookup    = (*LookupHash)(nil)
	_ Unique    = (*LookupHashUnique)(nil)
	_ Lookup    = (*LookupHashUnique)(nil)
)

func init() {
	Register("lookup_hash", NewLookupHash)
	Register("lookup_hash_unique", NewLookupHashUnique)
}

//====================================================================

// LookupHash defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup.
type LookupHash struct {
	name string
	lkp  lookupInternal
}

// NewLookupHash creates a LookupHash vindex.
func NewLookupHash(name string, m map[string]string) (Vindex, error) {
	lh := &LookupHash{name: name}
	lh.lkp.Init(m)
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

// Map returns the corresponding KeyspaceId values for the given ids.
func (lh *LookupHash) Map(vcursor VCursor, ids []sqltypes.Value) ([][][]byte, error) {
	out := make([][][]byte, 0, len(ids))
	results, err := lh.lkp.Lookup(vcursor, ids)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			num, err := sqltypes.ToUint64(row[0])
			if err != nil {
				return nil, fmt.Errorf("lookupHash.Map.ToUint64: %v", err)
			}
			ksids = append(ksids, vhash(num))
		}
		out = append(out, ksids)
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lh *LookupHash) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	values, err := unhashList(ksids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	return lh.lkp.Verify(vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lh *LookupHash) Create(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return fmt.Errorf("lookup.Create.vunhash: %v", err)
	}
	return lh.lkp.Create(vcursor, ids, values, ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lh *LookupHash) Delete(vcursor VCursor, ids []sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete.vunhash: %v", err)
	}
	return lh.lkp.Delete(vcursor, ids, sqltypes.NewUint64(v))
}

// MarshalJSON returns a JSON representation of LookupHash.
func (lh *LookupHash) MarshalJSON() ([]byte, error) {
	return json.Marshal(lh.lkp)
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

//====================================================================

// LookupHashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type LookupHashUnique struct {
	name string
	lkp  lookupInternal
}

// NewLookupHashUnique creates a LookupHashUnique vindex.
func NewLookupHashUnique(name string, m map[string]string) (Vindex, error) {
	lhu := &LookupHashUnique{name: name}
	lhu.lkp.Init(m)
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

// Map returns the corresponding KeyspaceId values for the given ids.
func (lhu *LookupHashUnique) Map(vcursor VCursor, ids []sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	results, err := lhu.lkp.Lookup(vcursor, ids)
	if err != nil {
		return nil, err
	}
	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, nil)
		case 1:
			num, err := sqltypes.ToUint64(result.Rows[0][0])
			if err != nil {
				return nil, fmt.Errorf("LookupHash.Map: %v", err)
			}
			out = append(out, vhash(num))
		default:
			return nil, fmt.Errorf("LookupHash.Map: unexpected multiple results from vindex %s: %v", lhu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lhu *LookupHashUnique) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	values, err := unhashList(ksids)
	if err != nil {
		return nil, fmt.Errorf("lookup.Verify.vunhash: %v", err)
	}
	return lhu.lkp.Verify(vcursor, ids, values)
}

// Create reserves the id by inserting it into the vindex table.
func (lhu *LookupHashUnique) Create(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	values, err := unhashList(ksids)
	if err != nil {
		return fmt.Errorf("lookup.Create.vunhash: %v", err)
	}
	return lhu.lkp.Create(vcursor, ids, values, ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lhu *LookupHashUnique) Delete(vcursor VCursor, ids []sqltypes.Value, ksid []byte) error {
	v, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete.vunhash: %v", err)
	}
	return lhu.lkp.Delete(vcursor, ids, sqltypes.NewUint64(v))
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (lhu *LookupHashUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(lhu.lkp)
}
