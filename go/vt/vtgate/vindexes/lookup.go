/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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
	_ Unique    = (*LookupUnique)(nil)
	_ Lookup    = (*LookupUnique)(nil)
	_ NonUnique = (*LookupNonUnique)(nil)
	_ Lookup    = (*LookupNonUnique)(nil)
)

func init() {
	Register("lookup", NewLookup)
	Register("lookup_unique", NewLookupUnique)
}

// LookupNonUnique defines a vindex that uses a lookup table and create a mapping between from ids and KeyspaceId.
// It's NonUnique and a Lookup.
type LookupNonUnique struct {
	name string
	lkp  lookupInternal
}

// String returns the name of the vindex.
func (ln *LookupNonUnique) String() string {
	return ln.name
}

// Cost returns the cost of this vindex as 20.
func (ln *LookupNonUnique) Cost() int {
	return 20
}

// Map returns the corresponding KeyspaceId values for the given rowsColValues.
func (ln *LookupNonUnique) Map(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([][][]byte, error) {
	out := make([][][]byte, 0, len(rowsColValues))
	results, err := ln.lkp.Lookup(vcursor, rowsColValues)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			ksids = append(ksids, row[0].ToBytes())
		}
		out = append(out, ksids)
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (ln *LookupNonUnique) Verify(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return ln.lkp.Verify(vcursor, rowsColValues, ksidsToValues(ksids))
}

// Create reserves the id by inserting it into the vindex table.
func (ln *LookupNonUnique) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	return ln.lkp.Create(vcursor, rowsColValues, ksidsToValues(ksids), ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (ln *LookupNonUnique) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	return ln.lkp.Delete(vcursor, rowsColValues, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
}

// MarshalJSON returns a JSON representation of LookupHash.
func (ln *LookupNonUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(ln.lkp)
}

// NewLookup creates a LookupNonUnique vindex.
func NewLookup(name string, m map[string]string) (Vindex, error) {
	lookup := &LookupNonUnique{name: name}
	lookup.lkp.Init(m)
	return lookup, nil
}

func ksidsToValues(ksids [][]byte) []sqltypes.Value {
	values := make([]sqltypes.Value, 0, len(ksids))
	for _, ksid := range ksids {
		values = append(values, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
	}
	return values
}

//====================================================================

// LookupUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type LookupUnique struct {
	name string
	lkp  lookupInternal
}

// NewLookupUnique creates a LookupUnique vindex.
func NewLookupUnique(name string, m map[string]string) (Vindex, error) {
	lu := &LookupUnique{name: name}
	lu.lkp.Init(m)
	return lu, nil
}

// String returns the name of the vindex.
func (lu *LookupUnique) String() string {
	return lu.name
}

// Cost returns the cost of this vindex as 10.
func (lu *LookupUnique) Cost() int {
	return 10
}

// Map returns the corresponding KeyspaceId values for the given rowsColValues.
func (lu *LookupUnique) Map(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(rowsColValues))
	results, err := lu.lkp.Lookup(vcursor, rowsColValues)
	if err != nil {
		return nil, err
	}
	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, nil)
		case 1:
			out = append(out, result.Rows[0][0].ToBytes())
		default:
			return nil, fmt.Errorf("Lookup.Map: unexpected multiple results from vindex %s: %v", lu.lkp.Table, rowsColValues[i][0])
		}
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lu *LookupUnique) Verify(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return lu.lkp.Verify(vcursor, rowsColValues, ksidsToValues(ksids))
}

// Create reserves the id by inserting it into the vindex table.
func (lu *LookupUnique) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	return lu.lkp.Create(vcursor, rowsColValues, ksidsToValues(ksids), ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lu *LookupUnique) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	return lu.lkp.Delete(vcursor, rowsColValues, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
}

// MarshalJSON returns a JSON representation of LookupUnique.
func (lu *LookupUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(lu.lkp)
}
