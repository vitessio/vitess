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
	"strings"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ NonUnique = (*LookupNonUnique)(nil)
	_ Lookup    = (*LookupNonUnique)(nil)
	_ Unique    = (*LookupUnique)(nil)
	_ Lookup    = (*LookupUnique)(nil)
	_ NonUnique = (*MultiColLookupNonUnique)(nil)
	_ Lookup    = (*MultiColLookupNonUnique)(nil)
)

func init() {
	Register("lookup", NewLookup)
	Register("lookup_unique", NewLookupUnique)
}

// MultiColLookupNonUnique defines a vindex that uses a lookup table and create a mapping between from ids and KeyspaceId.
//It's NonUnique and a MultiColLookup.
type MultiColLookupNonUnique struct {
	name string
	lkp  multiColLookupInternal
}

// String returns the name of the vindex.
func (ln *MultiColLookupNonUnique) String() string {
	return ln.name
}

// Cost returns the cost of this vindex as 20.
func (ln *MultiColLookupNonUnique) Cost() int {
	//TODO @rafael
	return 20
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (ln *MultiColLookupNonUnique) Map(vcursor VCursor, ids []sqltypes.Value) ([][][]byte, error) {
	out := make([][][]byte, 0, len(ids))
	results, err := ln.lkp.Lookup(vcursor, ids)
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
func (ln *MultiColLookupNonUnique) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return ln.lkp.Verify(vcursor, ids, ksidsToValues(ksids))
}

// Create reserves the id by inserting it into the vindex table.
func (ln *MultiColLookupNonUnique) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	return ln.lkp.Create(vcursor, rowsColValues, ksidsToValues(ksids), ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (ln *MultiColLookupNonUnique) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	return ln.lkp.Delete(vcursor, rowsColValues, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
}

// MarshalJSON returns a JSON representation of LookupHash.
func (ln *MultiColLookupNonUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(ln.lkp)
}

//====================================================================

// LookupNonUnique defines a vindex that uses a lookup table and create a mapping between id and KeyspaceId.
//It's NonUnique and a Lookup.
type LookupNonUnique struct {
	name string
	lkp  lookupInternal
}

// NewLookup creates a LookupNonUnique vindex.
func NewLookup(name string, m map[string]string) (Vindex, error) {
	var fromColumns []string
	for _, from := range strings.Split(m["from"], ",") {
		fromColumns = append(fromColumns, strings.TrimSpace(from))
	}
	// In the future we can just use MultiColLookupNonUnique for everything.
	// A vindex with only one column in a special case of the multicol lookup.
	// However, to reduce risk and don't change so many things at the same time,
	// I would like to keep the multicolumn implementation separete from the original
	// one
	if len(fromColumns) > 1 {
		lookup := &MultiColLookupNonUnique{name: name}
		lookup.lkp.Init(m)
		return lookup, nil
	}
	lookup := &LookupNonUnique{name: name}
	lookup.lkp.Init(m)
	return lookup, nil
}

// String returns the name of the vindex.
func (ln *LookupNonUnique) String() string {
	return ln.name
}

// Cost returns the cost of this vindex as 20.
func (ln *LookupNonUnique) Cost() int {
	return 20
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (ln *LookupNonUnique) Map(vcursor VCursor, ids []sqltypes.Value) ([][][]byte, error) {
	out := make([][][]byte, 0, len(ids))
	results, err := ln.lkp.Lookup(vcursor, ids)
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
func (ln *LookupNonUnique) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return ln.lkp.Verify(vcursor, ids, ksidsToValues(ksids))
}

// Create reserves the id by inserting it into the vindex table.
func (ln *LookupNonUnique) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	// In the non multicolumn case, ids is always a slice with 1 element.
	var ids []sqltypes.Value
	for _, vindexValues := range rowsColValues {
		ids = append(ids, vindexValues[0])
	}
	return ln.lkp.Create(vcursor, ids, ksidsToValues(ksids), ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (ln *LookupNonUnique) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	var ids []sqltypes.Value
	for _, vindexValues := range rowsColValues {
		ids = append(ids, vindexValues[0])
	}
	return ln.lkp.Delete(vcursor, ids, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
}

// MarshalJSON returns a JSON representation of Lookup
func (ln *LookupNonUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(ln.lkp)
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

// Map returns the corresponding KeyspaceId values for the given ids.
func (lu *LookupUnique) Map(vcursor VCursor, ids []sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	results, err := lu.lkp.Lookup(vcursor, ids)
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
			return nil, fmt.Errorf("Lookup.Map: unexpected multiple results from vindex %s: %v", lu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lu *LookupUnique) Verify(vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return lu.lkp.Verify(vcursor, ids, ksidsToValues(ksids))
}

// Create reserves the id by inserting it into the vindex table.
func (lu *LookupUnique) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	// In the non multicolumn case, ids is always a slice with 1 element.
	var ids []sqltypes.Value
	for _, vindexValues := range rowsColValues {
		ids = append(ids, vindexValues[0])
	}
	return lu.lkp.Create(vcursor, ids, ksidsToValues(ksids), ignoreMode)
}

// Delete deletes the entry from the vindex table.
func (lu *LookupUnique) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	var ids []sqltypes.Value
	for _, vindexValues := range rowsColValues {
		ids = append(ids, vindexValues[0])
	}
	return lu.lkp.Delete(vcursor, ids, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
}

// MarshalJSON returns a JSON representation of LookupUnique.
func (lu *LookupUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(lu.lkp)
}
