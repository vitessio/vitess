// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

func init() {
	planbuilder.Register("lookup_hash", NewLookupHash)
	planbuilder.Register("lookup_hash_unique", NewLookupHashUnique)
}

//====================================================================

// LookupHash defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup.
type LookupHash struct {
	name string
	lkp  lookup
}

// NewLookupHash creates a LookupHash vindex.
func NewLookupHash(name string, m map[string]interface{}) (planbuilder.Vindex, error) {
	lhu := &LookupHash{name: name}
	lhu.lkp.Init(m)
	return lhu, nil
}

// String returns the name of the vindex.
func (vind *LookupHash) String() string {
	return vind.name
}

// Cost returns the cost of this vindex as 20.
func (vind *LookupHash) Cost() int {
	return 20
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *LookupHash) Map(vcursor planbuilder.VCursor, ids []interface{}) ([][][]byte, error) {
	return vind.lkp.Map2(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vind *LookupHash) Verify(vcursor planbuilder.VCursor, id interface{}, ksid []byte) (bool, error) {
	return vind.lkp.Verify(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHash) Create(vcursor planbuilder.VCursor, id interface{}, ksid []byte) error {
	return vind.lkp.Create(vcursor, id, ksid)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHash) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid []byte) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

//====================================================================

// LookupHashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type LookupHashUnique struct {
	name string
	lkp  lookup
}

// NewLookupHashUnique creates a LookupHashUnique vindex.
func NewLookupHashUnique(name string, m map[string]interface{}) (planbuilder.Vindex, error) {
	lhu := &LookupHashUnique{name: name}
	lhu.lkp.Init(m)
	return lhu, nil
}

// String returns the name of the vindex.
func (vind *LookupHashUnique) String() string {
	return vind.name
}

// Cost returns the cost of this vindex as 10.
func (vind *LookupHashUnique) Cost() int {
	return 10
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *LookupHashUnique) Map(vcursor planbuilder.VCursor, ids []interface{}) ([][]byte, error) {
	return vind.lkp.Map1(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vind *LookupHashUnique) Verify(vcursor planbuilder.VCursor, id interface{}, ksid []byte) (bool, error) {
	return vind.lkp.Verify(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHashUnique) Create(vcursor planbuilder.VCursor, id interface{}, ksid []byte) error {
	return vind.lkp.Create(vcursor, id, ksid)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHashUnique) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid []byte) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

//====================================================================

// lookup implements the functions for the Lookup vindexes.
type lookup struct {
	Table, From, To    string
	sel, ver, ins, del string
}

func (lkp *lookup) Init(m map[string]interface{}) {
	get := func(name string) string {
		v, _ := m[name].(string)
		return v
	}
	t := get("Table")
	from := get("From")
	to := get("To")

	lkp.Table = t
	lkp.From = from
	lkp.To = to
	lkp.sel = fmt.Sprintf("select %s from %s where %s = :%s", to, t, from, from)
	lkp.ver = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", from, t, from, from, to, to)
	lkp.ins = fmt.Sprintf("insert into %s(%s, %s) values(:%s, :%s)", t, from, to, from, to)
	lkp.del = fmt.Sprintf("delete from %s where %s = :%s and %s = :%s", t, from, from, to, to)
}

// Map1 is for a unique vindex.
func (lkp *lookup) Map1(vcursor planbuilder.VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	bq := &querytypes.BoundQuery{
		Sql: lkp.sel,
	}
	for _, id := range ids {
		bq.BindVariables = map[string]interface{}{
			lkp.From: id,
		}
		result, err := vcursor.Execute(bq)
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		if len(result.Rows) == 0 {
			out = append(out, []byte{})
			continue
		}
		if len(result.Rows) != 1 {
			return nil, fmt.Errorf("lookup.Map: unexpected multiple results from vindex %s: %v", lkp.Table, id)
		}
		num, err := getNumber(result.Rows[0][0].ToNative())
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

// Map2 is for a non-unique vindex.
func (lkp *lookup) Map2(vcursor planbuilder.VCursor, ids []interface{}) ([][][]byte, error) {
	out := make([][][]byte, 0, len(ids))
	bq := &querytypes.BoundQuery{
		Sql: lkp.sel,
	}
	for _, id := range ids {
		bq.BindVariables = map[string]interface{}{
			lkp.From: id,
		}
		result, err := vcursor.Execute(bq)
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		var ksids [][]byte
		for _, row := range result.Rows {
			num, err := getNumber(row[0].ToNative())
			if err != nil {
				return nil, fmt.Errorf("lookup.Map: %v", err)
			}
			ksids = append(ksids, vhash(num))
		}
		out = append(out, ksids)
	}
	return out, nil
}

// Verify returns true if id maps to ksid.
func (lkp *lookup) Verify(vcursor planbuilder.VCursor, id interface{}, ksid []byte) (bool, error) {
	val, err := vunhash(ksid)
	if err != nil {
		return false, fmt.Errorf("lookup.Verify: %v", err)
	}
	bq := &querytypes.BoundQuery{
		Sql: lkp.ver,
		BindVariables: map[string]interface{}{
			lkp.From: id,
			lkp.To:   val,
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return false, fmt.Errorf("lookup.Verify: %v", err)
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	return true, nil
}

// Create creates an association between id and ksid by inserting a row in the vindex table.
func (lkp *lookup) Create(vcursor planbuilder.VCursor, id interface{}, ksid []byte) error {
	val, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	bq := &querytypes.BoundQuery{
		Sql: lkp.ins,
		BindVariables: map[string]interface{}{
			lkp.From: id,
			lkp.To:   val,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	return nil
}

// Delete deletes the association between ids and ksid.
func (lkp *lookup) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid []byte) error {
	val, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete: %v", err)
	}
	bq := &querytypes.BoundQuery{
		Sql: lkp.del,
		BindVariables: map[string]interface{}{
			lkp.To: val,
		},
	}
	for _, id := range ids {
		bq.BindVariables[lkp.From] = id
		if _, err := vcursor.Execute(bq); err != nil {
			return fmt.Errorf("lookup.Delete: %v", err)
		}
	}
	return nil
}
