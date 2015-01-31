// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

func init() {
	planbuilder.Register("lookup_hash", NewLookupHash)
	planbuilder.Register("lookup_hash_autoinc", NewLookupHashAuto)
	planbuilder.Register("lookup_hash_unique", NewLookupHashUnique)
	planbuilder.Register("lookup_hash_unique_autoinc", NewLookupHashUniqueAuto)
}

//====================================================================

// LookupHash defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup.
type LookupHash struct {
	lkp lookup
}

// NewLookupHash creates a LookupHash vindex.
func NewLookupHash(m map[string]interface{}) (planbuilder.Vindex, error) {
	lhu := &LookupHash{}
	lhu.lkp.Init(m)
	return lhu, nil
}

// Cost returns the cost of this vindex as 20.
func (vind *LookupHash) Cost() int {
	return 20
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *LookupHash) Map(vcursor planbuilder.VCursor, ids []interface{}) ([][]key.KeyspaceId, error) {
	return vind.lkp.Map2(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vind *LookupHash) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	return vind.lkp.Verify(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHash) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Create(vcursor, id, ksid)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHash) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

//====================================================================

// LookupHashAuto defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// NonUnique and a Lookup. It's also a LookupGenerator, because it
// can use the autoinc capabilities of the lookup table.
type LookupHashAuto struct {
	lkp lookup
}

// NewLookupHashAuto creates a new LookupHashAuto.
func NewLookupHashAuto(m map[string]interface{}) (planbuilder.Vindex, error) {
	h := &LookupHashAuto{}
	h.lkp.Init(m)
	return h, nil
}

// Cost returns the cost of this index as 20.
func (vind *LookupHashAuto) Cost() int {
	return 20
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *LookupHashAuto) Map(vcursor planbuilder.VCursor, ids []interface{}) ([][]key.KeyspaceId, error) {
	return vind.lkp.Map2(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vind *LookupHashAuto) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	return vind.lkp.Verify(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHashAuto) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Create(vcursor, id, ksid)
}

// Generate reserves the id by inserting it into the vindex table.
func (vind *LookupHashAuto) Generate(vcursor planbuilder.VCursor, ksid key.KeyspaceId) (id int64, err error) {
	return vind.lkp.Generate(vcursor, ksid)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHashAuto) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

//====================================================================

// LookupHashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type LookupHashUnique struct {
	lkp lookup
}

// NewLookupHashUnique creates a LookupHashUnique vindex.
func NewLookupHashUnique(m map[string]interface{}) (planbuilder.Vindex, error) {
	lhu := &LookupHashUnique{}
	lhu.lkp.Init(m)
	return lhu, nil
}

// Cost returns the cost of this vindex as 10.
func (vind *LookupHashUnique) Cost() int {
	return 10
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *LookupHashUnique) Map(vcursor planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	return vind.lkp.Map1(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vind *LookupHashUnique) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	return vind.lkp.Verify(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHashUnique) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Create(vcursor, id, ksid)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHashUnique) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

//====================================================================

// LookupHashUniqueAuto defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup. It's also a LookupGenerator, because it
// can use the autoinc capabilities of the lookup table.
type LookupHashUniqueAuto struct {
	lkp lookup
}

// NewLookupHashUniqueAuto creates a new LookupHashUniqueAuto.
func NewLookupHashUniqueAuto(m map[string]interface{}) (planbuilder.Vindex, error) {
	h := &LookupHashUniqueAuto{}
	h.lkp.Init(m)
	return h, nil
}

// Cost returns the cost of this index as 10.
func (vind *LookupHashUniqueAuto) Cost() int {
	return 10
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *LookupHashUniqueAuto) Map(vcursor planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	return vind.lkp.Map1(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vind *LookupHashUniqueAuto) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	return vind.lkp.Verify(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHashUniqueAuto) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	return vind.lkp.Create(vcursor, id, ksid)
}

// Generate reserves the id by inserting it into the vindex table.
func (vind *LookupHashUniqueAuto) Generate(vcursor planbuilder.VCursor, ksid key.KeyspaceId) (id int64, err error) {
	return vind.lkp.Generate(vcursor, ksid)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHashUniqueAuto) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid key.KeyspaceId) error {
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
	lkp.del = fmt.Sprintf("delete from %s where %s in ::%s and %s = :%s", t, from, from, to, to)
}

// Map1 is for a unique vindex.
func (lkp *lookup) Map1(vcursor planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	out := make([]key.KeyspaceId, 0, len(ids))
	bq := &tproto.BoundQuery{
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
			out = append(out, "")
			continue
		}
		if len(result.Rows) != 1 {
			return nil, fmt.Errorf("lookup.Map: unexpected multiple results from vindex %s: %v", lkp.Table, id)
		}
		inum, err := mproto.Convert(result.Fields[0].Type, result.Rows[0][0])
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		num, err := getNumber(inum)
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

// Map2 is for a non-unique vindex.
func (lkp *lookup) Map2(vcursor planbuilder.VCursor, ids []interface{}) ([][]key.KeyspaceId, error) {
	out := make([][]key.KeyspaceId, 0, len(ids))
	bq := &tproto.BoundQuery{
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
		var ksids []key.KeyspaceId
		for _, row := range result.Rows {
			inum, err := mproto.Convert(result.Fields[0].Type, row[0])
			if err != nil {
				return nil, fmt.Errorf("lookup.Map: %v", err)
			}
			num, err := getNumber(inum)
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
func (lkp *lookup) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	val, err := vunhash(ksid)
	if err != nil {
		return false, fmt.Errorf("lookup.Verify: %v", err)
	}
	bq := &tproto.BoundQuery{
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
func (lkp *lookup) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	val, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	bq := &tproto.BoundQuery{
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

// Generate generates an id and associates the ksid to the new id.
func (lkp *lookup) Generate(vcursor planbuilder.VCursor, ksid key.KeyspaceId) (id int64, err error) {
	val, err := vunhash(ksid)
	if err != nil {
		return 0, fmt.Errorf("lookup.Generate: %v", err)
	}
	bq := &tproto.BoundQuery{
		Sql: lkp.ins,
		BindVariables: map[string]interface{}{
			lkp.From: nil,
			lkp.To:   val,
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return 0, fmt.Errorf("lookup.Generate: %v", err)
	}
	return int64(result.InsertId), err
}

// Delete deletes the association between ids and ksid.
func (lkp *lookup) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid key.KeyspaceId) error {
	val, err := vunhash(ksid)
	if err != nil {
		return fmt.Errorf("lookup.Delete: %v", err)
	}
	bq := &tproto.BoundQuery{
		Sql: lkp.del,
		BindVariables: map[string]interface{}{
			lkp.From: ids,
			lkp.To:   val,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return fmt.Errorf("lookup.Delete: %v", err)
	}
	return nil
}
