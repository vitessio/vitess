/*
Copyright 2023 The Vitess Authors.

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

package planbuilder

import (
	"context"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// hashIndex is a functional, unique Vindex.
type hashIndex struct{ name string }

func (v *hashIndex) String() string   { return v.name }
func (*hashIndex) Cost() int          { return 1 }
func (*hashIndex) IsUnique() bool     { return true }
func (*hashIndex) NeedsVCursor() bool { return false }
func (*hashIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*hashIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func newHashIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &hashIndex{name: name}, nil
}

// lookupIndex is a unique Vindex, and satisfies Lookup.
type lookupIndex struct{ name string }

func (v *lookupIndex) String() string   { return v.name }
func (*lookupIndex) Cost() int          { return 2 }
func (*lookupIndex) IsUnique() bool     { return true }
func (*lookupIndex) NeedsVCursor() bool { return false }
func (*lookupIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*lookupIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*lookupIndex) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*lookupIndex) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*lookupIndex) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}
func newLookupIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &lookupIndex{name: name}, nil
}

var _ vindexes.Lookup = (*lookupIndex)(nil)

// nameLkpIndex satisfies Lookup, NonUnique.
type nameLkpIndex struct{ name string }

func (v *nameLkpIndex) String() string                     { return v.name }
func (*nameLkpIndex) Cost() int                            { return 3 }
func (*nameLkpIndex) IsUnique() bool                       { return false }
func (*nameLkpIndex) NeedsVCursor() bool                   { return false }
func (*nameLkpIndex) AllowBatch() bool                     { return true }
func (*nameLkpIndex) AutoCommitEnabled() bool              { return false }
func (*nameLkpIndex) GetCommitOrder() vtgatepb.CommitOrder { return vtgatepb.CommitOrder_NORMAL }
func (*nameLkpIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*nameLkpIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*nameLkpIndex) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*nameLkpIndex) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*nameLkpIndex) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}
func (*nameLkpIndex) Query() (string, []string) {
	return "select name, keyspace_id from name_user_vdx where name in ::name", []string{"name"}
}
func (*nameLkpIndex) MapResult([]sqltypes.Value, []*sqltypes.Result) ([]key.Destination, error) {
	return nil, nil
}
func newNameLkpIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &nameLkpIndex{name: name}, nil
}

var _ vindexes.Vindex = (*nameLkpIndex)(nil)
var _ vindexes.Lookup = (*nameLkpIndex)(nil)
var _ vindexes.LookupPlanable = (*nameLkpIndex)(nil)

// costlyIndex satisfies Lookup, NonUnique.
type costlyIndex struct{ name string }

func (v *costlyIndex) String() string   { return v.name }
func (*costlyIndex) Cost() int          { return 10 }
func (*costlyIndex) IsUnique() bool     { return false }
func (*costlyIndex) NeedsVCursor() bool { return false }
func (*costlyIndex) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*costlyIndex) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*costlyIndex) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*costlyIndex) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*costlyIndex) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}
func newCostlyIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &costlyIndex{name: name}, nil
}

var _ vindexes.Vindex = (*costlyIndex)(nil)
var _ vindexes.Lookup = (*costlyIndex)(nil)

// multiColIndex satisfies multi column vindex.
type multiColIndex struct{ name string }

func (m *multiColIndex) String() string   { return m.name }
func (*multiColIndex) Cost() int          { return 1 }
func (*multiColIndex) IsUnique() bool     { return true }
func (*multiColIndex) NeedsVCursor() bool { return false }
func (*multiColIndex) Map(ctx context.Context, vcursor vindexes.VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*multiColIndex) Verify(ctx context.Context, vcursor vindexes.VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*multiColIndex) PartialVindex() bool { return true }
func newMultiColIndex(name string, _ map[string]string) (vindexes.Vindex, error) {
	return &multiColIndex{name: name}, nil
}

var _ vindexes.MultiColumn = (*multiColIndex)(nil)

// unqLkpVdxBackfill satisfies Lookup, Unique.
type unqLkpVdxBackfill struct {
	name       string
	inBackfill bool
	cost       int
}

func (u *unqLkpVdxBackfill) String() string                     { return u.name }
func (u *unqLkpVdxBackfill) Cost() int                          { return u.cost }
func (*unqLkpVdxBackfill) IsUnique() bool                       { return false }
func (*unqLkpVdxBackfill) NeedsVCursor() bool                   { return false }
func (*unqLkpVdxBackfill) AllowBatch() bool                     { return true }
func (*unqLkpVdxBackfill) AutoCommitEnabled() bool              { return false }
func (*unqLkpVdxBackfill) GetCommitOrder() vtgatepb.CommitOrder { return vtgatepb.CommitOrder_NORMAL }
func (*unqLkpVdxBackfill) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*unqLkpVdxBackfill) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return nil, nil
}
func (*unqLkpVdxBackfill) Create(context.Context, vindexes.VCursor, [][]sqltypes.Value, [][]byte, bool) error {
	return nil
}
func (*unqLkpVdxBackfill) Delete(context.Context, vindexes.VCursor, [][]sqltypes.Value, []byte) error {
	return nil
}
func (*unqLkpVdxBackfill) Update(context.Context, vindexes.VCursor, []sqltypes.Value, []byte, []sqltypes.Value) error {
	return nil
}
func (*unqLkpVdxBackfill) Query() (string, []string) {
	return "select unq_key, keyspace_id from unq_lkp_idx where unq_key in ::unq_key", []string{"unq_key"}
}
func (*unqLkpVdxBackfill) MapResult([]sqltypes.Value, []*sqltypes.Result) ([]key.Destination, error) {
	return nil, nil
}
func (u *unqLkpVdxBackfill) IsBackfilling() bool { return u.inBackfill }

func newUnqLkpVdxBackfill(name string, m map[string]string) (vindexes.Vindex, error) {
	vdx := &unqLkpVdxBackfill{name: name}
	if val, ok := m["write_only"]; ok {
		vdx.inBackfill = val == "true"
	}
	if val, ok := m["cost"]; ok {
		vdx.cost, _ = strconv.Atoi(val)
	}
	return vdx, nil
}

var _ vindexes.Vindex = (*unqLkpVdxBackfill)(nil)
var _ vindexes.Lookup = (*unqLkpVdxBackfill)(nil)
var _ vindexes.LookupPlanable = (*unqLkpVdxBackfill)(nil)
var _ vindexes.LookupBackfill = (*unqLkpVdxBackfill)(nil)

func init() {
	vindexes.Register("hash_test", newHashIndex)
	vindexes.Register("lookup_test", newLookupIndex)
	vindexes.Register("name_lkp_test", newNameLkpIndex)
	vindexes.Register("costly", newCostlyIndex)
	vindexes.Register("multiCol_test", newMultiColIndex)
	vindexes.Register("unq_lkp_test", newUnqLkpVdxBackfill)
}
