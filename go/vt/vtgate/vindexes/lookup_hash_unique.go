// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

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

func init() {
	planbuilder.Register("lookup_hash_unique_autoinc", NewLookupHashUniqueAuto)
	planbuilder.Register("lookup_hash_unique", NewLookupHashUnique)
}
