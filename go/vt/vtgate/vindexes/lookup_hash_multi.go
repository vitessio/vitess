// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

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

func init() {
	planbuilder.Register("lookup_hash_autoinc", NewLookupHashAuto)
	planbuilder.Register("lookup_hash", NewLookupHash)
}
