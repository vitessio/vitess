// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"encoding/json"
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
	lkp  lookup
}

// NewLookupHash creates a LookupHash vindex.
func NewLookupHash(name string, m map[string]string) (Vindex, error) {
	lhu := &LookupHash{name: name}
	lhu.lkp.Init(m, true)
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
func (vind *LookupHash) Map(vcursor VCursor, ids []interface{}) ([][][]byte, error) {
	return vind.lkp.MapNonUniqueLookup(vcursor, ids)
}

// Verify returns true if ids maps to ksids.
func (vind *LookupHash) Verify(vcursor VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	return vind.lkp.Verify(vcursor, ids, ksids)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHash) Create(vcursor VCursor, id []interface{}, ksids [][]byte) error {
	return vind.lkp.Create(vcursor, id, ksids)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHash) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

// MarshalJSON returns a JSON representation of LookupHash.
func (vind *LookupHash) MarshalJSON() ([]byte, error) {
	return json.Marshal(vind.lkp)
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
func NewLookupHashUnique(name string, m map[string]string) (Vindex, error) {
	lhu := &LookupHashUnique{name: name}
	lhu.lkp.Init(m, true)
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
func (vind *LookupHashUnique) Map(vcursor VCursor, ids []interface{}) ([][]byte, error) {
	return vind.lkp.MapUniqueLookup(vcursor, ids)
}

// Verify returns true if ids maps to ksids.
func (vind *LookupHashUnique) Verify(vcursor VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	return vind.lkp.Verify(vcursor, ids, ksids)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *LookupHashUnique) Create(vcursor VCursor, id []interface{}, ksids [][]byte) error {
	return vind.lkp.Create(vcursor, id, ksids)
}

// Delete deletes the entry from the vindex table.
func (vind *LookupHashUnique) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	return vind.lkp.Delete(vcursor, ids, ksid)
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (vind *LookupHashUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(vind.lkp)
}
