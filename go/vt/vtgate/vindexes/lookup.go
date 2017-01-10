package vindexes

import (
	"encoding/json"
)

func init() {
	Register("lookup", NewLookup)
	Register("lookup_unique", NewLookupUnique)
}

// LookupNonUnique defines a vindex that uses a lookup table and create a mapping between id and KeyspaceId.
//It's NonUnique and a Lookup.
type LookupNonUnique struct {
	name string
	lkp  lookup
}

// NewLookup creates a LookupNonUnique vindex.
func NewLookup(name string, m map[string]string) (Vindex, error) {
	lookup := &LookupNonUnique{name: name}
	lookup.lkp.Init(m, false)
	return lookup, nil
}

// String returns the name of the vindex.
func (vindex *LookupNonUnique) String() string {
	return vindex.name
}

// Cost returns the cost of this vindex as 20.
func (vindex *LookupNonUnique) Cost() int {
	return 20
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vindex *LookupNonUnique) Map(vcursor VCursor, ids []interface{}) ([][][]byte, error) {
	return vindex.lkp.MapNonUniqueLookup(vcursor, ids)
}

// Verify returns true if ids maps to ksids.
func (vindex *LookupNonUnique) Verify(vcursor VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	return vindex.lkp.Verify(vcursor, ids, ksids)
}

// Create reserves the id by inserting it into the vindex table.
func (vindex *LookupNonUnique) Create(vcursor VCursor, id []interface{}, ksids [][]byte) error {
	return vindex.lkp.Create(vcursor, id, ksids)
}

// Delete deletes the entry from the vindex table.
func (vindex *LookupNonUnique) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	return vindex.lkp.Delete(vcursor, ids, ksid)
}

// MarshalJSON returns a JSON representation of LookupHash.
func (vindex *LookupNonUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(vindex.lkp)
}

// LookupUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type LookupUnique struct {
	name string
	lkp  lookup
}

// NewLookupUnique creates a LookupHashUnique vindex.
func NewLookupUnique(name string, m map[string]string) (Vindex, error) {
	lu := &LookupUnique{name: name}
	lu.lkp.Init(m, false)
	return lu, nil
}

// String returns the name of the vindex.
func (vindex *LookupUnique) String() string {
	return vindex.name
}

// Cost returns the cost of this vindex as 10.
func (vindex *LookupUnique) Cost() int {
	return 10
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vindex *LookupUnique) Map(vcursor VCursor, ids []interface{}) ([][]byte, error) {
	return vindex.lkp.MapUniqueLookup(vcursor, ids)
}

// Verify returns true if ids maps to ksids.
func (vindex *LookupUnique) Verify(vcursor VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	return vindex.lkp.Verify(vcursor, ids, ksids)
}

// Create reserves the id by inserting it into the vindex table.
func (vindex *LookupUnique) Create(vcursor VCursor, id []interface{}, ksids [][]byte) error {
	return vindex.lkp.Create(vcursor, id, ksids)
}

// Delete deletes the entry from the vindex table.
func (vindex *LookupUnique) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	return vindex.lkp.Delete(vcursor, ids, ksid)
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (vindex *LookupUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(vindex.lkp)
}
