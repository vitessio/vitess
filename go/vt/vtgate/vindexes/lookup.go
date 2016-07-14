package vindexes

import (
	"encoding/json"
	"fmt"
	"encoding/hex"
)

func init() {
	Register("lookup", NewLookup)
	Register("lookup_unique", NewLookupUnique)
}

/* LookupNonUnique defines a vindex that uses a lookup table and create a mapping between id and KeyspaceId.
 * The table is expected to define the id column as unique. It's
 * NonUnique and a Lookup.
 */
type LookupNonUnique struct {
	name string
	lkp  lookup
}

// NewLookup creates a LookupNonUnique vindex.
func NewLookup(name string, m map[string]string) (Vindex, error) {
	lookup := &LookupNonUnique{name: name}
	lookup.lkp.Init(m)
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
	return vindex.lkp.MapLookup(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vindex *LookupNonUnique) Verify(vcursor VCursor, id interface{}, ksid []byte) (bool, error) {
	return vindex.lkp.VerifyLookup(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vindex *LookupNonUnique) Create(vcursor VCursor, id interface{}, ksid []byte) error {
	return vindex.lkp.CreateLookup(vcursor, id, ksid)
}

// Delete deletes the entry from the vindex table.
func (vindex *LookupNonUnique) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	return vindex.lkp.DeleteLookup(vcursor, ids, ksid)
}

// MarshalJSON returns a JSON representation of LookupHash.
func (vindex *LookupNonUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(vindex.lkp)
}

/* LookupUnique defines a vindex that uses a lookup table.
 * The table is expected to define the id column as unique. It's
 * Unique and a Lookup.
 */
type LookupUnique struct {
	name string
	lkp  lookup
}

// NewLookupUnique creates a LookupHashUnique vindex.
func NewLookupUnique(name string, m map[string]string) (Vindex, error) {
	lu := &LookupUnique{name: name}
	lu.lkp.Init(m)
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
	return vindex.lkp.MapLookupUnique(vcursor, ids)
}

// Verify returns true if id maps to ksid.
func (vindex *LookupUnique) Verify(vcursor VCursor, id interface{}, ksid []byte) (bool, error) {
	return vindex.lkp.VerifyLookup(vcursor, id, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vindex *LookupUnique) Create(vcursor VCursor, id interface{}, ksid []byte) error {
	return vindex.lkp.CreateLookup(vcursor, id, ksid)
}

// Delete deletes the entry from the vindex table.
func (vindex *LookupUnique) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	return vindex.lkp.DeleteLookup(vcursor, ids, ksid)
}

// MarshalJSON returns a JSON representation of LookupHashUnique.
func (vindex *LookupUnique) MarshalJSON() ([]byte, error) {
	return json.Marshal(vindex.lkp)
}

// MapLookupUnique is for a unique vindex.
func (lkp *lookup) MapLookupUnique(vcursor VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		result, err := vcursor.Execute(lkp.sel, map[string]interface{}{
			lkp.From: id,
		})
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
		source, err := getBytes(result.Rows[0][0].ToNative())
		if err != nil {
			return nil, err
		}
		data,err := hex.DecodeString(string(source))
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	return out, nil
}

// MapLookup is for a non-unique vindex.
func (lkp *lookup) MapLookup(vcursor VCursor, ids []interface{}) ([][][]byte, error) {
	out := make([][][]byte, 0, len(ids))
	for _, id := range ids {
		result, err := vcursor.Execute(lkp.sel, map[string]interface{}{
			lkp.From: id,
		})
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		var ksids [][]byte
		for _, row := range result.Rows {
			source, err := getBytes(row[0].ToNative())
			if err != nil {
				return nil, err
			}
			data, err := hex.DecodeString(string(source))
			if err != nil {
				return nil, fmt.Errorf("lookup.Map: %v", err)
			}
			ksids = append(ksids, data)
		}
		out = append(out, ksids)
	}
	return out, nil
}

// Create creates an association between id and keyspaceid by inserting a row in the vindex table.
func (lkp *lookup) CreateLookup(vcursor VCursor, id interface{}, keyspaceid []byte) error {
	keyspace_id := hex.EncodeToString(keyspaceid)
	if _, err := vcursor.Execute(lkp.ins, map[string]interface{}{
		lkp.From: id,
		lkp.To:   keyspace_id,
	}); err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	return nil
}

// Delete deletes the association between ids and keyspaceid.
func (lkp *lookup) DeleteLookup(vcursor VCursor, ids []interface{}, keyspaceid []byte) error {
	keyspace_id := hex.EncodeToString(keyspaceid)
	bindvars := map[string]interface{}{
		lkp.To: keyspace_id,
	}
	for _, id := range ids {
		bindvars[lkp.From] = id
		if _, err := vcursor.Execute(lkp.del, bindvars); err != nil {
			return fmt.Errorf("lookup.Delete: %v", err)
		}
	}
	return nil
}

// VerifyLookup returns true if id maps to ksid.
func (lkp *lookup) VerifyLookup(vcursor VCursor, id interface{}, keyspaceid []byte) (bool, error) {
	keyspace_id := hex.EncodeToString(keyspaceid)
	result, err := vcursor.Execute(lkp.ver, map[string]interface{}{
		lkp.From: id,
		lkp.To:   keyspace_id,
	})
	if err != nil {
		return false, fmt.Errorf("lookup.Verify: %v", err)
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	return true, nil
}
