package vindexes

import (
	"bytes"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_        Vindex = (*Null)(nil)
	nullksid        = []byte{0}
)

// Null defines a vindex that always return 0. It's Unique and
// Functional.
// This is useful for rows that always go into the first shard.
// This Vindex can be used for validating an unsharded->sharded transition.
// Unlike other vindexes, this one will work even for NULL input values. This
// will allow you to keep MySQL auto-inc columns unchanged.
type Null struct {
	name string
}

// NewNull creates a new Null.
func NewNull(name string, m map[string]string) (Vindex, error) {
	return &Null{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Null) String() string {
	return vind.name
}

// Cost returns the cost of this index as 100.
func (vind *Null) Cost() int {
	return 100
}

// IsUnique returns true since the Vindex is unique.
func (vind *Null) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *Null) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (vind *Null) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for i := 0; i < len(ids); i++ {
		out = append(out, key.DestinationKeyspaceID(nullksid))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *Null) Verify(cursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		out[i] = bytes.Equal(nullksid, ksids[i])
	}
	return out, nil
}

func init() {
	Register("null", NewNull)
}
