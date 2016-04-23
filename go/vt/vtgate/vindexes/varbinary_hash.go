package vindexes

import (
	"bytes"
	"fmt"
)

// Varbinary defines vindex that hashes an varbinary to a KeyspaceId
// by just returning the bytes.
type Varbinary struct {
	name string
}

// NewVarbinary creates a new Varbinary.
func NewVarbinary(name string, m map[string]interface{}) (Vindex, error) {
	return &Varbinary{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Varbinary) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *Varbinary) Cost() int {
	return 1
}

// Verify returns true if id maps to ksid.
func (vind *Varbinary) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	data, err := getHashBinary(id)
	if err != nil {
		return false, fmt.Errorf("Varbinary_hash.Verify: %v", err)
	}
	return bytes.Compare(data, ksid) == 0, nil
}

func getHashBinary(v interface{}) ([]byte, error) {
	val, err := GetBytes(v)
	return val, err
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *Varbinary) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := getHashBinary(id)
		if err != nil {
			return nil, fmt.Errorf("VarBinary_hash.Map :%v", err)
		}
		out = append(out, data)
	}
	return out, nil
}

func init() {
	Register("varbinary_hash", NewVarbinary)
}
