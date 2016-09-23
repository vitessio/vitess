package vindexes

import (
	"bytes"
	"fmt"
)

// Binary is a vindex that converts binary bits to a keyspace id.
type Binary struct {
	name string
}

// NewBinary creates a new Binary.
func NewBinary(name string, _ map[string]string) (Vindex, error) {
	return &Binary{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Binary) String() string {
	return vind.name
}

// Cost returns the cost as 1.
func (vind *Binary) Cost() int {
	return 0
}

// Verify returns true if id maps to ksid.
func (vind *Binary) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	data, err := getBytes(id)
	if err != nil {
		return false, fmt.Errorf("Binary.Verify: %v", err)
	}
	return bytes.Compare(data, ksid) == 0, nil
}

// Map returns the corresponding keyspace id values for the given ids.
func (vind *Binary) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := getBytes(id)
		if err != nil {
			return nil, fmt.Errorf("Binary.Map :%v", err)
		}
		out = append(out, data)
	}
	return out, nil
}

// ReverseMap returns the associated id for the ksid.
func (*Binary) ReverseMap(_ VCursor, ksid []byte) (interface{}, error) {
	if ksid == nil {
		return nil, fmt.Errorf("Binary.ReverseMap: is nil")
	}
	return []byte(ksid), nil
}

func init() {
	Register("binary", NewBinary)
}
