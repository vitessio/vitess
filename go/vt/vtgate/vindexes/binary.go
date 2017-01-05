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
	return 1
}

// Verify returns true if ids maps to ksids.
func (vind *Binary) Verify(_ VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	if len(ids) != len(ksids) {
		return false, fmt.Errorf("Binary.Verify: length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	for rowNum := range ids {
		data, err := getBytes(ids[rowNum])
		if err != nil {
			return false, fmt.Errorf("Binary.Verify: %v", err)
		}
		if bytes.Compare(data, ksids[rowNum]) != 0 {
			return false, nil
		}
	}
	return true, nil
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

// ReverseMap returns the associated ids for the ksids.
func (*Binary) ReverseMap(_ VCursor, ksids [][]byte) ([]interface{}, error) {
	var reverseIds = make([]interface{}, len(ksids))
	for rownum, keyspaceID := range ksids {
		if keyspaceID == nil {
			return nil, fmt.Errorf("Binary.ReverseMap: keyspaceId is nil")
		}
		reverseIds[rownum] = []byte(keyspaceID)
	}
	return reverseIds, nil
}

func init() {
	Register("binary", NewBinary)
}
