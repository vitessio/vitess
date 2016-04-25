package vindexes

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Varchar defines vindex that hashes an varchar to a KeyspaceId
// by using bytes.toLower().
type Varchar struct {
	name string
}

// NewVarchar creates a new Varchar.
func NewVarchar(name string, m map[string]interface{}) (Vindex, error) {
	return &Varchar{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Varchar) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *Varchar) Cost() int {
	return 1
}

// Verify returns true if id maps to ksid.
func (vind *Varchar) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	data, err := binHash(id, true)
	if err != nil {
		return false, fmt.Errorf("Varchar_hash.Verify: %v", err)
	}
	return bytes.Compare(data, ksid) == 0, nil
}

// GetBytes returns bytes for a given value
func GetBytes(key interface{}, toLower bool) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	if toLower {
		return bytes.ToLower(buf.Bytes()), nil
	}
	return buf.Bytes(), nil
}

func binHash(key interface{}, toLower bool) ([]byte, error) {
	source, err := GetBytes(key,toLower)
	if err != nil {
		return nil, fmt.Errorf("unexpected data type for binHash: %T", key)
	}
	dest := make([]byte, len(source))
	block3DES.Encrypt(dest, bytes.ToLower(source))
	return dest, nil
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *Varchar) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := binHash(id, true)
		if err != nil {
			return nil, fmt.Errorf("Varchar_hash.Map :%v", err)
		}
		out = append(out, data)
	}
	return out, nil
}

func init() {
	Register("varcharHash", NewVarchar)
}
