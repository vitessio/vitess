package vindexes

import (
	"bytes"
	"fmt"
)

// UTF8cihash defines vindex that hashes an varchar to a KeyspaceId
// by using bytes.toLower().
type UTF8cihash struct {
	name string
}

// Newutf8cihash creates a new utf8cihash.
func Newutf8cihash(name string, m map[string]interface{}) (Vindex, error) {
	return &UTF8cihash{name: name}, nil
}

// String returns the name of the vindex.
func (vind *UTF8cihash) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *UTF8cihash) Cost() int {
	return 1
}

// Verify returns true if id maps to ksid.
func (vind *UTF8cihash) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	data, err := getutf8cihash(id)
	if err != nil {
		return false, fmt.Errorf("utf8cihash.Verify: %v", err)
	}
	return bytes.Compare(data, ksid) == 0, nil
}

func getutf8cihash(key interface{}) ([]byte, error) {
	source, ok := key.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected data type for binHash: %T", key)
	}
	val, error  := binHash(bytes.ToLower(source))
	return val, error
}

func binHash(source []byte) ([]byte, error) {
	dest := make([]byte, len(source))
	block3DES.Encrypt(dest, source)
	return dest, nil
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *UTF8cihash) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := getutf8cihash(id)
		if err != nil {
			return nil, fmt.Errorf("utf8cihash.Map :%v", err)
		}
		out = append(out, data)
	}
	return out, nil
}

func init() {
	Register("utf8cihash", Newutf8cihash)
}
