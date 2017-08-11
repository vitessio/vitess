/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"bytes"
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ Functional = (*Hash)(nil)
	_ Reversible = (*Hash)(nil)
)

// Hash defines vindex that hashes an int64 to a KeyspaceId
// by using null-key 3DES hash. It's Unique, Reversible and
// Functional.
type Hash struct {
	name string
}

// NewHash creates a new Hash.
func NewHash(name string, m map[string]string) (Vindex, error) {
	return &Hash{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Hash) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *Hash) Cost() int {
	return 1
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *Hash) Map(_ VCursor, ids []sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := sqltypes.ToUint64(id)
		if err != nil {
			return nil, fmt.Errorf("hash.Map: %v", err)
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *Hash) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		num, err := sqltypes.ToUint64(ids[i])
		if err != nil {
			return nil, fmt.Errorf("hash.Verify: %v", err)
		}
		out[i] = (bytes.Compare(vhash(num), ksids[i]) == 0)
	}
	return out, nil
}

// ReverseMap returns the ids from ksids.
func (vind *Hash) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	reverseIds := make([]sqltypes.Value, 0, len(ksids))
	for _, keyspaceID := range ksids {
		val, err := vunhash(keyspaceID)
		if err != nil {
			return reverseIds, err
		}
		reverseIds = append(reverseIds, sqltypes.NewUint64(val))
	}
	return reverseIds, nil
}

var block3DES cipher.Block

func init() {
	var err error
	block3DES, err = des.NewTripleDESCipher(make([]byte, 24))
	if err != nil {
		panic(err)
	}
	Register("hash", NewHash)
}

func vhash(shardKey uint64) []byte {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	block3DES.Encrypt(hashed[:], keybytes[:])
	return []byte(hashed[:])
}

func vunhash(k []byte) (uint64, error) {
	if len(k) != 8 {
		return 0, fmt.Errorf("invalid keyspace id: %v", hex.EncodeToString(k))
	}
	var unhashed [8]byte
	block3DES.Decrypt(unhashed[:], k)
	return binary.BigEndian.Uint64(unhashed[:]), nil
}
