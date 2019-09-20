/*
Copyright 2019 The Vitess Authors.

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
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	_ Vindex     = (*Hash)(nil)
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

// IsUnique returns true since the Vindex is unique.
func (vind *Hash) IsUnique() bool {
	return true
}

// IsFunctional returns true since the Vindex is functional.
func (vind *Hash) IsFunctional() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (vind *Hash) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		var num uint64
		var err error

		if id.IsSigned() {
			// This is ToUint64 with no check on negative values.
			str := id.ToString()
			var ival int64
			ival, err = strconv.ParseInt(str, 10, 64)
			num = uint64(ival)
		} else {
			num, err = sqltypes.ToUint64(id)
		}

		if err != nil {
			out[i] = key.DestinationNone{}
			continue
		}
		out[i] = key.DestinationKeyspaceID(vhash(num))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *Hash) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		num, err := sqltypes.ToUint64(ids[i])
		if err != nil {
			return nil, vterrors.Wrap(err, "hash.Verify")
		}
		out[i] = bytes.Equal(vhash(num), ksids[i])
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
