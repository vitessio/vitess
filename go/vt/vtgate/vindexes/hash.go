// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"bytes"
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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
func (vind *Hash) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, fmt.Errorf("hash.Map: %v", err)
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

// Verify returns true if id maps to ksid.
func (vind *Hash) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	num, err := getNumber(id)
	if err != nil {
		return false, fmt.Errorf("hash.Verify: %v", err)
	}
	return bytes.Compare(vhash(num), ksid) == 0, nil
}

// ReverseMap returns the id from ksid.
func (vind *Hash) ReverseMap(_ VCursor, ksid []byte) (interface{}, error) {
	return vunhash(ksid)
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

func vhash(shardKey int64) []byte {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], uint64(shardKey))
	block3DES.Encrypt(hashed[:], keybytes[:])
	return []byte(hashed[:])
}

func vunhash(k []byte) (int64, error) {
	if len(k) != 8 {
		return 0, fmt.Errorf("invalid keyspace id: %v", hex.EncodeToString(k))
	}
	var unhashed [8]byte
	block3DES.Decrypt(unhashed[:], k)
	return int64(binary.BigEndian.Uint64(unhashed[:])), nil
}
