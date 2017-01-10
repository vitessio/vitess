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

// Verify returns true if ids maps to ksids.
func (vind *Hash) Verify(_ VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	if len(ids) != len(ksids) {
		return false, fmt.Errorf("hash.Verify: length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	for rowNum := range ids {
		num, err := getNumber(ids[rowNum])
		if err != nil {
			return false, fmt.Errorf("hash.Verify: %v", err)
		}
		if bytes.Compare(vhash(num), ksids[rowNum]) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// ReverseMap returns the ids from ksids.
func (vind *Hash) ReverseMap(_ VCursor, ksids [][]byte) ([]interface{}, error) {
	reverseIds := make([]interface{}, len(ksids))
	var err error
	for rownum, keyspaceID := range ksids {
		reverseIds[rownum], err = vunhash(keyspaceID)
		if err != nil {
			return reverseIds, err
		}
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
