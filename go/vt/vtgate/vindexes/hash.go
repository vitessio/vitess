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
	"strconv"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

// Hash defines vindex that hashes an int64 to a KeyspaceId
// by using null-key 3DES hash. It's Unique, Reversible and
// Functional.
type Hash struct {
	hv HashAuto
}

// NewHash creates a new Hash.
func NewHash(m map[string]interface{}) (planbuilder.Vindex, error) {
	h := &Hash{}
	h.hv.Init(m)
	return h, nil
}

// Cost returns the cost of this index as 1.
func (vind *Hash) Cost() int {
	return vind.hv.Cost()
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *Hash) Map(_ planbuilder.VCursor, ids []interface{}) ([][]byte, error) {
	return vind.hv.Map(nil, ids)
}

// Verify returns true if id maps to ksid.
func (vind *Hash) Verify(_ planbuilder.VCursor, id interface{}, ksid []byte) (bool, error) {
	return vind.hv.Verify(nil, id, ksid)
}

// ReverseMap returns the id from ksid.
func (vind *Hash) ReverseMap(_ planbuilder.VCursor, ksid []byte) (interface{}, error) {
	return vind.hv.ReverseMap(nil, ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *Hash) Create(vcursor planbuilder.VCursor, id interface{}) error {
	return vind.hv.Create(vcursor, id)
}

// Delete deletes the entry from the vindex table.
func (vind *Hash) Delete(vcursor planbuilder.VCursor, ids []interface{}, _ []byte) error {
	return vind.hv.Delete(vcursor, ids, []byte{})
}

// HashAuto defines vindex that hashes an int64 to a KeyspaceId
// by using null-key 3DES hash. It's Unique, Reversible and
// Functional. Additionally, it's also a FunctionalGenerator
// because it's capable of generating new values from a vindex table
// with a single unique autoinc column.
type HashAuto struct {
	Table, Column string
	ins, del      string
}

// NewHashAuto creates a new HashAuto.
func NewHashAuto(m map[string]interface{}) (planbuilder.Vindex, error) {
	hva := &HashAuto{}
	hva.Init(m)
	return hva, nil
}

// Init initializes HashAuto.
func (vind *HashAuto) Init(m map[string]interface{}) {
	get := func(name string) string {
		v, _ := m[name].(string)
		return v
	}
	t := get("Table")
	c := get("Column")
	vind.Table = t
	vind.Column = c
	vind.ins = fmt.Sprintf("insert into %s(%s) values(:%s)", t, c, c)
	vind.del = fmt.Sprintf("delete from %s where %s in ::%s", t, c, c)
}

// Cost returns the cost of this index as 1.
func (vind *HashAuto) Cost() int {
	return 1
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *HashAuto) Map(_ planbuilder.VCursor, ids []interface{}) ([][]byte, error) {
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
func (vind *HashAuto) Verify(_ planbuilder.VCursor, id interface{}, ksid []byte) (bool, error) {
	num, err := getNumber(id)
	if err != nil {
		return false, fmt.Errorf("hash.Verify: %v", err)
	}
	return bytes.Compare(vhash(num), ksid) == 0, nil
}

// ReverseMap returns the id from ksid.
func (vind *HashAuto) ReverseMap(_ planbuilder.VCursor, ksid []byte) (interface{}, error) {
	return vunhash(ksid)
}

// Create reserves the id by inserting it into the vindex table.
func (vind *HashAuto) Create(vcursor planbuilder.VCursor, id interface{}) error {
	bq := &querytypes.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.Column: id,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return fmt.Errorf("hash.Create: %v", err)
	}
	return nil
}

// Generate generates a new id by using the autoinc of the vindex table.
func (vind *HashAuto) Generate(vcursor planbuilder.VCursor) (id int64, err error) {
	bq := &querytypes.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.Column: nil,
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return 0, fmt.Errorf("hash.Generate: %v", err)
	}
	return int64(result.InsertID), err
}

// Delete deletes the entry from the vindex table.
func (vind *HashAuto) Delete(vcursor planbuilder.VCursor, ids []interface{}, _ []byte) error {
	bq := &querytypes.BoundQuery{
		Sql: vind.del,
		BindVariables: map[string]interface{}{
			vind.Column: ids,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return fmt.Errorf("hash.Delete: %v", err)
	}
	return nil
}

func getNumber(v interface{}) (int64, error) {
	// Failsafe check: v will never be a []byte.
	if val, ok := v.([]byte); ok {
		v = string(val)
	}

	switch v := v.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case string:
		signed, err := strconv.ParseInt(v, 0, 64)
		if err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(v, 0, 64)
		if err == nil {
			return int64(unsigned), nil
		}
		return 0, fmt.Errorf("getNumber: %v", err)
	}
	return 0, fmt.Errorf("unexpected type for %v: %T", v, v)
}

var block3DES cipher.Block

func init() {
	var err error
	block3DES, err = des.NewTripleDESCipher(make([]byte, 24))
	if err != nil {
		panic(err)
	}
	planbuilder.Register("hash", NewHash)
	planbuilder.Register("hash_autoinc", NewHashAuto)
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
