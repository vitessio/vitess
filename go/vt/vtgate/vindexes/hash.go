// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	_ planbuilder.Unique              = (*HashVindex)(nil)
	_ planbuilder.Reversible          = (*HashVindex)(nil)
	_ planbuilder.FunctionalGenerator = (*HashVindex)(nil)
)

// HashVindex defines vindex that hashes an int64 to a KeyspaceId
// by using null-key 3DES hash. It's Unique, Reversible and
// Functional. Additionally, it's also a FunctionalGenerator
// because it's capable of generating new values from a vindex table
// with a single unique autoinc column.
type HashVindex struct {
	Table, Column string
	ins, del      string
}

// NewHashVindex creates a new HashVindex.
func NewHashVindex(m map[string]interface{}) (planbuilder.Vindex, error) {
	get := func(name string) string {
		v, _ := m[name].(string)
		return v
	}
	t := get("Table")
	c := get("Column")
	return &HashVindex{
		Table:  t,
		Column: c,
		ins:    fmt.Sprintf("insert into %s(%s) values(:%s)", t, c, c),
		del:    fmt.Sprintf("delete from %s where %s in ::%s", t, c, c),
	}, nil
}

// Cost returns the cost of this index as 1.
func (vind *HashVindex) Cost() int {
	return 1
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *HashVindex) Map(_ planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	out := make([]key.KeyspaceId, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, fmt.Errorf("HashVindex.Map: %v", err)
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

// Verify returns true if id maps to ksid.
func (vind *HashVindex) Verify(_ planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	num, err := getNumber(id)
	if err != nil {
		return false, fmt.Errorf("HashVindex.Verify: %v", err)
	}
	return vhash(num) == ksid, nil
}

// ReverseMap returns the id from ksid.
func (vind *HashVindex) ReverseMap(_ planbuilder.VCursor, ksid key.KeyspaceId) (interface{}, error) {
	return vunhash(ksid), nil
}

// Create reserves the id by inserting it into the vindex table.
func (vind *HashVindex) Create(vcursor planbuilder.VCursor, id interface{}) error {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.Column: id,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return fmt.Errorf("HashVindex.Create: %v", err)
	}
	return nil
}

// Generate generates a new id by using the autoinc of the vindex table.
func (vind *HashVindex) Generate(vcursor planbuilder.VCursor) (id int64, err error) {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.Column: nil,
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return 0, fmt.Errorf("HashVindex.Generate: %v", err)
	}
	return int64(result.InsertId), err
}

// Delete deletes the entry from the vindex table.
func (vind *HashVindex) Delete(vcursor planbuilder.VCursor, ids []interface{}, _ key.KeyspaceId) error {
	bq := &tproto.BoundQuery{
		Sql: vind.del,
		BindVariables: map[string]interface{}{
			vind.Column: ids,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return fmt.Errorf("HashVindex.Delete: %v", err)
	}
	return nil
}

func getNumber(v interface{}) (int64, error) {
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
	planbuilder.Register("hash", NewHashVindex)
}

func vhash(shardKey int64) key.KeyspaceId {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], uint64(shardKey))
	block3DES.Encrypt(hashed[:], keybytes[:])
	return key.KeyspaceId(hashed[:])
}

func vunhash(k key.KeyspaceId) int64 {
	if len(k) != 8 {
		panic(fmt.Errorf("invalid keyspace id: %+q", k))
	}
	var unhashed [8]byte
	block3DES.Decrypt(unhashed[:], []byte(k))
	return int64(binary.BigEndian.Uint64(unhashed[:]))
}
