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

type HashVindex struct {
	Table, Column string
	ins, del      string
}

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
		del:    fmt.Sprintf("delete from %s where %s = :%s", t, c, c),
	}, nil
}

func (vind *HashVindex) Cost() int {
	return 1
}

func (vind *HashVindex) Map(_ planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	out := make([]key.KeyspaceId, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, err
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

func (vind *HashVindex) Verify(_ planbuilder.VCursor, id interface{}, ks key.KeyspaceId) (bool, error) {
	num, err := getNumber(id)
	if err != nil {
		return false, err
	}
	return vhash(num) == ks, nil
}

func (vind *HashVindex) ReverseMap(_ planbuilder.VCursor, k key.KeyspaceId) (interface{}, error) {
	return vunhash(k), nil
}

func (vind *HashVindex) Create(vcursor planbuilder.VCursor, id interface{}) error {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.Column: id,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return err
	}
	return nil
}

func (vind *HashVindex) Generate(vcursor planbuilder.VCursor) (id uint64, err error) {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.Column: nil,
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return 0, err
	}
	return result.InsertId, err
}

func (vind *HashVindex) Delete(vcursor planbuilder.VCursor, id interface{}, _ key.KeyspaceId) error {
	bq := &tproto.BoundQuery{
		Sql: vind.del,
		BindVariables: map[string]interface{}{
			vind.Column: id,
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return err
	}
	return nil
}

func getNumber(v interface{}) (uint64, error) {
	switch v := v.(type) {
	case int:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
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

func vhash(shardKey uint64) key.KeyspaceId {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	block3DES.Encrypt(hashed[:], keybytes[:])
	return key.KeyspaceId(hashed[:])
}

func vunhash(k key.KeyspaceId) uint64 {
	if len(k) != 8 {
		panic(fmt.Errorf("invalid keyspace id: %+q", k))
	}
	var unhashed [8]byte
	block3DES.Decrypt(unhashed[:], []byte(k))
	return binary.BigEndian.Uint64(unhashed[:])
}
