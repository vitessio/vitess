// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	_ planbuilder.Unique     = (*HashVindex)(nil)
	_ planbuilder.Reversible = (*HashVindex)(nil)
)

type HashVindex struct{}

func NewHashVindex(_ map[string]interface{}) (planbuilder.Vindex, error) {
	return &HashVindex{}, nil
}

func (vind *HashVindex) Cost() int {
	return 1
}

func (vind *HashVindex) Map(ids []interface{}) ([]key.KeyspaceId, error) {
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

func (vind *HashVindex) Verify(id interface{}, ks key.KeyspaceId) (bool, error) {
	num, err := getNumber(id)
	if err != nil {
		return false, err
	}
	return vhash(num) == ks, nil
}

func (vind *HashVindex) ReverseMap(k key.KeyspaceId) (interface{}, error) {
	return vunhash(k), nil
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
	case sqltypes.Value:
		result, err := v.ParseUint64()
		if err != nil {
			return 0, fmt.Errorf("error parsing %v: %v", v, err)
		}
		return result, nil
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
	var unhashed [8]byte
	block3DES.Decrypt(unhashed[:], []byte(k))
	return binary.BigEndian.Uint64(unhashed[:])
}
