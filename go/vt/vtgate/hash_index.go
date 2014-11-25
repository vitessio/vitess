// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

type HashIndex struct {
	keyspace string
	serv     SrvTopoServer
	cell     string
}

func NewHashIndex(keyspace string, serv SrvTopoServer, cell string) *HashIndex {
	return &HashIndex{
		keyspace: keyspace,
		serv:     serv,
		cell:     cell,
	}
}

func (hindex *HashIndex) Resolve(tabletType topo.TabletType, shardKeys []interface{}) (newKeyspace string, shards []string, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(hindex.serv, hindex.cell, hindex.keyspace, tabletType)
	if err != nil {
		return "", nil, err
	}
	shards = make([]string, 0, len(shardKeys))
	for _, shardKey := range shardKeys {
		num, err := getNumber(shardKey)
		if err != nil {
			return "", nil, err
		}
		shard, err := getShardForKeyspaceId(allShards, vhash(num))
		if err != nil {
			return "", nil, err
		}
		shards = append(shards, shard)
	}
	return newKeyspace, shards, nil
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

var (
	block3DES cipher.Block
	iv3DES    = make([]byte, 8)
)

func init() {
	var err error
	block3DES, err = des.NewTripleDESCipher(make([]byte, 24))
	if err != nil {
		panic(err)
	}
}

func vhash(shardKey uint64) key.KeyspaceId {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	encrypter := cipher.NewCBCEncrypter(block3DES, iv3DES)
	encrypter.CryptBlocks(hashed[:], keybytes[:])
	return key.KeyspaceId(hashed[:])
}

func vunhash(k key.KeyspaceId) uint64 {
	var unhashed [8]byte
	decrypter := cipher.NewCBCDecrypter(block3DES, iv3DES)
	decrypter.CryptBlocks(unhashed[:], []byte(k))
	return binary.BigEndian.Uint64(unhashed[:])
}
