// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	//	"crypto/md5"
	"encoding/hex"
	"hash"
	//	"hash/crc64"
	"os"

	"github.com/youtube/vitess/go/cgzip"
)

// Use this to simulate failures in tests
var (
	simulateFailures = false
)

func init() {
	_, statErr := os.Stat("/tmp/vtSimulateFetchFailures")
	simulateFailures = statErr == nil
}

// our hasher, implemented using md5
// type hasher struct {
// 	hash.Hash
// }

// func newHasher() *hasher {
// 	return &hasher{md5.New()}
// }

// func (h *hasher) HashString() string {
// 	return hex.EncodeToString(h.Sum(nil))
// }

// our hasher, implemented using crc64
//type hasher struct {
//	hash.Hash64
//}

//func newHasher() *hasher {
//	return &hasher{crc64.New(crc64.MakeTable(crc64.ECMA))}
//}

//func (h *hasher) HashString() string {
//	return hex.EncodeToString(h.Sum(nil))
//}

// our hasher, implemented using cgzip crc32
type hasher struct {
	hash.Hash32
}

func newHasher() *hasher {
	return &hasher{cgzip.NewCrc32()}
}

func (h *hasher) HashString() string {
	return hex.EncodeToString(h.Sum(nil))
}
