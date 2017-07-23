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
