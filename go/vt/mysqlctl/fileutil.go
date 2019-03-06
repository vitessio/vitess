/*
Copyright 2018 The Vitess Authors.

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
	"encoding/hex"
	"hash"
	"hash/crc32"
	"os"
)

// Use this to simulate failures in tests
var (
	simulateFailures = false
)

func init() {
	_, statErr := os.Stat("/tmp/vtSimulateFetchFailures")
	simulateFailures = statErr == nil
}

// our hasher, implemented using crc32
type hasher struct {
	hash.Hash32
}

func newHasher() *hasher {
	return &hasher{crc32.NewIEEE()}
}

func (h *hasher) HashString() string {
	return hex.EncodeToString(h.Sum(nil))
}
