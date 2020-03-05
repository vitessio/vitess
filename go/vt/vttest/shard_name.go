/*
Copyright 2019 The Vitess Authors.

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

package vttest

import (
	"fmt"
)

// GetShardName returns an appropriate shard name, as a string.
// A single shard name is simply 0; otherwise it will attempt to split up 0x100
// into multiple shards.  For example, in a two sharded keyspace, shard 0 is
// -80, shard 1 is 80-.  This function currently only applies to sharding setups
// where the shard count is 256 or less, and all shards are equal width.
func GetShardName(shard, total int) string {
	width := 0x100 / total
	switch {
	case total == 1:
		return "0"
	case shard == 0:
		return fmt.Sprintf("-%02x", width)
	case shard == total-1:
		return fmt.Sprintf("%02x-", shard*width)
	default:
		return fmt.Sprintf("%02x-%02x", shard*width, (shard+1)*width)
	}
}

// GetShardNames creates a slice of shard names for N shards
func GetShardNames(total int) (names []string) {
	for i := 0; i < total; i++ {
		names = append(names, GetShardName(i, total))
	}
	return
}
