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

package topoproto

import (
	"testing"
)

func TestParseKeyspaceShard(t *testing.T) {
	zkPath := "/zk/global/ks/sh"
	keyspace := "key01"
	shard := "shard0"

	for _, delim := range []string{"/", ":"} {
		keyspaceShard := keyspace + delim + shard
		if _, _, err := ParseKeyspaceShard(zkPath); err == nil {
			t.Errorf("zk path: %s should cause error.", zkPath)
		}
		k, s, err := ParseKeyspaceShard(keyspaceShard)
		if err != nil {
			t.Fatalf("Failed to parse valid keyspace%sshard pair: %s", delim, keyspaceShard)
		}
		if k != keyspace {
			t.Errorf("keyspace parsed from keyspace%sshard pair %s is %s, but expect %s", delim, keyspaceShard, k, keyspace)
		}
		if s != shard {
			t.Errorf("shard parsed from keyspace%sshard pair %s is %s, but expect %s", delim, keyspaceShard, s, shard)
		}
	}
}
