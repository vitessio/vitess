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

This test simulates the first time a database has to be split.

- we start with a keyspace with a single shard and a single table
- we add and populate the sharding key
- we set the sharding key in the topology
- we clone into 2 instances
- we enable filtered replication
- we move all serving types
- we remove the source tablets
- we remove the original shard

*/

package v3

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/initialsharding"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func TestInitialSharding(t *testing.T) {
	code, err := sharding.ClusterWrapper()
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}
	sharding.TestInitialShardingWithVersion(t, 3, topodata.KeyspaceIdType_UINT64)
	defer sharding.ClusterInstance.Teardown()
}
