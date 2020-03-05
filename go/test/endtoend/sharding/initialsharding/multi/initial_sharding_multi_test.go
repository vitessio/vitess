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

This test simulates the first time a database has to be split
in a multi-vttablet-single-mysql environment

We have 2 keyspaces. One keyspace is in managing mode. It's vttablets
own the MySQL instances and can reparent, start/stop server, start/stop
replication etc. Other keyspace is in non-managing mode and cannot do
any of these actions. Only TabletExternallyReparented is allowed, but
resharding should still work.

For each keyspace:
- we start with a keyspace with a single shard and a single table
- we add and populate the sharding key
- we set the sharding key in the topology
- we clone into 2 instances
- we enable filtered replication
- we move all serving types
- we remove the source tablets
- we remove the original shard

*/

package multi

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	sharding "vitess.io/vitess/go/test/endtoend/sharding/initialsharding"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestInitialShardingMulti(t *testing.T) {
	defer cluster.PanicHandler(t)
	code, err := sharding.ClusterWrapper(true)
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}
	sharding.AssignMysqlPortFromKs1ToKs2()
	sharding.TestInitialSharding(t, &sharding.ClusterInstance.Keyspaces[0], querypb.Type_UINT64, true, false)
	println("-----------------------------")
	println("Done with 1st keyspace test")
	println("-----------------------------")
	sharding.TestInitialSharding(t, &sharding.ClusterInstance.Keyspaces[1], querypb.Type_UINT64, true, true)
	println("----------Done with 2nd keyspace test----------")
	sharding.KillVtgateInstances()
	sharding.KillTabletsInKeyspace(&sharding.ClusterInstance.Keyspaces[0])
	sharding.KillTabletsInKeyspace(&sharding.ClusterInstance.Keyspaces[1])
	defer sharding.ClusterInstance.Teardown()
}
