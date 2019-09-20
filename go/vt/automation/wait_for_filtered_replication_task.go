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

package automation

import (
	"golang.org/x/net/context"
	automationpb "vitess.io/vitess/go/vt/proto/automation"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// WaitForFilteredReplicationTask runs vtctl WaitForFilteredReplication to block until the destination master
// (i.e. the receiving side of the filtered replication) has caught up to max_delay with the source shard.
type WaitForFilteredReplicationTask struct {
}

// Run is part of the Task interface.
func (t *WaitForFilteredReplicationTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	keyspaceAndShard := topoproto.KeyspaceShardString(parameters["keyspace"], parameters["shard"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"],
		[]string{"WaitForFilteredReplication", "-max_delay", parameters["max_delay"], keyspaceAndShard})
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *WaitForFilteredReplicationTask) RequiredParameters() []string {
	return []string{"keyspace", "shard", "max_delay", "vtctld_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *WaitForFilteredReplicationTask) OptionalParameters() []string {
	return nil
}
