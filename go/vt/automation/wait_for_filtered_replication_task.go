package automation

import (
	"context"

	automationpb "vitess.io/vitess/go/vt/proto/automation"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// WaitForFilteredReplicationTask runs vtctl WaitForFilteredReplication to block until the destination primary
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
