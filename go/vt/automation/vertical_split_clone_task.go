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
	"context"

	automationpb "vitess.io/vitess/go/vt/proto/automation"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// VerticalSplitCloneTask runs VerticalSplitClone on a remote vtworker to
// split out tables from an existing keyspace to a different keyspace.
type VerticalSplitCloneTask struct {
}

// Run is part of the Task interface.
func (t *VerticalSplitCloneTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	// Run a "Reset" first to clear the state of a previous finished command.
	// This reset is best effort. We ignore the output and error of it.
	// TODO(mberlin): Remove explicit reset when vtworker supports it implicility.
	ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], []string{"Reset"})

	// TODO(mberlin): Add parameters for the following options?
	//                        '--source_reader_count', '1',
	//                        '--destination_writer_count', '1',
	args := []string{"VerticalSplitClone"}
	args = append(args, "--tables="+parameters["tables"])
	if online := parameters["online"]; online != "" {
		args = append(args, "--online="+online)
	}
	if offline := parameters["offline"]; offline != "" {
		args = append(args, "--offline="+offline)
	}
	if chunkCount := parameters["chunk_count"]; chunkCount != "" {
		args = append(args, "--chunk_count="+chunkCount)
	}
	if minRowsPerChunk := parameters["min_rows_per_chunk"]; minRowsPerChunk != "" {
		args = append(args, "--min_rows_per_chunk="+minRowsPerChunk)
	}
	if writeQueryMaxRows := parameters["write_query_max_rows"]; writeQueryMaxRows != "" {
		args = append(args, "--write_query_max_rows="+writeQueryMaxRows)
	}
	if writeQueryMaxSize := parameters["write_query_max_size"]; writeQueryMaxSize != "" {
		args = append(args, "--write_query_max_size="+writeQueryMaxSize)
	}
	if minHealthyRdonlyTablets := parameters["min_healthy_rdonly_tablets"]; minHealthyRdonlyTablets != "" {
		args = append(args, "--min_healthy_rdonly_tablets="+minHealthyRdonlyTablets)
	}
	if maxTPS := parameters["max_tps"]; maxTPS != "" {
		args = append(args, "--max_tps="+maxTPS)
	}
	if maxReplicationLag := parameters["max_replication_lag"]; maxReplicationLag != "" {
		args = append(args, "--max_replication_lag="+maxReplicationLag)
	}
	args = append(args, topoproto.KeyspaceShardString(parameters["dest_keyspace"], parameters["shard"]))

	output, err := ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *VerticalSplitCloneTask) RequiredParameters() []string {
	return []string{"dest_keyspace", "shard", "tables", "vtworker_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *VerticalSplitCloneTask) OptionalParameters() []string {
	return []string{"online", "offline", "chunk_count", "min_rows_per_chunk", "write_query_max_rows", "write_query_max_size", "min_healthy_rdonly_tablets", "max_tps", "max_replication_lag"}
}
