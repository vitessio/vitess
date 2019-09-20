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

// MigrateServedTypesTask runs vtctl MigrateServedTypes to migrate a serving
// type from the source shard to the shards that it replicates to.
type MigrateServedTypesTask struct {
}

// Run is part of the Task interface.
func (t *MigrateServedTypesTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	args := []string{"MigrateServedTypes"}
	if cells := parameters["cells"]; cells != "" {
		args = append(args, "--cells="+cells)
	}
	if reverse := parameters["reverse"]; reverse != "" {
		args = append(args, "--reverse="+reverse)
	}
	args = append(args,
		topoproto.KeyspaceShardString(parameters["keyspace"], parameters["source_shard"]),
		parameters["type"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *MigrateServedTypesTask) RequiredParameters() []string {
	return []string{"keyspace", "source_shard", "type", "vtctld_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *MigrateServedTypesTask) OptionalParameters() []string {
	return []string{"cells", "reverse"}
}
