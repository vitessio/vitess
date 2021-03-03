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
)

// RebuildKeyspaceGraphTask runs vtctl RebuildKeyspaceGraph to migrate a serving
// type from the source shard to the shards that it replicates to.
type RebuildKeyspaceGraphTask struct {
}

// Run is part of the Task interface.
func (t *RebuildKeyspaceGraphTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	args := []string{"RebuildKeyspaceGraph"}
	if cells := parameters["cells"]; cells != "" {
		args = append(args, "--cells="+cells)
	}
	args = append(args, parameters["keyspace"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *RebuildKeyspaceGraphTask) RequiredParameters() []string {
	return []string{"keyspace", "vtctld_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *RebuildKeyspaceGraphTask) OptionalParameters() []string {
	return []string{"cells"}
}
