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

// VerticalSplitDiffTask runs VerticalSplitDiff on a remote vtworker to compare
// the split out tables against the source keyspace.
type VerticalSplitDiffTask struct {
}

// Run is part of the Task interface.
func (t *VerticalSplitDiffTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	// Run a "Reset" first to clear the state of a previous finished command.
	// This reset is best effort. We ignore the output and error of it.
	// TODO(mberlin): Remove explicit reset when vtworker supports it implicility.
	ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], []string{"Reset"})

	args := []string{"VerticalSplitDiff"}
	if minHealthyRdonlyTablets := parameters["min_healthy_rdonly_tablets"]; minHealthyRdonlyTablets != "" {
		args = append(args, "--min_healthy_rdonly_tablets="+minHealthyRdonlyTablets)
	}
	args = append(args, topoproto.KeyspaceShardString(parameters["dest_keyspace"], parameters["shard"]))

	output, err := ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *VerticalSplitDiffTask) RequiredParameters() []string {
	return []string{"dest_keyspace", "shard", "vtworker_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *VerticalSplitDiffTask) OptionalParameters() []string {
	return []string{"min_healthy_rdonly_tablets"}
}
