// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
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
