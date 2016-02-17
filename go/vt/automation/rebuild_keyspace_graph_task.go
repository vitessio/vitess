// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
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
