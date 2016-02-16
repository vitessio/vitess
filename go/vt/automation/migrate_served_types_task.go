// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
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
