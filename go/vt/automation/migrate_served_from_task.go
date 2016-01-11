// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
)

// MigrateServedFromTask runs vtctl MigrateServedFrom to let vertically split
// out tables get served from the new destination keyspace.
type MigrateServedFromTask struct {
}

// Run is part of the Task interface.
func (t *MigrateServedFromTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	args := []string{"MigrateServedFrom"}
	if cells := parameters["cells"]; cells != "" {
		args = append(args, "--cells="+cells)
	}
	if reverse := parameters["reverse"]; reverse != "" {
		args = append(args, "--reverse="+reverse)
	}
	args = append(args,
		topoproto.KeyspaceShardString(parameters["dest_keyspace"], parameters["shard"]),
		parameters["type"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *MigrateServedFromTask) RequiredParameters() []string {
	return []string{"dest_keyspace", "shard", "type", "vtctld_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *MigrateServedFromTask) OptionalParameters() []string {
	return []string{"cells", "reverse"}
}
