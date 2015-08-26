// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

// MigrateServedTypesTask runs vtctl MigrateServedTypes to migrate a serving
// type from the source shard to the shards that it replicates to.
type MigrateServedTypesTask struct {
}

// Run is part of the Task interface.
func (t *MigrateServedTypesTask) Run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	keyspaceAndShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["source_shard"])
	args := []string{"MigrateServedTypes"}
	if reverse, ok := parameters["reverse"]; ok {
		args = append(args, "--reverse="+reverse)
	}
	args = append(args, keyspaceAndShard, parameters["type"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *MigrateServedTypesTask) RequiredParameters() []string {
	return []string{"keyspace", "source_shard", "type", "vtctld_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *MigrateServedTypesTask) OptionalParameters() []string {
	return []string{"reverse"}
}
