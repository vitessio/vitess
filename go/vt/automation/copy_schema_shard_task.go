// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

// CopySchemaShardTask runs vtctl CopySchemaShard to copy the schema from one shard to another.
type CopySchemaShardTask struct {
}

// Run is part of the Task interface.
func (t *CopySchemaShardTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	args := []string{"CopySchemaShard"}
	if tables := parameters["tables"]; tables != "" {
		args = append(args, "--tables="+tables)
	}
	if excludeTables := parameters["exclude_tables"]; excludeTables != "" {
		args = append(args, "--exclude_tables="+excludeTables)
	}
	args = append(args, parameters["source_keyspace_and_shard"], parameters["dest_keyspace_and_shard"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"], args)
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *CopySchemaShardTask) RequiredParameters() []string {
	return []string{"source_keyspace_and_shard", "dest_keyspace_and_shard", "vtctld_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *CopySchemaShardTask) OptionalParameters() []string {
	return []string{"tables", "exclude_tables"}
}
