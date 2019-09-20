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
