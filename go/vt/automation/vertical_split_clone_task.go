// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
)

// VerticalSplitCloneTask runs VerticalSplitClone on a remote vtworker to
// split out tables from an existing keyspace to a different keyspace.
type VerticalSplitCloneTask struct {
}

// Run is part of the Task interface.
func (t *VerticalSplitCloneTask) Run(parameters map[string]string) ([]*automationpb.TaskContainer, string, error) {
	// TODO(mberlin): Add parameters for the following options?
	//                        '--source_reader_count', '1',
	//                        '--destination_writer_count', '1',
	args := []string{"VerticalSplitClone"}
	args = append(args, "--tables="+parameters["tables"])
	if destinationPackCount := parameters["destination_pack_count"]; destinationPackCount != "" {
		args = append(args, "--destination_pack_count="+destinationPackCount)
	}
	if minHealthyRdonlyTablets := parameters["min_healthy_rdonly_tablets"]; minHealthyRdonlyTablets != "" {
		args = append(args, "--min_healthy_rdonly_tablets="+minHealthyRdonlyTablets)
	}
	args = append(args, topoproto.KeyspaceShardString(parameters["dest_keyspace"], parameters["shard"]))
	output, err := ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], args)

	// TODO(mberlin): Remove explicit reset when vtworker supports it implicility.
	if err == nil {
		// Ignore output and error of the Reset.
		ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], []string{"Reset"})
	}
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *VerticalSplitCloneTask) RequiredParameters() []string {
	return []string{"dest_keyspace", "shard", "tables", "vtworker_endpoint"}
}

// OptionalParameters is part of the Task interface.
func (t *VerticalSplitCloneTask) OptionalParameters() []string {
	return []string{"destination_pack_count", "min_healthy_rdonly_tablets"}
}
