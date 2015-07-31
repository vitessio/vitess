// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

// CopySchemaShardTask runs vtctl CopySchemaShard to copy the schema from one shard to another.
type CopySchemaShardTask struct {
}

// Run is part of the Task interface.
func (t *CopySchemaShardTask) Run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	keyspaceAndSourceShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["source_shard"])
	keyspaceAndDestShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["dest_shard"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"],
		[]string{"CopySchemaShard", keyspaceAndSourceShard, keyspaceAndDestShard})
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *CopySchemaShardTask) RequiredParameters() []string {
	return []string{"keyspace", "source_shard", "dest_shard", "vtctld_endpoint"}
}
