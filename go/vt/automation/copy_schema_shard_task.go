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

func (t *CopySchemaShardTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	sourceShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["source_shard"])
	destShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["dest_shard"])
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"], []string{"CopySchemaShard", sourceShard, destShard})
	return nil, output, err
}

func (t *CopySchemaShardTask) requiredParameters() []string {
	return []string{"keyspace", "source_shard", "dest_shard", "vtctld_endpoint"}
}
