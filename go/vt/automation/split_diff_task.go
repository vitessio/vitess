// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

// SplitDiffTask runs SplitDiff on a remote vtworker to compare the old shard against its new split shards.
type SplitDiffTask struct {
}

// Run is part of the Task interface.
func (t *SplitDiffTask) Run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	keyspaceAndDestShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["dest_shard"])
	output, err := ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"],
		[]string{"SplitDiff", keyspaceAndDestShard})

	// TODO(mberlin): Remove explicit reset when vtworker supports it implicility.
	if err == nil {
		// Ignore output and error of the Reset.
		ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], []string{"Reset"})
	}
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *SplitDiffTask) RequiredParameters() []string {
	return []string{"keyspace", "dest_shard", "vtworker_endpoint"}
}
