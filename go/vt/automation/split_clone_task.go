// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

// SplitCloneTask runs SplitClone on a remote vtworker to split an existing shard.
type SplitCloneTask struct {
}

// Run is part of the Task interface.
func (t *SplitCloneTask) Run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	keyspaceAndSourceShard := fmt.Sprintf("%v/%v", parameters["keyspace"], parameters["source_shard"])
	// TODO(mberlin): Add parameters for the following options?
	//                        '--source_reader_count', '1',
	//                        '--destination_pack_count', '1',
	//                        '--destination_writer_count', '1',
	//                        '--strategy=-populate_blp_checkpoint',
	output, err := ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"],
		[]string{"SplitClone", "--strategy=-populate_blp_checkpoint", keyspaceAndSourceShard})

	// TODO(mberlin): Remove explicit reset when vtworker supports it implicility.
	if err == nil {
		// Ignore output and error of the Reset.
		ExecuteVtworker(context.TODO(), parameters["vtworker_endpoint"], []string{"Reset"})
	}
	return nil, output, err
}

// RequiredParameters is part of the Task interface.
func (t *SplitCloneTask) RequiredParameters() []string {
	return []string{"keyspace", "source_shard", "vtworker_endpoint"}
}
