// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

// ChangeSlaveTypeTask runs vtctl ChangeSlaveType e.g. to return an rdonly tablet to the serving graph.
type ChangeSlaveTypeTask struct {
}

func (t *ChangeSlaveTypeTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	output, err := ExecuteVtctl(context.TODO(), parameters["vtctld_endpoint"],
		[]string{"ChangeSlaveType", parameters["tablet_alias"], parameters["tablet_type"]})
	return nil, output, err
}

func (t *ChangeSlaveTypeTask) requiredParameters() []string {
	return []string{"tablet_alias", "tablet_type", "vtctld_endpoint"}
}
