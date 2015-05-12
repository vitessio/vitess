// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// TestingEchoTask is used only for testing. It returns the join of all parameter values.
type TestingEchoTask struct {
}

func (t *TestingEchoTask) run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	for _, v := range parameters {
		output += v
	}
	return
}

func (t *TestingEchoTask) requiredParameters() []string {
	return []string{"echo_text"}
}
