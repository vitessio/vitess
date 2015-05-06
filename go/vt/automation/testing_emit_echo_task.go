// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// TestingEmitEchoTask is used only for testing. It emits a TestingEchoTask.
type TestingEmitEchoTask struct {
}

func (t *TestingEmitEchoTask) run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	return []*pb.TaskContainer{
		NewTaskContainerWithSingleTask("TestingEchoTask", parameters),
	}, "emitted TestingEchoTask", nil
}

func (t *TestingEmitEchoTask) requiredParameters() []string {
	return []string{}
}
