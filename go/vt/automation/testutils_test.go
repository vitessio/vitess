// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"errors"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

func testingTaskCreator(taskName string) Task {
	switch taskName {
	// Tasks for testing only.
	case "TestingEchoTask":
		return &TestingEchoTask{}
	case "TestingEmitEchoTask":
		return &TestingEmitEchoTask{}
	case "TestingFailTask":
		return &TestingFailTask{}
	default:
		return nil
	}
}

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

// TestingFailTask is used only for testing. It always fails.
type TestingFailTask struct {
}

func (t *TestingFailTask) run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	return nil, "something went wrong", errors.New("full error message")
}

func (t *TestingFailTask) requiredParameters() []string {
	return []string{}
}
