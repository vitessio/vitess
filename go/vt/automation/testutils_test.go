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
	case "TestingFailTask":
		return &TestingFailTask{}
	case "TestingEmitEchoTask":
		return &TestingEmitEchoTask{}
	case "TestingEmitEchoFailEchoTask":
		return &TestingEmitEchoFailEchoTask{}
	default:
		return nil
	}
}

// TestingEchoTask is used only for testing. It returns the join of all parameter values.
type TestingEchoTask struct {
}

func (t *TestingEchoTask) Run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	for _, v := range parameters {
		output += v
	}
	return
}

func (t *TestingEchoTask) RequiredParameters() []string {
	return []string{"echo_text"}
}

// TestingFailTask is used only for testing. It always fails.
type TestingFailTask struct {
}

func (t *TestingFailTask) Run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	return nil, "something went wrong", errors.New("full error message")
}

func (t *TestingFailTask) RequiredParameters() []string {
	return []string{}
}

// TestingEmitEchoTask is used only for testing. It emits a TestingEchoTask.
type TestingEmitEchoTask struct {
}

func (t *TestingEmitEchoTask) Run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	return []*pb.TaskContainer{
		NewTaskContainerWithSingleTask("TestingEchoTask", parameters),
	}, "emitted TestingEchoTask", nil
}

func (t *TestingEmitEchoTask) RequiredParameters() []string {
	return []string{}
}

// TestingEmitEchoFailEchoTask is used only for testing.
// It emits three sequential tasks: Echo, Fail, Echo.
type TestingEmitEchoFailEchoTask struct {
}

func (t *TestingEmitEchoFailEchoTask) Run(parameters map[string]string) (newTasks []*pb.TaskContainer, output string, err error) {
	newTasks = []*pb.TaskContainer{
		NewTaskContainerWithSingleTask("TestingEchoTask", parameters),
		NewTaskContainerWithSingleTask("TestingFailTask", parameters),
		NewTaskContainerWithSingleTask("TestingEchoTask", parameters),
	}
	return newTasks, "emitted tasks: Echo, Fail, Echo", nil
}

func (t *TestingEmitEchoFailEchoTask) RequiredParameters() []string {
	return []string{}
}
