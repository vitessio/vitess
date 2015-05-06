// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Helper functions for "Task" protobuf message.

// MarkTaskSucceeded marks the task as done.
func MarkTaskSucceeded(t *pb.Task, output string) {
	t.State = pb.TaskState_DONE
	t.Output = output
}

// MarkTaskFailed marks the task as failed.
func MarkTaskFailed(t *pb.Task, err error) {
	t.State = pb.TaskState_DONE
	t.Error = err.Error()
}

// NewTask creates a new task protobuf message for "taskName" with "parameters".
func NewTask(taskName string, parameters map[string]string) *pb.Task {
	return &pb.Task{
		State:      pb.TaskState_NOT_STARTED,
		Name:       taskName,
		Parameters: parameters,
	}
}
