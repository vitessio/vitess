// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Helper functions for "Task" protobuf message.

// MarkTaskSucceeded marks the task as done.
func MarkTaskSucceeded(t *automationpb.Task, output string) {
	t.State = automationpb.TaskState_DONE
	t.Output = output
}

// MarkTaskFailed marks the task as failed.
func MarkTaskFailed(t *automationpb.Task, output string, err error) {
	t.State = automationpb.TaskState_DONE
	t.Output = output
	t.Error = err.Error()
}

// NewTask creates a new task protobuf message for "taskName" with "parameters".
func NewTask(taskName string, parameters map[string]string) *automationpb.Task {
	return &automationpb.Task{
		State:      automationpb.TaskState_NOT_STARTED,
		Name:       taskName,
		Parameters: parameters,
	}
}
