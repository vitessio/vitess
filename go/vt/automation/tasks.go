/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
