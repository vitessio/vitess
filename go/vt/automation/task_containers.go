// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Helper functions for "TaskContainer" protobuf message.

// NewTaskContainerWithSingleTask creates a new task container with exactly one task.
func NewTaskContainerWithSingleTask(taskName string, parameters map[string]string) *automationpb.TaskContainer {
	return &automationpb.TaskContainer{
		ParallelTasks: []*automationpb.Task{
			NewTask(taskName, parameters),
		},
	}
}

// NewTaskContainer creates an empty task container. Use AddTask() to add tasks to it.
func NewTaskContainer() *automationpb.TaskContainer {
	return &automationpb.TaskContainer{
		ParallelTasks: []*automationpb.Task{},
	}
}

// AddTask adds a new task to an existing task container.
func AddTask(t *automationpb.TaskContainer, taskName string, parameters map[string]string) {
	t.ParallelTasks = append(t.ParallelTasks, NewTask(taskName, parameters))
}

// AddMissingTaskID assigns a task id to each task in "tc".
func AddMissingTaskID(tc []*automationpb.TaskContainer, taskIDGenerator *IDGenerator) {
	for _, taskContainer := range tc {
		for _, task := range taskContainer.ParallelTasks {
			if task.Id == "" {
				task.Id = taskIDGenerator.GetNextID()
			}
		}
	}
}
