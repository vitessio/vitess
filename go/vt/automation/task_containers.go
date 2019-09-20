/*
Copyright 2019 The Vitess Authors.

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
	automationpb "vitess.io/vitess/go/vt/proto/automation"
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
