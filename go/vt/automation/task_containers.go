// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Helper functions for "TaskContainer" protobuf message.

// NewTaskContainerWithSingleTask creates a new task container with exactly one task.
func NewTaskContainerWithSingleTask(taskName string, parameters map[string]string) *pb.TaskContainer {
	return &pb.TaskContainer{
		ParallelTasks: []*pb.Task{
			NewTask(taskName, parameters),
		},
	}
}

// NewTaskContainer creates an empty task container. Use AddTask() to add tasks to it.
func NewTaskContainer() *pb.TaskContainer {
	return &pb.TaskContainer{
		ParallelTasks: []*pb.Task{},
	}
}

// AddTask adds a new task to an existing task container.
func AddTask(t *pb.TaskContainer, taskName string, parameters map[string]string) {
	t.ParallelTasks = append(t.ParallelTasks, NewTask(taskName, parameters))
}
