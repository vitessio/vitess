// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// ClusterOperationInstance is a runtime type which enhances the protobuf message "ClusterOperation" with runtime specific data.
// Unlike the protobuf message, the additional runtime data will not be part of a checkpoint.
type ClusterOperationInstance struct {
	pb.ClusterOperation
	taskIDGenerator *IDGenerator
}

// NewClusterOperationInstance creates a new cluster operation instance with one initial task.
func NewClusterOperationInstance(clusterOpID string, initialTask *pb.TaskContainer, taskIDGenerator *IDGenerator) *ClusterOperationInstance {
	c := &ClusterOperationInstance{
		pb.ClusterOperation{
			Id:          clusterOpID,
			SerialTasks: []*pb.TaskContainer{},
			State:       pb.ClusterOperationState_CLUSTER_OPERATION_NOT_STARTED,
		},
		taskIDGenerator,
	}
	c.InsertTaskContainers([]*pb.TaskContainer{initialTask}, 0)
	return c
}

// addMissingTaskID assigns a task id to each task in "tc".
func (c *ClusterOperationInstance) addMissingTaskID(tc []*pb.TaskContainer) {
	for _, taskContainer := range tc {
		for _, task := range taskContainer.ParallelTasks {
			if task.Id == "" {
				task.Id = c.taskIDGenerator.GetNextID()
			}
		}
	}
}

// InsertTaskContainers  inserts "newTaskContainers" at pos in the current list of task containers. Existing task containers will be moved after the new task containers.
func (c *ClusterOperationInstance) InsertTaskContainers(newTaskContainers []*pb.TaskContainer, pos int) {
	c.addMissingTaskID(newTaskContainers)

	newSerialTasks := make([]*pb.TaskContainer, len(c.SerialTasks)+len(newTaskContainers))
	copy(newSerialTasks, c.SerialTasks[:pos])
	copy(newSerialTasks[pos:], newTaskContainers)
	copy(newSerialTasks[pos+len(newTaskContainers):], c.SerialTasks[pos:])
	c.SerialTasks = newSerialTasks
}
