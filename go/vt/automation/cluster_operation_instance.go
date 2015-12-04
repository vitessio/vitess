// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"github.com/golang/protobuf/proto"
	automationpb "github.com/youtube/vitess/go/vt/proto/automation"
)

// ClusterOperationInstance is a runtime type which enhances the protobuf message "ClusterOperation" with runtime specific data.
// Unlike the protobuf message, the additional runtime data will not be part of a checkpoint.
// Methods of this struct are not thread-safe.
type ClusterOperationInstance struct {
	automationpb.ClusterOperation
	taskIDGenerator *IDGenerator
}

// NewClusterOperationInstance creates a new cluster operation instance with one initial task.
func NewClusterOperationInstance(clusterOpID string, initialTask *automationpb.TaskContainer, taskIDGenerator *IDGenerator) ClusterOperationInstance {
	c := ClusterOperationInstance{
		automationpb.ClusterOperation{
			Id:          clusterOpID,
			SerialTasks: []*automationpb.TaskContainer{},
			State:       automationpb.ClusterOperationState_CLUSTER_OPERATION_NOT_STARTED,
		},
		taskIDGenerator,
	}
	c.InsertTaskContainers([]*automationpb.TaskContainer{initialTask}, 0)
	return c
}

// InsertTaskContainers  inserts "newTaskContainers" at pos in the current list of task containers. Existing task containers will be moved after the new task containers.
func (c *ClusterOperationInstance) InsertTaskContainers(newTaskContainers []*automationpb.TaskContainer, pos int) {
	AddMissingTaskID(newTaskContainers, c.taskIDGenerator)

	newSerialTasks := make([]*automationpb.TaskContainer, len(c.SerialTasks)+len(newTaskContainers))
	copy(newSerialTasks, c.SerialTasks[:pos])
	copy(newSerialTasks[pos:], newTaskContainers)
	copy(newSerialTasks[pos+len(newTaskContainers):], c.SerialTasks[pos:])
	c.SerialTasks = newSerialTasks
}

// Clone creates a deep copy of the inner protobuf.
// Other elements e.g. taskIDGenerator are not deep-copied.
func (c ClusterOperationInstance) Clone() ClusterOperationInstance {
	var clone = c
	clone.ClusterOperation = *(proto.Clone(&c.ClusterOperation).(*automationpb.ClusterOperation))
	return clone
}
