// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"github.com/golang/protobuf/proto"
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// ClusterOperationInstance is a runtime type which enhances the protobuf message "ClusterOperation" with runtime specific data.
// Unlike the protobuf message, the additional runtime data will not be part of a checkpoint.
// Methods of this struct are not thread-safe.
type ClusterOperationInstance struct {
	pb.ClusterOperation
	taskIDGenerator *IDGenerator
}

// NewClusterOperationInstance creates a new cluster operation instance with one initial task.
func NewClusterOperationInstance(clusterOpID string, initialTask *pb.TaskContainer, taskIDGenerator *IDGenerator) ClusterOperationInstance {
	c := ClusterOperationInstance{
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

// InsertTaskContainers  inserts "newTaskContainers" at pos in the current list of task containers. Existing task containers will be moved after the new task containers.
func (c *ClusterOperationInstance) InsertTaskContainers(newTaskContainers []*pb.TaskContainer, pos int) {
	AddMissingTaskID(newTaskContainers, c.taskIDGenerator)

	newSerialTasks := make([]*pb.TaskContainer, len(c.SerialTasks)+len(newTaskContainers))
	copy(newSerialTasks, c.SerialTasks[:pos])
	copy(newSerialTasks[pos:], newTaskContainers)
	copy(newSerialTasks[pos+len(newTaskContainers):], c.SerialTasks[pos:])
	c.SerialTasks = newSerialTasks
}

// Clone creates a deep copy of the inner protobuf.
// Other elements e.g. taskIDGenerator are not deep-copied.
func (c ClusterOperationInstance) Clone() ClusterOperationInstance {
	var clone = c
	clone.ClusterOperation = *(proto.Clone(&c.ClusterOperation).(*pb.ClusterOperation))
	return clone
}
