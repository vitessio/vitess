// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

// TaskNode represents a node in the execution graph of tasks.
// The execution graph is always a tree and can be visualized as a combination of a treeview and a matrix:
// Sequential tasks can be presented as rows and parallel tasks as columns witin a row.
// Consequently, each node can have two pointers:
// a) a pointer "to the right" for another parallel task
// b) a pointer "to the bottom" for another sequential task
// For example:
// - sequential task 1
// |- sub task 1a
// |- sub task 1b
// - parallel task 2a_shard1 |  parallel task 2a_shard2
//   |-       task 2b_shard1 |           task 2b_shard2
// - sequential task 3
type TaskNode struct {
	taskname   string
	parameters map[string]string

	nextParallelTask   *TaskNode
	nextSequentialTask *TaskNode
}

// NewTaskNode creates an empty task node.
// It will be filled by the first sequential or parallel task which will be added to it.
func NewTaskNode() *TaskNode {
	return &TaskNode{}
}

// AddSequentialTask adds a new sequential task at the end of the tree (i.e. it adds a row).
// The return value is a pointer to the subtree starting at the new task.
func (tn *TaskNode) AddSequentialTask(name string, parameters map[string]string) (*TaskNode, error) {
	return nil, nil
}

// AddEmptySequentialTask adds a new sequential task which may be filled in later.
// Use this method to effectively "finish" the current row and add a new row.
func (tn *TaskNode) AddEmptySequentialTask() (*TaskNode, error) {
	return nil, nil
}

// AddParallelTask adds a new parallel task next to the current task (i.e. it adds a column).
// The return value is a pointer to the subtree starting at the new task.
func (tn *TaskNode) AddParallelTask(name string, parameters map[string]string) (*TaskNode, error) {
	return nil, nil
}
