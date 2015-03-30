// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package automation contains code to execute high-level cluster operations
(e.g. resharding) as a series of low-level operations
(e.g. vtctl, shell commands, ...).
*/
package automation

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Scheduler executes automation tasks and maintains the execution state.
type Scheduler struct {
	clusterOperations map[string]pb.TaskContainerSpec

	toBeScheduledClusterOperations chan pb.TaskContainerSpec

	globalValues *ValueMap
}

// NewScheduler creates a new instance.
func NewScheduler() (*Scheduler, error) {
	defaultClusterOperations := map[string]pb.TaskContainerSpec{
		// Required input:
		// - source_shard_list
		// - shard_count
		"reshard": pb.TaskContainerSpec{
			Type: pb.TaskContainerSpecType_SERIAL_TASKS,
			SerialTask: []*pb.TaskContainerSpec{
				// Generate intermediate variables.
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_SINGLE_TASK,
					SingleTask: &pb.TaskSpec{
						Name: "ShardCountToShardList",
						Parameter: []*pb.TaskParameter{
							&pb.TaskParameter{
								Name:  "shard_count",
								Value: &pb.Value{Value: []string{"${shard_count}"}},
							},
						},
						OutputKey: "dest_shard_list",
					},
				},

				// Actual operation.
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "ForceReparentToAnyMaster",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
							},
						},
						CollectionReference: "dest_shard_list",
						Parallelism:         -1,
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "CopySchemaShardFromAnyRdonlyTablet",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "dest_shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
							},
						},
						CollectionReference: "dest_shard_list",
						Parallelism:         -1,
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "SplitClone",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "source_shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
							},
						},
						CollectionReference: "source_shard_list",
						Parallelism:         1,
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "SplitDiff",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "dest_shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
							},
						},
						CollectionReference: "dest_shard_list",
						Parallelism:         -1,
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "MigrateServedTypes",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "source_shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
								&pb.TaskParameter{
									Name:  "served_type",
									Value: &pb.Value{Value: []string{"rdonly"}},
								},
							},
						},
						CollectionReference: "source_shard_list",
						Parallelism:         -1,
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "MigrateServedTypes",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "source_shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
								&pb.TaskParameter{
									Name:  "served_type",
									Value: &pb.Value{Value: []string{"replica"}},
								},
							},
						},
						CollectionReference: "source_shard_list",
						Parallelism:         -1,
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_FOREACH_TASKS,
					ForeachTask: &pb.ForEachTaskSpec{
						Task: &pb.TaskSpec{
							Name: "MigrateServedTypes",
							Parameter: []*pb.TaskParameter{
								&pb.TaskParameter{
									Name:  "source_shard",
									Value: &pb.Value{Value: []string{"${i}"}},
								},
								&pb.TaskParameter{
									Name:  "served_type",
									Value: &pb.Value{Value: []string{"master"}},
								},
							},
						},
						CollectionReference: "source_shard_list",
						Parallelism:         -1,
					},
				},
			},
		},
	}

	return &Scheduler{
		clusterOperations:              defaultClusterOperations,
		toBeScheduledClusterOperations: make(chan pb.TaskContainerSpec, 10),
		globalValues:                   NewValueMap(),
	}, nil
}

func (s *Scheduler) registerClusterOperation(name string, op pb.TaskContainerSpec) {
	// TODO(mberlin): Do not overwrite an existing operation.
	s.clusterOperations[name] = op
}

// Run takes care of executing tasks. Blocks until the scheduler is shutdown.
func (s *Scheduler) Run() {
	// TODO(mberlin): Off-load executions to a pool of worker.
	for op := range s.toBeScheduledClusterOperations {
		s.processTaskContainer(op)
	}
}

func (s *Scheduler) processTaskContainer(taskContainer pb.TaskContainerSpec) {
	// TODO(mberlin): Add missing cases e.g. foreach.
	switch taskContainer.Type {
	case pb.TaskContainerSpecType_SINGLE_TASK:
		s.executeSingleTask(taskContainer.SingleTask)
	case pb.TaskContainerSpecType_SERIAL_TASKS:
		for _, serialTaskContainer := range taskContainer.SerialTask {
			s.processTaskContainer(*serialTaskContainer)
		}
	default:
		// TODO(mberlin): Add error handling.
	}
}

func (s *Scheduler) executeSingleTask(singleTask *pb.TaskSpec) error {
	// TODO(mberlin): Remove debug output.
	fmt.Printf("executing singleTask: %v\n", singleTask)
	parameters := make(map[string]*pb.Value)

	for _, taskParameter := range singleTask.Parameter {
		// TODO(mberlin): Assert that Value is not nil.
		// TODO(mberlin): Assert that all variables were expanded.
		input := expandVariables(*taskParameter.Value, s.globalValues)
		parameters[taskParameter.Name] = &input
	}
	singleTask.Input = parameters

	// TODO(mberlin): Check that the list of the task's required parameters is included in the parameters map.

	// TODO(mberlin): Remove debug output.
	fmt.Printf("running: %v params: %v\n", singleTask.Name, parameters)
	task := s.createTaskInstance(singleTask.Name)

	output, err := task.run(parameters)
	if err != nil {
		return err
	}

	// TODO(mberlin): Remove debug output.
	fmt.Printf("output: %v\n", output)
	singleTask.OutputValue = output
	s.globalValues.RecordValue(singleTask.OutputKey, *singleTask.OutputValue)

	return nil
}

func expandVariables(parameter pb.Value, vm *ValueMap) pb.Value {
	result := pb.Value{}

	// TODO(mberlin): Best way to move this out of the loop? Global constant? Pass as argument?
	re := regexp.MustCompile(`\${([^\${}]+)}`)
	for _, valueEntry := range parameter.Value {
		newValueEntry := valueEntry
		allSubmatches := re.FindAllStringSubmatch(newValueEntry, -1)
		for _, submatch := range allSubmatches {
			variableName := submatch[1]
			value, ok := vm.GetValue(variableName)
			if !ok {
				// TODO(mberlin): Error handling.
			}
			// TODO(mberlin): Fail on values which have more than one list entry or come up with additional features in the variable name syntax e.g. to join them all automatically using a separator.
			newValueEntry = strings.Replace(newValueEntry, "${"+variableName+"}", value.Value[0], -1)
		}
		result.Value = append(result.Value, newValueEntry)
	}

	return result
}

func (s *Scheduler) createTaskInstance(taskName string) Task {
	switch taskName {
	case "ShellTask":
		return &ShellTask{}
	default:
		return nil
	}
}

// EnqueueClusterOperation can be used to start a new cluster operation.
func (s *Scheduler) EnqueueClusterOperation(ctx context.Context, req *pb.EnqueueClusterOperationRequest) (*pb.EnqueueClusterOperationResponse, error) {
	// TODO(mberlin): Generate a globally unique id.
	id := "test"

	// TODO(mberlin): Return error when the operation is not known.
	clusterOp := s.clusterOperations[req.Name]
	s.toBeScheduledClusterOperations <- clusterOp

	return &pb.EnqueueClusterOperationResponse{
		Id: id,
	}, nil
}
