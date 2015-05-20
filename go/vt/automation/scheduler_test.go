// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"testing"
	"time"

	context "golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

func TestEnqueueClusterOperation(t *testing.T) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	scheduler.registerClusterOperation("ShellTask",
		pb.TaskContainerSpec{
			Type: pb.TaskContainerSpecType_SINGLE_TASK,
			SingleTask: &pb.TaskSpec{
				Name: "ShellTask",
				Parameter: []*pb.TaskParameter{
					&pb.TaskParameter{
						Name:  "command",
						Value: &pb.Value{Value: []string{"/bin/echo", "test"}},
					},
				},
			},
		})

	request := &pb.EnqueueClusterOperationRequest{
		Name: "ShellTask",
	}
	resp, err := scheduler.EnqueueClusterOperation(context.TODO(), request)
	if err != nil {
		t.Errorf("Failed to start cluster operation. Request: %v", resp)
	}

	// TODO(mberlin): Replace this hack with a clean start and shutdown.
	go scheduler.Run()
	time.Sleep(50 * time.Millisecond)
}

func TestEnqueueTwoIdenticalTasks(t *testing.T) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	scheduler.registerClusterOperation("TwoShellTasks",
		pb.TaskContainerSpec{
			Type: pb.TaskContainerSpecType_SERIAL_TASKS,
			SerialTask: []*pb.TaskContainerSpec{
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_SINGLE_TASK,
					SingleTask: &pb.TaskSpec{
						Name: "ShellTask",
						Parameter: []*pb.TaskParameter{
							&pb.TaskParameter{
								Name:  "command",
								Value: &pb.Value{Value: []string{"/bin/echo", "one"}},
							},
						},
						OutputKey: "first-command",
					},
				},
				&pb.TaskContainerSpec{
					Type: pb.TaskContainerSpecType_SINGLE_TASK,
					SingleTask: &pb.TaskSpec{
						Name: "ShellTask",
						Parameter: []*pb.TaskParameter{
							&pb.TaskParameter{
								Name:  "command",
								Value: &pb.Value{Value: []string{"/bin/echo", "${first-command}"}},
							},
						},
					},
				},
			},
		})

	request := &pb.EnqueueClusterOperationRequest{
		Name: "TwoShellTasks",
	}
	resp, err := scheduler.EnqueueClusterOperation(context.TODO(), request)
	if err != nil {
		t.Errorf("Failed to start cluster operation. Request: %v", resp)
	}

	// TODO(mberlin): Replace this hack with a clean start and shutdown.
	go scheduler.Run()
	time.Sleep(50 * time.Millisecond)
}
