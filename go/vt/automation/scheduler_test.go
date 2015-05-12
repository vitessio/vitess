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

func TestSchedulerImmediateShutdown(t *testing.T) {
	// Make sure that the scheduler shuts down cleanly when it was instantiated, but not started with Run().
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.ShutdownAndWait()
}

func TestEnqueueSingleTask(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingEchoTask", "echoed text")
}

func TestEnqueueEmittingTask(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingEmitEchoTask", "echoed text from emitted task")
}

func enqueueClusterOperationAndCheckOutput(t *testing.T, taskName string, expectedOutput string) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	scheduler.Run()

	enqueueRequest := &pb.EnqueueClusterOperationRequest{
		Name: taskName,
		Parameters: map[string]string{
			"echo_text": expectedOutput,
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.TODO(), enqueueRequest)
	if err != nil {
		t.Fatalf("Failed to start cluster operation. Request: %v Error: %v", enqueueRequest, err)
	}

	getDetailsRequest := &pb.GetClusterOperationDetailsRequest{
		Id: enqueueResponse.Id,
	}
	for {
		getDetailsResponse, err := scheduler.GetClusterOperationDetails(context.TODO(), getDetailsRequest)
		if err != nil {
			t.Fatalf("Failed to get details for cluster operation. Request: %v Error: %v", getDetailsRequest, err)
		}
		if getDetailsResponse.ClusterOp.State == pb.ClusterOperationState_CLUSTER_OPERATION_DONE {
			tc := getDetailsResponse.ClusterOp.SerialTasks
			lastTc := tc[len(tc)-1]
			if lastTc.ParallelTasks[len(lastTc.ParallelTasks)-1].Output != expectedOutput {
				t.Fatalf("ClusterOperation finished but did not return expected output. Full ClusterOperation details: %v", getDetailsResponse.ClusterOp)
			}
			break
		} else {
			t.Logf("Waiting for clusterOp: %v", getDetailsResponse.ClusterOp)
			time.Sleep(5 * time.Millisecond)
		}
	}

	scheduler.ShutdownAndWait()
}

func TestEnqueueFailsDueToMissingParameter(t *testing.T) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	scheduler.Run()

	enqueueRequest := &pb.EnqueueClusterOperationRequest{
		Name: "TestingEchoTask",
		Parameters: map[string]string{
			"unrelevant-parameter": "value",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.TODO(), enqueueRequest)
	if err == nil {
		t.Fatalf("Scheduler should have failed to start cluster operation because not all required parameters were provided. Request: %v Error: %v Response: %v", enqueueRequest, err, enqueueResponse)
	}

	scheduler.ShutdownAndWait()
}
