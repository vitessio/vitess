/*
Copyright 2017 Google Inc.

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
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	context "golang.org/x/net/context"

	automationpb "vitess.io/vitess/go/vt/proto/automation"
)

// newTestScheduler constructs a scheduler with test tasks.
// If tasks should be available as cluster operation, they still have to be registered manually with scheduler.registerClusterOperation.
func newTestScheduler(t *testing.T) *Scheduler {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.setTaskCreator(testingTaskCreator)
	return scheduler
}

func enqueueClusterOperationAndCheckOutput(t *testing.T, taskName string, expectedOutput string, expectedError string) *automationpb.ClusterOperation {
	scheduler := newTestScheduler(t)
	defer scheduler.ShutdownAndWait()
	scheduler.registerClusterOperation("TestingEchoTask")
	scheduler.registerClusterOperation("TestingFailTask")
	scheduler.registerClusterOperation("TestingEmitEchoTask")
	scheduler.registerClusterOperation("TestingEmitEchoFailEchoTask")

	scheduler.Run()

	enqueueRequest := &automationpb.EnqueueClusterOperationRequest{
		Name: taskName,
		Parameters: map[string]string{
			"echo_text": expectedOutput,
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.Background(), enqueueRequest)
	if err != nil {
		t.Fatalf("Failed to start cluster operation. Request: %v Error: %v", enqueueRequest, err)
	}

	return waitForClusterOperation(t, scheduler, enqueueResponse.Id, expectedOutput, expectedError)
}

// waitForClusterOperation is a helper function which blocks until the Cluster Operation has finished.
func waitForClusterOperation(t *testing.T, scheduler *Scheduler, id string, expectedOutputLastTask string, expectedErrorLastTask string) *automationpb.ClusterOperation {
	if expectedOutputLastTask == "" && expectedErrorLastTask == "" {
		t.Fatal("Error in test: Cannot wait for an operation where both output and error are expected to be empty.")
	}

	getDetailsRequest := &automationpb.GetClusterOperationDetailsRequest{
		Id: id,
	}

	for {
		getDetailsResponse, err := scheduler.GetClusterOperationDetails(context.Background(), getDetailsRequest)
		if err != nil {
			t.Fatalf("Failed to get details for cluster operation. Request: %v Error: %v", getDetailsRequest, err)
		}
		if getDetailsResponse.ClusterOp.State == automationpb.ClusterOperationState_CLUSTER_OPERATION_DONE {
			tc := getDetailsResponse.ClusterOp.SerialTasks
			// Check the last task which have finished. (It may not be the last one because tasks can fail.)
			var lastTc *automationpb.TaskContainer
			for i := len(tc) - 1; i >= 0; i-- {
				if tc[i].ParallelTasks[len(tc[i].ParallelTasks)-1].State == automationpb.TaskState_DONE {
					lastTc = tc[i]
					break
				}
			}
			if expectedOutputLastTask != "" {
				if got := lastTc.ParallelTasks[len(lastTc.ParallelTasks)-1].Output; !strings.Contains(got, expectedOutputLastTask) {
					t.Fatalf("ClusterOperation finished but did not contain expected output. got: %v want: %v Full ClusterOperation details: %v", got, expectedOutputLastTask, proto.MarshalTextString(getDetailsResponse.ClusterOp))
				}
			}
			if expectedErrorLastTask != "" {
				if lastError := lastTc.ParallelTasks[len(lastTc.ParallelTasks)-1].Error; !strings.Contains(lastError, expectedErrorLastTask) {
					t.Fatalf("ClusterOperation finished last error does not contain expected error. got: '%v' want: '%v' Full ClusterOperation details: %v", lastError, expectedErrorLastTask, getDetailsResponse.ClusterOp)
				}
			}
			return getDetailsResponse.ClusterOp
		}

		t.Logf("Waiting for clusterOp: %v", getDetailsResponse.ClusterOp)
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSchedulerImmediateShutdown(t *testing.T) {
	// Make sure that the scheduler shuts down cleanly when it was instantiated, but not started with Run().
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.ShutdownAndWait()
}

func TestEnqueueSingleTask(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingEchoTask", "echoed text", "")
}

func TestEnqueueEmittingTask(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingEmitEchoTask", "echoed text from emitted task", "")
}

func TestFailedTaskFailsClusterOperation(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingFailTask", "something went wrong", "full error message")
}

func TestFailedTaskFailsWholeClusterOperationEarly(t *testing.T) {
	// If a task fails in the middle of a cluster operation, the remaining tasks must not be executed.
	details := enqueueClusterOperationAndCheckOutput(t, "TestingEmitEchoFailEchoTask", "", "full error message")
	got := details.SerialTasks[2].ParallelTasks[0].Error
	want := "full error message"
	if got != want {
		t.Errorf("TestFailedTaskFailsWholeClusterOperationEarly: got error: '%v' want error: '%v'", got, want)
	}
	if details.SerialTasks[3].ParallelTasks[0].State != automationpb.TaskState_NOT_STARTED {
		t.Errorf("TestFailedTaskFailsWholeClusterOperationEarly: Task after a failing task must not have been started.")
	}
}

func TestEnqueueFailsDueToMissingParameter(t *testing.T) {
	scheduler := newTestScheduler(t)
	defer scheduler.ShutdownAndWait()
	scheduler.registerClusterOperation("TestingEchoTask")

	scheduler.Run()

	enqueueRequest := &automationpb.EnqueueClusterOperationRequest{
		Name: "TestingEchoTask",
		Parameters: map[string]string{
			"unrelevant-parameter": "value",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.Background(), enqueueRequest)

	if err == nil {
		t.Fatalf("Scheduler should have failed to start cluster operation because not all required parameters were provided. Request: %v Error: %v Response: %v", enqueueRequest, err, enqueueResponse)
	}
	want := "required parameters are missing: [echo_text]"
	if err.Error() != want {
		t.Fatalf("Wrong error message. got: '%v' want: '%v'", err, want)
	}
}

func TestEnqueueFailsDueToUnregisteredClusterOperation(t *testing.T) {
	scheduler := newTestScheduler(t)
	defer scheduler.ShutdownAndWait()

	scheduler.Run()

	enqueueRequest := &automationpb.EnqueueClusterOperationRequest{
		Name: "TestingEchoTask",
		Parameters: map[string]string{
			"unrelevant-parameter": "value",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.Background(), enqueueRequest)

	if err == nil {
		t.Fatalf("Scheduler should have failed to start cluster operation because it should not have been registered. Request: %v Error: %v Response: %v", enqueueRequest, err, enqueueResponse)
	}
	want := "no ClusterOperation with name: TestingEchoTask is registered"
	if err.Error() != want {
		t.Fatalf("Wrong error message. got: '%v' want: '%v'", err, want)
	}
}

func TestGetDetailsFailsUnknownId(t *testing.T) {
	scheduler := newTestScheduler(t)
	defer scheduler.ShutdownAndWait()

	scheduler.Run()

	getDetailsRequest := &automationpb.GetClusterOperationDetailsRequest{
		Id: "-1", // There will never be a ClusterOperation with this id.
	}

	getDetailsResponse, err := scheduler.GetClusterOperationDetails(context.Background(), getDetailsRequest)
	if err == nil {
		t.Fatalf("Did not fail to get details for invalid ClusterOperation id. Request: %v Response: %v Error: %v", getDetailsRequest, getDetailsResponse, err)
	}
	want := "ClusterOperation with id: -1 not found"
	if err.Error() != want {
		t.Fatalf("Wrong error message. got: '%v' want: '%v'", err, want)
	}
}

func TestEnqueueFailsBecauseTaskInstanceCannotBeCreated(t *testing.T) {
	scheduler := newTestScheduler(t)
	defer scheduler.ShutdownAndWait()
	scheduler.setTaskCreator(defaultTaskCreator)
	// TestingEchoTask is registered as cluster operation, but its task cannot be instantied because "testingTaskCreator" was not set.
	scheduler.registerClusterOperation("TestingEchoTask")

	scheduler.Run()

	enqueueRequest := &automationpb.EnqueueClusterOperationRequest{
		Name: "TestingEchoTask",
		Parameters: map[string]string{
			"unrelevant-parameter": "value",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.Background(), enqueueRequest)

	if err == nil {
		t.Fatalf("Scheduler should have failed to start cluster operation because the task could not be instantiated. Request: %v Error: %v Response: %v", enqueueRequest, err, enqueueResponse)
	}
	want := "no implementation found for: TestingEchoTask"
	if err.Error() != want {
		t.Fatalf("Wrong error message. got: '%v' want: '%v'", err, want)
	}
}

func TestTaskEmitsTaskWhichCannotBeInstantiated(t *testing.T) {
	scheduler := newTestScheduler(t)
	defer scheduler.ShutdownAndWait()
	scheduler.setTaskCreator(func(taskName string) Task {
		// TaskCreator which doesn't know TestingEchoTask (but emitted by TestingEmitEchoTask).
		switch taskName {
		case "TestingEmitEchoTask":
			return &TestingEmitEchoTask{}
		default:
			return nil
		}
	})
	scheduler.registerClusterOperation("TestingEmitEchoTask")

	scheduler.Run()

	enqueueRequest := &automationpb.EnqueueClusterOperationRequest{
		Name: "TestingEmitEchoTask",
		Parameters: map[string]string{
			"echo_text": "to be emitted task should fail to instantiate",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.Background(), enqueueRequest)
	if err != nil {
		t.Fatalf("Failed to start cluster operation. Request: %v Error: %v", enqueueRequest, err)
	}

	details := waitForClusterOperation(t, scheduler, enqueueResponse.Id, "emitted TestingEchoTask", "no implementation found for: TestingEchoTask")
	if len(details.SerialTasks) != 1 {
		t.Errorf("A task has been emitted, but it shouldn't. Details:\n%v", proto.MarshalTextString(details))
	}
}
