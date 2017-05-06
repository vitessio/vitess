/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resharding

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func TestParallelRunner(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")

	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, false /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApproval(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")

	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)
	defer close(notifications)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	if err := checkUIChangeFromNoneStarted(m, uuid, notifications); err != nil {
		t.Fatal(err)
	}

	// Wait for the workflow to end.
	m.Wait(context.Background(), uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalFromFirstDone(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}
	tw, err := testworkflow(m, uuid)
	if err != nil {
		t.Fatal(err)
	}

	// Change the checkpoint of the workflow to let task1 succeed
	// before starting the workflow.
	wi, err := m.WorkflowInfoForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow info from manager: %v", err)
	}
	checkpointWriter := NewCheckpointWriter(ts, tw.checkpoint, wi)
	task1ID := createTestTaskID(phaseSimple, 0)
	checkpointWriter.UpdateTask(task1ID, workflowpb.TaskState_TaskDone, nil)

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)
	defer close(notifications)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}
	if err := checkUIChangeFirstApproved(m, uuid, notifications); err != nil {
		t.Fatal(err)
	}
	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalFromFirstRunning(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}
	tw, err := testworkflow(m, uuid)
	if err != nil {
		t.Fatal(err)
	}

	// Change the checkpoint of the workflow to let task1 succeeded
	// before starting the workflow.
	wi, err := m.WorkflowInfoForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow info from manager: %v", err)
	}
	checkpointWriter := NewCheckpointWriter(ts, tw.checkpoint, wi)
	task1ID := createTestTaskID(phaseSimple, 0)
	checkpointWriter.UpdateTask(task1ID, workflowpb.TaskState_TaskRunning, nil)

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)
	defer close(notifications)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	if err := checkUIChangeFirstApproved(m, uuid, notifications); err != nil {
		t.Fatal(err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalFromFirstDoneSecondRunning(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}
	tw, err := testworkflow(m, uuid)
	if err != nil {
		t.Fatal(err)
	}

	// Change the checkpoint of the workflow to let task1 succeeded
	// before starting the workflow.
	wi, err := m.WorkflowInfoForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow info from manager: %v", err)
	}
	checkpointWriter := NewCheckpointWriter(ts, tw.checkpoint, wi)
	task1ID := createTestTaskID(phaseSimple, 0)
	checkpointWriter.UpdateTask(task1ID, workflowpb.TaskState_TaskDone, nil)
	task2ID := createTestTaskID(phaseSimple, 1)
	checkpointWriter.UpdateTask(task2ID, workflowpb.TaskState_TaskRunning, nil)

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)
	defer close(notifications)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	if err := checkUIChangeAllApproved(notifications); err != nil {
		t.Fatal(err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalFirstRunningSecondRunning(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}
	tw, err := testworkflow(m, uuid)
	if err != nil {
		t.Fatal(err)
	}

	// Change the checkpoint in TestWorkflow to let task1 succeeded
	// before starting the workflow.
	wi, err := m.WorkflowInfoForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow info from manager: %v", err)
	}
	checkpointWriter := NewCheckpointWriter(ts, tw.checkpoint, wi)
	task1ID := createTestTaskID(phaseSimple, 0)
	checkpointWriter.UpdateTask(task1ID, workflowpb.TaskState_TaskRunning, nil)
	task2ID := createTestTaskID(phaseSimple, 1)
	checkpointWriter.UpdateTask(task2ID, workflowpb.TaskState_TaskRunning, nil)

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)
	defer close(notifications)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	if err := checkUIChangeAllApproved(notifications); err != nil {
		t.Fatal(err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}

	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalFromAllDone(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */)
	if err != nil {
		t.Fatal(err)
	}
	tw, err := testworkflow(m, uuid)
	if err != nil {
		t.Fatal(err)
	}

	// Change the checkpoint in TestWorkflow to let all tasks succeeded
	// before starting the workflow.
	wi, err := m.WorkflowInfoForTesting(uuid)
	if err != nil {
		t.Fatalf("fail to get workflow info from manager: %v", err)
	}
	checkpointWriter := NewCheckpointWriter(ts, tw.checkpoint, wi)
	task1ID := createTestTaskID(phaseSimple, 0)
	checkpointWriter.UpdateTask(task1ID, workflowpb.TaskState_TaskDone, nil)
	task2ID := createTestTaskID(phaseSimple, 1)
	checkpointWriter.UpdateTask(task2ID, workflowpb.TaskState_TaskDone, nil)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)
	close(notifications)
	// Check notification about phase node if exists, make sure no actions added.

	if message, err := checkNoActions(notifications, string(phaseSimple)); err != nil {
		t.Fatalf("there should be no actions for node %v: %v, %v", phaseSimple, message, err)
	}

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerRetry(t *testing.T) {
	// Tasks in the workflow are forced to fail at the first attempt. Then we
	// retry task1, after it is finished successfully, we retry task2.
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, false /* enableApprovals*/, true /* retry */)
	if err != nil {
		t.Fatal(err)
	}

	notifications, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)
	defer close(notifications)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	task1ID := createTestTaskID(phaseSimple, 0)
	task2ID := createTestTaskID(phaseSimple, 1)
	task1Node := &workflow.Node{
		PathName: "0",
		Actions: []*workflow.Action{
			{
				Name:  actionNameRetry,
				State: workflow.ActionStateEnabled,
				Style: workflow.ActionStyleWaiting,
			},
		},
	}
	task2Node := &workflow.Node{
		PathName: "1",
		Actions: []*workflow.Action{
			{
				Name:  actionNameRetry,
				State: workflow.ActionStateEnabled,
				Style: workflow.ActionStyleWaiting,
			},
		},
	}

	// Wait for retry actions enabled after tasks failed at the first attempt.
	if err := consumeNotificationsUntil(notifications, task1Node, task2Node); err != nil {
		t.Fatalf("Should get expected update of nodes: %v", task1Node)
	}
	if err := verifyTask(context.Background(), ts, uuid, task1ID, workflowpb.TaskState_TaskDone, errMessage); err != nil {
		t.Errorf("verify task %v failed: %v", task1ID, err)
	}
	if err := verifyTask(context.Background(), ts, uuid, task2ID, workflowpb.TaskState_TaskDone, errMessage); err != nil {
		t.Errorf("verify task %v failed: %v", task2ID, err)
	}

	// Retry task1. Task1 is launched after removing actions.
	if err := triggerAction(ctx, m, path.Join("/", uuid, task1ID), actionNameRetry); err != nil {
		t.Fatal(err)
	}
	// Check the retry action is removed.
	task1Node.Actions = []*workflow.Action{}
	if err := consumeNotificationsUntil(notifications, task1Node); err != nil {
		t.Fatalf("Should get expected update of nodes: %v", task1Node)
	}
	// Verify task1 has succeed.
	if err := waitForFinished(notifications, "0", taskFinishedMessage); err != nil {
		t.Fatal(err)
	}
	if err := verifyTask(context.Background(), ts, uuid, task1ID, workflowpb.TaskState_TaskDone, ""); err != nil {
		t.Errorf("verify task %v failed: %v", task1ID, err)
	}

	// Retry task2
	if err := triggerAction(ctx, m, path.Join("/", uuid, task2ID), actionNameRetry); err != nil {
		t.Fatal(err)
	}
	// Check the retry action is removed. Task2 is launched after removing actions.
	task2Node.Actions = []*workflow.Action{}
	if err := consumeNotificationsUntil(notifications, task2Node); err != nil {
		t.Fatalf("Should get expected update of nodes: %v", task2Node)
	}
	// Verify task2 has succeed.
	if err := waitForFinished(notifications, "1", taskFinishedMessage); err != nil {
		t.Fatal(err)
	}
	if err := verifyTask(context.Background(), ts, uuid, task2ID, workflowpb.TaskState_TaskDone, ""); err != nil {
		t.Errorf("verify task %v failed: %v", task2ID, err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := verifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func setupTestWorkflow(ctx context.Context, ts topo.Server, enableApprovals, retry bool) (*workflow.Manager, string, *sync.WaitGroup, context.CancelFunc, error) {
	m := workflow.NewManager(ts)
	// Run the manager in the background.
	wg, _, cancel := startManager(m)

	// Create a testworkflow.
	enableApprovalsFlag := fmt.Sprintf("-enable_approvals=%v", enableApprovals)
	retryFlag := fmt.Sprintf("-retry=%v", retry)
	uuid, err := m.Create(ctx, testWorkflowFactoryName, []string{retryFlag, "-count=2", enableApprovalsFlag})
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("cannot create testworkflow: %v", err)
	}

	return m, uuid, wg, cancel, nil
}

func testworkflow(m *workflow.Manager, uuid string) (*TestWorkflow, error) {
	w, err := m.WorkflowForTesting(uuid)
	if err != nil {
		return nil, fmt.Errorf("fail to get workflow from manager: %v", err)
	}
	tw := w.(*TestWorkflow)
	return tw, nil
}

func startManager(m *workflow.Manager) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	// Run the manager in the background.
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.Run(ctx)
	}()

	m.WaitUntilRunning()
	return wg, ctx, cancel
}

func triggerAction(ctx context.Context, m *workflow.Manager, nodePath, actionName string) error {
	return m.NodeManager().Action(ctx, &workflow.ActionParameters{
		Path: nodePath,
		Name: actionName,
	})
}

func setupNotifications(m *workflow.Manager) (chan []byte, int, error) {
	// Set up notifications channel to monitor UI updates.
	notifications := make(chan []byte, 10)
	_, index, err := m.NodeManager().GetAndWatchFullTree(notifications)
	if err != nil {
		return nil, -1, fmt.Errorf("GetAndWatchTree Failed: %v", err)
	}
	return notifications, index, nil
}

func checkUIChangeFromNoneStarted(m *workflow.Manager, uuid string, notifications chan []byte) error {
	wantNode := &workflow.Node{
		PathName: string(phaseSimple),
		Actions: []*workflow.Action{
			{
				Name:  actionNameApproveFirstTask,
				State: workflow.ActionStateDisabled,
				Style: workflow.ActionStyleTriggered,
			},
			{
				Name:  actionNameApproveRemainingTasks,
				State: workflow.ActionStateDisabled,
				Style: workflow.ActionStyleTriggered,
			},
		},
	}

	// Approval buttons are initially disabled.
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}

	// First task is ready and approval button is enabled.
	wantNode.Actions[0].State = workflow.ActionStateEnabled
	wantNode.Actions[0].Style = workflow.ActionStyleWaiting
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}
	// Trigger the approval button.
	// It becomes disabled and shows approved message.
	if err := triggerAction(context.Background(), m, path.Join("/", uuid, string(phaseSimple)), actionNameApproveFirstTask); err != nil {
		return err
	}
	// Trigger the approval button again to test the code is robust against
	// duplicate requests.
	if err := triggerAction(context.Background(), m, path.Join("/", uuid, string(phaseSimple)), actionNameApproveFirstTask); err == nil {
		return fmt.Errorf("triggering the approval action %v again should fail", actionNameApproveFirstTask)
	}
	return checkUIChangeFirstApproved(m, uuid, notifications)
}

// checkUIChangeFirstApproved observes the UI change for 2 scenarios:
// 1. the first and second tasks are all running.
// 2. the first and second tasks have succeeded, but remaining tasks haven't
// succeeded yet.
func checkUIChangeAllApproved(notifications chan []byte) error {
	wantNode := &workflow.Node{
		PathName: string(phaseSimple),
		Actions: []*workflow.Action{
			{
				Name:  actionNameApproveFirstTaskDone,
				State: workflow.ActionStateDisabled,
				Style: workflow.ActionStyleTriggered,
			},
			{
				Name:  actionNameApproveRemainingTasksDone,
				State: workflow.ActionStateDisabled,
				Style: workflow.ActionStyleTriggered,
			},
		},
	}

	// Approval buttons are disabled and show approved messages.
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}

	// Approval buttons are cleared after the phase is finished.
	wantNode.Actions = []*workflow.Action{}
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}
	return nil
}

// checkUIChangeFirstApproved observes the UI change for 2 scenarios:
// 1. the first task is running and the second hasn't started.
// 2. the first task is done and the second hasn't started.
func checkUIChangeFirstApproved(m *workflow.Manager, uuid string, notifications chan []byte) error {
	wantNode := &workflow.Node{
		PathName: string(phaseSimple),
		Actions: []*workflow.Action{
			{
				Name:  actionNameApproveFirstTaskDone,
				State: workflow.ActionStateDisabled,
				Style: workflow.ActionStyleTriggered,
			},
			{
				Name:  actionNameApproveRemainingTasks,
				State: workflow.ActionStateDisabled,
				Style: workflow.ActionStyleTriggered,
			},
		},
	}

	// Approval buttons are initially disabled.
	// The approval button for the first task shows the approved message.
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}

	// The second task is ready and approval button for remaining tasks is
	// enabled.
	wantNode.Actions[1].State = workflow.ActionStateEnabled
	wantNode.Actions[1].Style = workflow.ActionStyleWaiting
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}
	// Trigger this approval button. It becomes disabled and shows the
	// approved message.
	if err := triggerAction(context.Background(), m, path.Join("/", uuid, string(phaseSimple)), actionNameApproveRemainingTasks); err != nil {
		return err
	}
	return checkUIChangeAllApproved(notifications)
}

// consumeNotificationsUntil waits for all wantNodes to be seen from the
// notifications of UI change.
func consumeNotificationsUntil(notifications chan []byte, wantNodes ...*workflow.Node) error {
	wantSet := make(map[*workflow.Node]bool)
	for _, n := range wantNodes {
		wantSet[n] = true
	}

	for monitor := range notifications {
		update := &workflow.Update{}
		if err := json.Unmarshal(monitor, update); err != nil {
			return err
		}

		if update.Nodes == nil || len(update.Nodes) != 1 {
			// Ignore unrelated UI updates. For example, the UI update often includes
			// multiple nodes when the workflow initialize its UI.
			continue
		}
		for _, n := range wantNodes {
			if checkNode(update.Nodes[0], n) {
				if wantSet[n] {
					delete(wantSet, n)
				}
			}
			if len(wantSet) == 0 {
				return nil
			}
		}
	}
	return fmt.Errorf("notifications channel is closed unexpectedly when waiting for expected nodes")
}

func checkNode(gotNode *workflow.Node, wantNode *workflow.Node) bool {
	if gotNode.PathName != wantNode.PathName || len(gotNode.Actions) != len(wantNode.Actions) {
		return false
	}

	for i := 0; i < len(wantNode.Actions); i++ {
		if !reflect.DeepEqual(gotNode.Actions[i], wantNode.Actions[i]) {
			return false
		}
	}
	return true
}

func checkNoActions(notifications chan []byte, nodePath string) (string, error) {
	for monitor := range notifications {
		update := &workflow.Update{}
		if err := json.Unmarshal(monitor, update); err != nil {
			return "", err
		}

		if update.Nodes == nil {
			continue
		}

		for _, n := range update.Nodes {
			if n.PathName == nodePath && len(n.Actions) > 0 {
				return string(monitor), fmt.Errorf("actions detected unexpectedly")
			}
		}
	}
	return "", nil
}

func waitForFinished(notifications chan []byte, path, message string) error {
	for monitor := range notifications {
		update := &workflow.Update{}
		if err := json.Unmarshal(monitor, update); err != nil {
			return err
		}

		if update.Nodes == nil || len(update.Nodes) != 1 {
			// Ignore unrelated UI updates. For example, the UI update often includes
			// multiple nodes when the workflow initialize its UI.
			continue
		}

		if update.Nodes[0].PathName == path && update.Nodes[0].Message == taskFinishedMessage {
			return nil
		}
	}
	return fmt.Errorf("notifications channel is closed unexpectedly when waiting for expected nodes")
}

func verifyAllTasksDone(ctx context.Context, ts topo.Server, uuid string) error {
	checkpoint, err := checkpoint(ctx, ts, uuid)
	if err != nil {
		return err
	}

	for _, task := range checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			return fmt.Errorf("task: %v should succeed: task status: %v, %v", task.Id, task.State, task.Attributes)
		}
	}
	return nil
}

func verifyTask(ctx context.Context, ts topo.Server, uuid, taskID string, taskState workflowpb.TaskState, taskError string) error {
	checkpoint, err := checkpoint(ctx, ts, uuid)
	if err != nil {
		return err
	}
	task := checkpoint.Tasks[taskID]

	if task.State != taskState || task.Error != taskError {
		return fmt.Errorf("task status: %v, %v fails to match expected status: %v, %v", task.State, task.Error, taskState, taskError)
	}
	return nil
}

func checkpoint(ctx context.Context, ts topo.Server, uuid string) (*workflowpb.WorkflowCheckpoint, error) {
	wi, err := ts.GetWorkflow(ctx, uuid)
	if err != nil {
		return nil, fmt.Errorf("fail to get workflow for: %v", uuid)
	}
	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(wi.Workflow.Data, checkpoint); err != nil {
		return nil, fmt.Errorf("fails to get checkpoint for the workflow: %v", err)
	}
	return checkpoint, nil
}
