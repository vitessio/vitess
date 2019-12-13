/*
Copyright 2019 The Vitess Authors.

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

package workflow

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

func TestParallelRunner(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")

	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, false /* enableApprovals*/, false /* retry */, false /* sequential */)
	if err != nil {
		t.Fatal(err)
	}

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	// Wait for the workflow to end.
	m.Wait(ctx, uuid)

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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

	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalOnStoppedWorkflow(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")

	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
	if err != nil {
		t.Fatal(err)
	}

	_, index, err := setupNotifications(m)
	if err != nil {
		t.Fatal(err)
	}
	defer m.NodeManager().CloseWatcher(index)

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	// Cancelling the ctx stops the workflow
	cancel()
	// Wait for the workflow to end.
	m.Wait(context.Background(), uuid)

	if err := verifyAllTasksState(ctx, ts, uuid, workflowpb.TaskState_TaskNotStarted); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestParallelRunnerStoppingWorkflowWithSequentialSteps(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")

	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, true /* sequential */)
	if err != nil {
		t.Fatal(err)
	}

	// Start the job
	if err := m.Start(ctx, uuid); err != nil {
		t.Fatalf("cannot start testworkflow: %v", err)
	}

	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestParallelRunnerApprovalFromFirstDone(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell")
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, true /* enableApprovals*/, false /* retry */, false /* sequential */)
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
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
	m, uuid, wg, cancel, err := setupTestWorkflow(ctx, ts, false /* enableApprovals*/, true /* retry */, false /* sequential */)
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
	task1Node := &Node{
		PathName: "0",
		Actions: []*Action{
			{
				Name:  actionNameRetry,
				State: ActionStateEnabled,
				Style: ActionStyleWaiting,
			},
		},
	}
	task2Node := &Node{
		PathName: "1",
		Actions: []*Action{
			{
				Name:  actionNameRetry,
				State: ActionStateEnabled,
				Style: ActionStyleWaiting,
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
	task1Node.Actions = []*Action{}
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
	task2Node.Actions = []*Action{}
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

	if err := VerifyAllTasksDone(ctx, ts, uuid); err != nil {
		t.Fatal(err)
	}
	// Stop the manager.
	if err := m.Stop(ctx, uuid); err != nil {
		t.Fatalf("cannot stop testworkflow: %v", err)
	}
	cancel()
	wg.Wait()
}

func setupTestWorkflow(ctx context.Context, ts *topo.Server, enableApprovals, retry, sequential bool) (*Manager, string, *sync.WaitGroup, context.CancelFunc, error) {
	m := NewManager(ts)
	// Run the manager in the background.
	wg, _, cancel := StartManager(m)

	// Create a testworkflow.
	enableApprovalsFlag := fmt.Sprintf("-enable_approvals=%v", enableApprovals)
	retryFlag := fmt.Sprintf("-retry=%v", retry)
	sequentialFlag := fmt.Sprintf("-sequential=%v", sequential)
	uuid, err := m.Create(ctx, testWorkflowFactoryName, []string{retryFlag, "-count=2", enableApprovalsFlag, sequentialFlag})
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("cannot create testworkflow: %v", err)
	}

	return m, uuid, wg, cancel, nil
}

func testworkflow(m *Manager, uuid string) (*TestWorkflow, error) {
	w, err := m.WorkflowForTesting(uuid)
	if err != nil {
		return nil, fmt.Errorf("fail to get workflow from manager: %v", err)
	}
	tw := w.(*TestWorkflow)
	return tw, nil
}

func triggerAction(ctx context.Context, m *Manager, nodePath, actionName string) error {
	return m.NodeManager().Action(ctx, &ActionParameters{
		Path: nodePath,
		Name: actionName,
	})
}

func setupNotifications(m *Manager) (chan []byte, int, error) {
	// Set up notifications channel to monitor UI updates.
	notifications := make(chan []byte, 10)
	_, index, err := m.NodeManager().GetAndWatchFullTree(notifications)
	if err != nil {
		return nil, -1, fmt.Errorf("GetAndWatchTree Failed: %v", err)
	}
	return notifications, index, nil
}

func checkUIChangeFromNoneStarted(m *Manager, uuid string, notifications chan []byte) error {
	wantNode := &Node{
		PathName: string(phaseSimple),
		Actions: []*Action{
			{
				Name:  actionNameApproveFirstTask,
				State: ActionStateDisabled,
				Style: ActionStyleTriggered,
			},
			{
				Name:  actionNameApproveRemainingTasks,
				State: ActionStateDisabled,
				Style: ActionStyleTriggered,
			},
		},
	}

	// Approval buttons are initially disabled.
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}

	// First task is ready and approval button is enabled.
	wantNode.Actions[0].State = ActionStateEnabled
	wantNode.Actions[0].Style = ActionStyleWaiting
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
	wantNode := &Node{
		PathName: string(phaseSimple),
		Actions: []*Action{
			{
				Name:  actionNameApproveFirstTaskDone,
				State: ActionStateDisabled,
				Style: ActionStyleTriggered,
			},
			{
				Name:  actionNameApproveRemainingTasksDone,
				State: ActionStateDisabled,
				Style: ActionStyleTriggered,
			},
		},
	}

	// Approval buttons are disabled and show approved messages.
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}

	// Approval buttons are cleared after the phase is finished.
	wantNode.Actions = []*Action{}
	if err := consumeNotificationsUntil(notifications, wantNode); err != nil {
		return fmt.Errorf("should get expected update of node: %v", wantNode)
	}
	return nil
}

// checkUIChangeFirstApproved observes the UI change for 2 scenarios:
// 1. the first task is running and the second hasn't started.
// 2. the first task is done and the second hasn't started.
func checkUIChangeFirstApproved(m *Manager, uuid string, notifications chan []byte) error {
	wantNode := &Node{
		PathName: string(phaseSimple),
		Actions: []*Action{
			{
				Name:  actionNameApproveFirstTaskDone,
				State: ActionStateDisabled,
				Style: ActionStyleTriggered,
			},
			{
				Name:  actionNameApproveRemainingTasks,
				State: ActionStateDisabled,
				Style: ActionStyleTriggered,
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
	wantNode.Actions[1].State = ActionStateEnabled
	wantNode.Actions[1].Style = ActionStyleWaiting
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
func consumeNotificationsUntil(notifications chan []byte, wantNodes ...*Node) error {
	wantSet := make(map[*Node]bool)
	for _, n := range wantNodes {
		wantSet[n] = true
	}

	for monitor := range notifications {
		update := &Update{}
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

func checkNode(gotNode *Node, wantNode *Node) bool {
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
		update := &Update{}
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
		update := &Update{}
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
