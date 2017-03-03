package resharding

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func TestParallelRunner(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	m := workflow.NewManager(ts)
	ctx := context.Background()

	// Run the manager in the background.
	wg, _, cancel := startManager(m)

	// Create a testworkflow.
	uuid, err := m.Create(ctx, testWorkflowFactoryName, []string{"-retry=false", "-count=2"})
	if err != nil {
		t.Fatalf("cannot create testworkflow: %v", err)
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

func TestParallelRunnerRetryAction(t *testing.T) {
	// Tasks in the workflow are forced to fail at the first attempt. Then we
	// retry task1, after it is finished successfully, we retry task2.
	ts := memorytopo.NewServer("cell")
	m := workflow.NewManager(ts)
	ctx := context.Background()
	// Run the manager in the background.
	wg, _, cancel := startManager(m)

	// Create a testworkflow.
	uuid, err := m.Create(ctx, testWorkflowFactoryName, []string{"-retry=true", "-count=2"})
	if err != nil {
		t.Fatalf("cannot create testworkflow: %v", err)
	}

	// We use notifications channel to monitor the update of UI.
	notifications := make(chan []byte, 10)
	_, index, err := m.NodeManager().GetAndWatchFullTree(notifications)
	if err != nil {
		t.Errorf("GetAndWatchTree Failed: %v", err)
	}
	defer m.NodeManager().CloseWatcher(index)
	go func() {
		// This goroutine is used to detect and trigger the retry actions.
		task1ID := createTestTaskID(phaseSimple, 0)
		task2ID := createTestTaskID(phaseSimple, 1)

		retry1 := false
		retry2 := false
		for {
			select {
			case monitor, ok := <-notifications:
				monitorStr := string(monitor)
				if !ok {
					t.Errorf("notifications channel is closed unexpectedly: %v, %v", ok, monitorStr)
				}
				if strings.Contains(monitorStr, "Retry") {
					if strings.Contains(monitorStr, task1ID) {
						verifyTaskSuccessOrFailure(context.Background(), ts, uuid, task1ID, false /* isSuccess*/)
						retry1 = true
					}
					if strings.Contains(monitorStr, task2ID) {
						verifyTaskSuccessOrFailure(context.Background(), ts, uuid, task2ID, false /* isSuccess*/)
						retry2 = true
					}
				}
				// After detecting both tasks have enabled retry actions after failure,
				// retry task1, check its success, then retry task2, check its success.
				if retry1 && retry2 {
					clickRetry(ctx, t, m, path.Join("/"+uuid, task1ID))
					waitForFinished(ctx, t, notifications, task1ID)
					if err := verifyTaskSuccessOrFailure(context.Background(), ts, uuid, task1ID, true /* isSuccess*/); err != nil {
						t.Errorf("verify task %v success failed: %v", task1ID, err)
					}

					clickRetry(ctx, t, m, path.Join("/"+uuid, task2ID))
					waitForFinished(ctx, t, notifications, task2ID)
					if err := verifyTaskSuccessOrFailure(context.Background(), ts, uuid, task2ID, true /* isSuccess*/); err != nil {
						t.Errorf("verify task %v success failed: %v", task2ID, err)
					}
					return
				}
			case <-ctx.Done():
				t.Errorf("context is canceled")
				return
			}
		}
	}()

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

func clickRetry(ctx context.Context, t *testing.T, m *workflow.Manager, nodePath string) {
	t.Logf("Click retry action on node: %v.", nodePath)
	if err := m.NodeManager().Action(ctx, &workflow.ActionParameters{
		Path: nodePath,
		Name: "Retry",
	}); err != nil {
		t.Errorf("unexpected action error: %v", err)
	}
}

func waitForFinished(ctx context.Context, t *testing.T, notifications chan []byte, taskID string) {
	for {
		select {
		case monitor, ok := <-notifications:
			monitorStr := string(monitor)
			if !ok {
				t.Errorf("unexpected notification: %v, %v", ok, monitorStr)
			}

			finishMessage := fmt.Sprintf(`"message":"task %v finished"`, taskID)
			if strings.Contains(monitorStr, finishMessage) {
				if strings.Contains(monitorStr, `"actions":[{"name:`) {
					t.Fatalf("the node actions should be empty after triggering retry: %v", monitorStr)
				}
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func verifyAllTasksDone(ctx context.Context, ts topo.Server, uuid string) error {
	wi, err := ts.GetWorkflow(ctx, uuid)
	if err != nil {
		return fmt.Errorf("fail to get workflow for: %v", uuid)
	}
	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(wi.Workflow.Data, checkpoint); err != nil {
		return fmt.Errorf("fails to get checkpoint for the workflow: %v", err)
	}

	for _, task := range checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			return fmt.Errorf("task: %v should succeed: task status: %v, %v", task.Id, task.State, task.Attributes)
		}
	}
	return nil
}

func verifyTaskSuccessOrFailure(ctx context.Context, ts topo.Server, uuid, taskID string, isSuccess bool) error {
	wi, err := ts.GetWorkflow(ctx, uuid)
	if err != nil {
		return fmt.Errorf("fail to get workflow for: %v", uuid)
	}

	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(wi.Workflow.Data, checkpoint); err != nil {
		return fmt.Errorf("fails to get checkpoint for the workflow: %v", err)
	}
	task := checkpoint.Tasks[taskID]

	taskError := ""
	if !isSuccess {
		taskError = errMessage
	}
	if task.State != workflowpb.TaskState_TaskDone || task.Error != taskError {
		return fmt.Errorf("task: %v should succeed. Task status: %v, %v", task.Id, task.State, task.Error)
	}
	return nil
}
