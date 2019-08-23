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

package workflow

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo/memorytopo"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

// TestWaitUntilRunning verifies that WaitUntilRunning() works as expected
// (blocking until Run() has advanced far enough), even across multiple Manager
// starts and stops.
func TestWaitUntilRunning(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	m := NewManager(ts)

	// Start it 3 times i.e. restart it 2 times.
	for i := 1; i <= 3; i++ {
		// Run the manager in the background.
		wg, _, cancel := StartManager(m)

		// Shut it down and wait for the shutdown to complete.
		cancel()
		wg.Wait()
	}
}

// TestManagerSimpleRun starts and stops a job within a Manager.
func TestManagerSimpleRun(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	m := NewManager(ts)

	// Run the manager in the background.
	wg, _, cancel := StartManager(m)

	// Create a Sleep job.
	uuid, err := m.Create(context.Background(), sleepFactoryName, []string{"-duration", "60"})
	if err != nil {
		t.Fatalf("cannot create sleep workflow: %v", err)
	}

	// Start the job
	if err := m.Start(context.Background(), uuid); err != nil {
		t.Fatalf("cannot start sleep workflow: %v", err)
	}

	// Stop the job
	if err := m.Stop(context.Background(), uuid); err != nil {
		t.Fatalf("cannot start sleep workflow: %v", err)
	}

	cancel()
	wg.Wait()
}

// TestManagerRestart starts a job within a manager, stops the
// manager, restarts a manager, and stops the job.
func TestManagerRestart(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	m := NewManager(ts)

	// Run the manager in the background.
	wg, _, cancel := StartManager(m)

	// Create a Sleep job.
	uuid, err := m.Create(context.Background(), sleepFactoryName, []string{"-duration", "60"})
	if err != nil {
		t.Fatalf("cannot create sleep workflow: %v", err)
	}

	// Start the job.
	if err := m.Start(context.Background(), uuid); err != nil {
		t.Fatalf("cannot start sleep workflow: %v", err)
	}

	// Stop the manager.
	cancel()
	wg.Wait()
	// Recreate the manager imitating restart.
	m = NewManager(ts)

	// Make sure the workflow is still in the topo server.  This
	// validates that interrupting the Manager leaves the jobs in
	// the right state in the topo server.
	wi, err := ts.GetWorkflow(context.Background(), uuid)
	if err != nil {
		t.Fatalf("cannot read workflow %v: %v", uuid, err)
	}
	if wi.State != workflowpb.WorkflowState_Running {
		t.Fatalf("unexpected workflow state %v was expecting %v", wi.State, workflowpb.WorkflowState_Running)
	}

	// Restart the manager.
	wg, _, cancel = StartManager(m)

	// Make sure the job is in there shortly.
	timeout := 0
	for {
		tree, err := m.NodeManager().GetFullTree()
		if err != nil {
			t.Fatalf("cannot get full node tree: %v", err)
		}
		if strings.Contains(string(tree), uuid) {
			break
		}
		timeout++
		if timeout == 1000 {
			t.Fatalf("failed to wait for full node tree to appear: %v", string(tree))
		}
		time.Sleep(time.Millisecond)
	}

	// Stop the job. Note Stop() waits until the background go
	// routine that saves the job is done, so when we return from
	// this call, the job is saved with the right updated State
	// inside the topo server.
	if err := m.Stop(context.Background(), uuid); err != nil {
		t.Fatalf("cannot stop sleep workflow: %v", err)
	}

	// And stop the manager.
	cancel()
	wg.Wait()

	// Make sure the workflow is stopped in the topo server.
	wi, err = ts.GetWorkflow(context.Background(), uuid)
	if err != nil {
		t.Fatalf("cannot read workflow %v: %v", uuid, err)
	}
	if wi.State != workflowpb.WorkflowState_Done {
		t.Fatalf("unexpected workflow state %v was expecting %v", wi.State, workflowpb.WorkflowState_Running)
	}
	if !strings.Contains(wi.Error, "canceled") {
		t.Errorf("invalid workflow error: %v", wi.Error)
	}
}
