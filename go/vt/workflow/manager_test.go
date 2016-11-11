package workflow

import (
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func startManager(t *testing.T, m *Manager) (*sync.WaitGroup, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		m.Run(ctx)
		wg.Done()
	}()

	// Wait for the manager to start, by watching its ctx object.
	timeout := 0
	for {
		m.mu.Lock()
		running := m.ctx != nil
		m.mu.Unlock()

		if running {
			break
		}
		timeout++
		if timeout == 1000 {
			t.Fatalf("failed to wait for running manager")
		}
		time.Sleep(time.Millisecond)
	}

	return wg, cancel
}

// TestManagerSimpleRun starts and stops a job within a Manager.
func TestManagerSimpleRun(t *testing.T) {
	ts := topo.Server{Impl: memorytopo.NewMemoryTopo([]string{"cell1"})}
	m := NewManager(ts)

	// Run the manager in the background.
	wg, cancel := startManager(t, m)

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
	ts := topo.Server{Impl: memorytopo.NewMemoryTopo([]string{"cell1"})}
	m := NewManager(ts)

	// Run the manager in the background.
	wg, cancel := startManager(t, m)

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
	// Recreate the manager immitating restart.
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
	wg, cancel = startManager(t, m)

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
