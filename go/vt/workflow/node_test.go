package workflow

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

type testWorkflow struct {
	mu      sync.Mutex
	actions []*ActionParameters
}

func (tw *testWorkflow) Run(ctx context.Context, manager *Manager, wi *topo.WorkflowInfo) error {
	return fmt.Errorf("not implemented")
}

func (tw *testWorkflow) Action(ctx context.Context, path, name string) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.actions = append(tw.actions, &ActionParameters{
		Path: path,
		Name: name,
	})
	return nil
}

// TestNodeManagerWithRoot unit tests basic NodeManager functionality
// wiht a single root node.
func TestNodeManagerWithRoot(t *testing.T) {
	nodeManager := NewNodeManager()
	tw := &testWorkflow{}

	// Subscribe a watcher.
	notifications := make(chan []byte, 10)
	result, index, err := nodeManager.GetAndWatchFullTree(notifications)
	if err != nil {
		t.Fatalf("GetAndWatchFullTree failed: %v", err)
	}
	defer nodeManager.CloseWatcher(index)
	if string(result) != `{"index":1,"fullUpdate":true}` {
		t.Errorf("unexpected first result: %v", string(result))
	}

	// Add a root level node, make sure we get notified.
	n := &Node{
		Listener: tw,

		Name:        "name",
		PathName:    "uuid1",
		Children:    []*Node{},
		LastChanged: time.Now().Unix(),
	}
	if err := nodeManager.AddRootNode(n); err != nil {
		t.Fatalf("adding root node failed: %v", err)
	}
	result, ok := <-notifications
	if !ok || !strings.Contains(string(result), `"name":"name"`) {
		t.Errorf("unexpected notification: %v %v", ok, string(result))
	}

	// Modify root node, make sure we get notified.
	n.Name = "name2"
	n.BroadcastChanges(false /* updateChildren */)
	result, ok = <-notifications
	if !ok ||
		!strings.Contains(string(result), `"name":"name2"`) ||
		strings.Contains(string(result), `"children":[]`) ||
		strings.Contains(string(result), `"fullUpdate":true`) {
		t.Errorf("unexpected notification: %v %v", ok, string(result))
	}

	// Trigger an action, make sure it goes through.
	// This is a synchronous call, no need to take tw.mu.
	if err := nodeManager.Action(context.Background(), &ActionParameters{
		Path: n.Path,
		Name: "action",
	}); err != nil {
		t.Errorf("unexpected Action error: %v", err)
	}
	if len(tw.actions) != 1 || tw.actions[0].Path != n.Path || tw.actions[0].Name != "action" {
		t.Errorf("unexpected Ation callback values: %v", tw.actions)
	}

	// Delete root node, make sure we get notified.
	nodeManager.RemoveRootNode(n)
	result, ok = <-notifications
	if !ok || !strings.Contains(string(result), `"deletes":["/uuid1"]`) {
		t.Errorf("unexpected notification: %v %v", ok, string(result))
	}
}
