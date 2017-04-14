package workflow

import (
	"bytes"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo/memorytopo"
)

func TestLongPolling(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	m := NewManager(ts)

	// Register the manager to a web handler, start a web server.
	m.HandleHTTPLongPolling("/workflow")
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	go http.Serve(listener, nil)

	// Run the manager in the background.
	wg, cancel := startManager(t, m)

	// Get the original tree with a 'create'.
	u := url.URL{Scheme: "http", Host: listener.Addr().String(), Path: "/workflow/create"}
	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatalf("/create failed: %v", err)
	}
	tree, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("/create reading failed: %v", err)
	}
	if string(tree) != `{"index":1,"fullUpdate":true}` {
		t.Errorf("unexpected first result: %v", string(tree))
	}

	// Add a node, make sure we get the update with the next poll
	tw := &testWorkflow{}
	n := &Node{
		Listener: tw,

		Name:        "name",
		PathName:    "uuid1",
		Children:    []*Node{},
		LastChanged: 143,
	}
	if err := m.NodeManager().AddRootNode(n); err != nil {
		t.Fatalf("adding root node failed: %v", err)
	}

	u.Path = "/workflow/poll/1"
	resp, err = http.Get(u.String())
	if err != nil {
		t.Fatalf("/poll/1 failed: %v", err)
	}
	tree, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("/poll/1 reading failed: %v", err)
	}
	if !strings.Contains(string(tree), `"name":"name"`) ||
		!strings.Contains(string(tree), `"path":"/uuid1"`) {
		t.Errorf("unexpected first result: %v", string(tree))
	}

	// Trigger an action, make sure it goes through.
	u.Path = "/workflow/action/1"
	message := `{"path":"/uuid1","name":"button1"}`
	buf := bytes.NewReader([]byte(message))
	if _, err := http.Post(u.String(), "application/json; charset=utf-8", buf); err != nil {
		t.Fatalf("/action/1 post failed: %v", err)
	}
	for timeout := 0; ; timeout++ {
		// This is an asynchronous action, need to take the lock.
		tw.mu.Lock()
		if len(tw.actions) == 1 && tw.actions[0].Path == n.Path && tw.actions[0].Name == "button1" {
			tw.mu.Unlock()
			break
		}
		tw.mu.Unlock()
		timeout++
		if timeout == 1000 {
			t.Fatalf("failed to wait for action")
		}
		time.Sleep(time.Millisecond)
	}

	// Send an update, make sure we see it.
	n.Name = "name2"
	n.BroadcastChanges(false /* updateChildren */)

	u.Path = "/workflow/poll/1"
	resp, err = http.Get(u.String())
	if err != nil {
		t.Fatalf("/poll/1 failed: %v", err)
	}
	tree, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("/poll/1 reading failed: %v", err)
	}
	if !strings.Contains(string(tree), `"name":"name2"`) {
		t.Errorf("unexpected update result: %v", string(tree))
	}

	// Stop the manager.
	cancel()
	wg.Wait()
}
