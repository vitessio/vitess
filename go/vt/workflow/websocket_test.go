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
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestWebSocket(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	m := NewManager(ts)

	// Register the manager to a web handler, start a web server.
	m.HandleHTTPWebSocket("/workflow")
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	go http.Serve(listener, nil)

	// Run the manager in the background.
	wg, _, cancel := StartManager(m)

	// Start a client websocket.
	u := url.URL{Scheme: "ws", Host: listener.Addr().String(), Path: "/workflow"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("WebSocket dial failed: %v", err)
	}

	// Read the original full dump.
	_, tree, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("WebSocket first read failed: %v", err)
	}
	if string(tree) != `{"index":1,"fullUpdate":true}` {
		t.Errorf("unexpected first result: %v", string(tree))
	}

	// Add a node, make sure we get the update.
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
	_, tree, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("WebSocket first read failed: %v", err)
	}
	if !strings.Contains(string(tree), `"name":"name"`) ||
		!strings.Contains(string(tree), `"path":"/uuid1"`) {
		t.Errorf("unexpected first result: %v", string(tree))
	}

	// Trigger an action, make sure it goes through.
	message := `{"path":"/uuid1","name":"button1"}`
	if err := c.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
		t.Errorf("unexpected WebSocket.WriteMessage error: %v", err)
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
	_, tree, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("WebSocket update read failed: %v", err)
	}
	if !strings.Contains(string(tree), `"name":"name2"`) {
		t.Errorf("unexpected update result: %v", string(tree))
	}

	// Close websocket, stop the manager.
	c.Close()
	cancel()
	wg.Wait()
}
