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
	"net/http"

	"github.com/gorilla/websocket"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
)

var upgrader = websocket.Upgrader{} // use default options

// HandleHTTPWebSocket registers the WebSocket handler.
func (m *Manager) HandleHTTPWebSocket(pattern string) {
	log.Infof("workflow Manager listening to websocket traffic at %v", pattern)
	http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if x := recover(); x != nil {
				errMsg := fmt.Sprintf("uncaught panic: %v", x)
				log.Error(errMsg)
				http.Error(w, errMsg, http.StatusInternalServerError)
			}
		}()

		// Check ACL.
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			msg := fmt.Sprintf("WorkflowManager acl.CheckAccessHTTP failed: %v", err)
			log.Error(msg)
			http.Error(w, msg, http.StatusUnauthorized)
			return
		}

		// Upgrade to WebSocket.
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Errorf("upgrade error: %v", err)
			return
		}
		defer c.Close()

		// Register the handler.
		tree, notifications, i, err := m.getAndWatchFullTree(r.URL)
		if err != nil {
			log.Warningf("GetAndWatchFullTree failed: %v", err)
			return
		}
		if notifications != nil {
			defer m.NodeManager().CloseWatcher(i)
		}

		// First we send the full dump
		if err := c.WriteMessage(websocket.TextMessage, tree); err != nil {
			log.Warningf("WriteMessage(tree) failed: %v", err)
			return
		}

		// If we didn't get a channel back (redirect case), we're done.
		// We will just return the redirect, and close the websocket.
		if notifications == nil {
			return
		}

		// Start a go routine to get messages, send them to a channel.
		recv := make(chan *ActionParameters, 10)
		go func() {
			for {
				mt, message, err := c.ReadMessage()
				if err != nil {
					log.Warningf("failed to read message from websocket: %v", err)
					close(recv)
					return
				}
				if mt != websocket.TextMessage {
					log.Warningf("weird message type: %v", mt)
				}

				ap := &ActionParameters{}
				if err := json.Unmarshal(message, ap); err != nil {
					log.Warningf("failed to JSON-decode message from websocket: %v", err)
					close(recv)
					return
				}
				recv <- ap
			}
		}()

		// Let's listen to the channels until we're done.
		for {
			select {
			case ap, ok := <-recv:
				if !ok {
					// The websocket was most likely closed.
					return
				}

				ctx := context.TODO()
				if err := m.NodeManager().Action(ctx, ap); err != nil {
					log.Warningf("Action failed: %v", err)
				}

			case message, ok := <-notifications:
				if !ok {
					// We ran out of space on the update
					// channel, so we had to close it.
					return
				}
				if err := c.WriteMessage(websocket.TextMessage, message); err != nil {
					log.Warningf("WriteMessage(tree) failed: %v", err)
					return
				}
			}
		}
	})
}
