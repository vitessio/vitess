package workflow

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/golang/glog"
	"github.com/gorilla/websocket"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
)

var upgrader = websocket.Upgrader{} // use default options

// HandleHTTP registers the WebSocket handler.
func (m *Manager) HandleHTTP(pattern string) {
	log.Infof("workflow Manager listening to websocket traffic at %v", pattern)
	http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
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
		notifications := make(chan []byte, 10)
		tree, i, err := m.NodeManager().GetAndWatchFullTree(notifications)
		if err != nil {
			log.Warningf("GetAndWatchFullTree failed: %v", err)
			return
		}
		defer m.NodeManager().CloseWatcher(i)

		// First we send the full dump
		if err := c.WriteMessage(websocket.TextMessage, tree); err != nil {
			log.Warningf("WriteMessage(tree) failed: %v", err)
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
