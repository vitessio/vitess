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
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

// This file contains the necessary object definitions and interfaces
// to expose a workflow display to the vtctld webapp.
//
// Most constants and variable names should match the Node object described in
// web/vtctld2/src/app/workflows/node.ts as it is exposed as JSON to
// the Angular 2 web app.

// NodeDisplay constants need to match node.ts.Display.
type NodeDisplay int

const (
	// NodeDisplayUnknown is an unknown value and should never be set.
	NodeDisplayUnknown NodeDisplay = 0

	// NodeDisplayIndeterminate is a progress bar that doesn't have
	// a current value, but just shows movement.
	NodeDisplayIndeterminate NodeDisplay = 1

	// NodeDisplayDeterminate is a progress bar driven by the
	// Progress field.
	NodeDisplayDeterminate NodeDisplay = 2

	// NodeDisplayNone shows no progress bar or status.
	NodeDisplayNone NodeDisplay = 3
)

// ActionState constants need to match node.ts.ActionState.
type ActionState int

const (
	// ActionStateUnknown is an unknown value and should never be set.
	ActionStateUnknown ActionState = 0

	// ActionStateEnabled is for when the action is enabled.
	ActionStateEnabled ActionState = 1

	// ActionStateDisabled is for when the action is disabled.
	ActionStateDisabled ActionState = 2
)

// ActionStyle constants need to match node.ts.ActionStyle.
type ActionStyle int

const (
	// ActionStyleUnknown is an unknown value and should never be set.
	ActionStyleUnknown ActionStyle = 0

	// ActionStyleNormal will just trigger the action.
	ActionStyleNormal ActionStyle = 1

	// ActionStyleWarning will display a warning dialog to confirm
	// action with Action.Message.
	ActionStyleWarning ActionStyle = 2

	// ActionStyleWaiting highlights to the user that the process
	// is waiting on the execution of the action.
	ActionStyleWaiting ActionStyle = 3

	// ActionStyleTriggered is a state where the button is greyed
	// out and cannot be pressed.
	ActionStyleTriggered ActionStyle = 4
)

// ActionListener is an interface for receiving notifications about actions triggered
// from workflow UI.
type ActionListener interface {
	// Action is called when the user requests an action on a node.
	// 'path' is the node's Path value and 'name' is the invoked action's name.
	Action(ctx context.Context, path, name string) error
}

// Node is the UI representation of a Workflow toplevel object, or of
// a Workflow task. It is just meant to be a tree, and the various
// Workflow implementations can expose a tree of Nodes that represent
// what they are doing.
//
// Actions are meant to be buttons, that will trigger the Action
// callback of a workflow.
//
// In order for the web UIs to be notified when changing this
// structure, any change to this Node has to be done inside a Modify()
// function.
//
// It should match the Node object described in
// web/vtctld2/src/app/workflows/node.ts as it is exposed as JSON to
// the Angular 2 web app.
type Node struct {
	// nodeManager is the NodeManager handling this Node.
	// It is set by AddRootNode, and propagated by AddChild.
	// Any change to this node must take the Manager's lock.
	nodeManager *NodeManager

	// Listener will be notified about actions invoked on this node.
	Listener ActionListener `json:"-"`

	// Next are all the attributes that are exported to node.ts.
	// Note that even though Path is publicly accessible it should be
	// changed only by Node and NodeManager, otherwise things can break.
	// It's public only to be exportable to json. Similarly PathName (which
	// is the last component of path) on top-level node should be populated
	// only by NodeManager. For non-top-level children it can be changed
	// at any time, Path will be adjusted correspondingly when the parent
	// node's BroadcastChanges() is called.

	Name            string                   `json:"name"`
	PathName        string                   `json:"pathName"`
	Path            string                   `json:"path"`
	Children        []*Node                  `json:"children,omitempty"`
	LastChanged     int64                    `json:"lastChanged"`
	CreateTime      int64                    `json:"createTime"`
	Progress        int                      `json:"progress"`
	ProgressMessage string                   `json:"progressMsg"`
	State           workflowpb.WorkflowState `json:"state"`
	Display         NodeDisplay              `json:"display,omitempty"`
	Message         string                   `json:"message"`
	Log             string                   `json:"log"`
	Disabled        bool                     `json:"disabled"`
	Actions         []*Action                `json:"actions"`
}

// Action must match node.ts Action.
type Action struct {
	Name    string      `json:"name"`
	State   ActionState `json:"state,omitempty"`
	Style   ActionStyle `json:"style,omitempty"`
	Message string      `json:"message"`
}

// Update is the data structure we send on the websocket or on the
// long-polling HTTP connection to the clients.
type Update struct {
	// Redirect is set to the URL to go to if we are not the
	// master.  It is only set in the initial response, and if set
	// then no other field in this structure is set.
	Redirect string `json:"redirect,omitempty"`

	// Index is the watcher index. It is only set in the initial
	// tree.
	Index int `json:"index,omitempty"`

	// Nodes is a list of nodes to update.
	Nodes []*Node `json:"nodes,omitempty"`

	// Deletes is a list of toplevel paths to delete.
	Deletes []string `json:"deletes,omitempty"`

	// FullUpdate is set to true if this is a full refresh of the data.
	FullUpdate bool `json:"fullUpdate,omitempty"`
}

// NewNode is a helper function to create new UI Node struct.
func NewNode() *Node {
	return &Node{
		Children: []*Node{},
		Actions:  []*Action{},
	}
}

// BroadcastChanges sends the new contents of the node to the watchers.
func (n *Node) BroadcastChanges(updateChildren bool) error {
	n.nodeManager.mu.Lock()
	defer n.nodeManager.mu.Unlock()
	return n.nodeManager.updateNodeAndBroadcastLocked(n, updateChildren)
}

// deepCopyFrom copies contents of otherNode into this node. Contents of Actions
// is copied into new Action objects, so that changes in otherNode are not
// immediately visible in this node. When copyChildren is false the contents of
// Children in this node is preserved fully even if it doesn't match the contents
// of otherNode. When copyChildren is true then contents of Children is copied
// into new Node objects similar to contents of Actions.
// Method returns error if children in otherNode have non-unique values of
// PathName, i.e. it's impossible to create unique values of Path for them.
func (n *Node) deepCopyFrom(otherNode *Node, copyChildren bool) error {
	oldChildren := n.Children
	*n = *otherNode
	n.Children = oldChildren

	n.Actions = []*Action{}
	for _, otherAction := range otherNode.Actions {
		action := &Action{}
		*action = *otherAction
		n.Actions = append(n.Actions, action)
	}

	if !copyChildren {
		return nil
	}
	n.Children = []*Node{}
	childNamesSet := make(map[string]bool)
	for _, otherChild := range otherNode.Children {
		if _, ok := childNamesSet[otherChild.PathName]; ok {
			return fmt.Errorf("node %v already has a child name %v", n.Path, otherChild.PathName)
		}
		childNamesSet[otherChild.PathName] = true

		// Populate a few values in case the otherChild is newly created by user.
		otherChild.nodeManager = n.nodeManager
		otherChild.Path = path.Join(n.Path, otherChild.PathName)

		child := NewNode()
		child.deepCopyFrom(otherChild, true /* copyChildren */)
		n.Children = append(n.Children, child)
	}
	return nil
}

// GetChildByPath returns the child node given the relative path to this node.
// The caller must ensure that the node tree is not modified during the call.
func (n *Node) GetChildByPath(subPath string) (*Node, error) {
	// Find the subnode if needed.
	parts := strings.Split(subPath, "/")

	currentNode := n
	for i := 0; i < len(parts); i++ {
		childPathName := parts[i]
		found := false
		for _, child := range currentNode.Children {
			if child.PathName == childPathName {
				found = true
				currentNode = child
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("node %v has no children named %v", currentNode.Path, childPathName)
		}
	}
	return currentNode, nil
}

// ActionParameters describe an action initiated by the user.
type ActionParameters struct {
	// Path is the path of the Node the action was performed on.
	Path string `json:"path"`

	// Name is the Name of the Action.
	Name string `json:"name"`
}

// NodeManager manages the Node tree.
type NodeManager struct {
	// mu protects all the fields.
	mu sync.Mutex

	// roots points at the root nodes, by uuid.
	roots map[string]*Node

	// watchers is a map of channels that need notifications when
	// we change our tree.
	watchers map[int]chan []byte

	// nextWatcherIndex is the index of the next registered watcher.
	nextWatcherIndex int
}

// NewNodeManager returns a new NodeManager.
func NewNodeManager() *NodeManager {
	return &NodeManager{
		roots:            make(map[string]*Node),
		watchers:         make(map[int]chan []byte),
		nextWatcherIndex: 1,
	}
}

// AddRootNode adds a toplevel Node to the NodeManager,
// and broadcasts the Node to the listeners.
func (m *NodeManager) AddRootNode(n *Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.roots[n.PathName]; ok {
		return fmt.Errorf("toplevel node %v (with name %v) already exists", n.Path, n.Name)
	}
	n.Path = "/" + n.PathName
	n.nodeManager = m

	savedNode := NewNode()
	m.roots[n.PathName] = savedNode
	return m.updateNodeAndBroadcastLocked(n, true /* updateChildren */)
}

// RemoveRootNode removes a toplevel Node from the NodeManager,
// and broadcasts the change to the listeners.
func (m *NodeManager) RemoveRootNode(n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.roots, n.PathName)

	u := &Update{
		Deletes: []string{n.Path},
	}
	m.broadcastUpdateLocked(u)
}

func (m *NodeManager) toJSON(index int) ([]byte, error) {
	u := &Update{
		Index:      index,
		FullUpdate: true,
	}
	for _, n := range m.roots {
		u.Nodes = append(u.Nodes, n)
	}
	sort.Slice(u.Nodes, func(i, j int) bool {
		return u.Nodes[i].CreateTime < u.Nodes[j].CreateTime
	})
	return json.Marshal(u)
}

// GetFullTree returns the JSON representation of the entire Node tree.
func (m *NodeManager) GetFullTree() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.toJSON(0)
}

// GetAndWatchFullTree returns the JSON representation of the entire Node tree,
// and registers a watcher to monitor changes to the tree.
func (m *NodeManager) GetAndWatchFullTree(notifications chan []byte) ([]byte, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	i := m.nextWatcherIndex
	m.nextWatcherIndex++

	result, err := m.toJSON(i)
	if err != nil {
		return nil, 0, err
	}

	// It worked, register the watcher.
	m.watchers[i] = notifications
	return result, i, nil
}

// broadcastUpdateLocked sends the provided update to all watchers.
// Has to be called with the lock.
func (m *NodeManager) broadcastUpdateLocked(u *Update) {
	data, err := json.Marshal(u)
	if err != nil {
		log.Errorf("Cannot JSON encode update: %v", err)
		return
	}

	// If we can't write on the channel, we close it and remove it
	// from the list. It probably means the web browser on the
	// other side is not there any more.
	for i, watcher := range m.watchers {
		select {
		case watcher <- data:
		default:
			log.Infof("Couldn't write to watcher %v, closing it.", i)
			close(watcher)
			delete(m.watchers, i)
		}
	}
}

// updateNodeAndBroadcastLocked updates the contents of the Node saved inside node manager that
// corresponds to the provided user node, and then broadcasts the contents to all watchers.
// If updateChildren is false then contents of the node's children (as well as the list of
// children) is not updated and not included into the broadcast.
// Has to be called with the lock.
func (m *NodeManager) updateNodeAndBroadcastLocked(userNode *Node, updateChildren bool) error {
	savedNode, err := m.getNodeByPathLocked(userNode.Path)
	if err != nil {
		return err
	}

	userNode.LastChanged = time.Now().Unix()
	if err := savedNode.deepCopyFrom(userNode, updateChildren); err != nil {
		return err
	}

	savedChildren := savedNode.Children
	if !updateChildren {
		// Note that since we are under mutex it's okay to temporarily change
		// Children right here in-place.
		savedNode.Children = nil
	}

	u := &Update{
		Nodes: []*Node{savedNode},
	}
	m.broadcastUpdateLocked(u)

	savedNode.Children = savedChildren
	return nil
}

// CloseWatcher unregisters the watcher from this Manager.
func (m *NodeManager) CloseWatcher(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.watchers, i)
}

// Action is called by the UI agents to trigger actions.
func (m *NodeManager) Action(ctx context.Context, ap *ActionParameters) error {
	n, err := m.getNodeByPath(ap.Path)
	if err != nil {
		return err
	}

	m.mu.Lock()

	if n.Listener == nil {
		m.mu.Unlock()
		return fmt.Errorf("Action %v is invoked on a node without listener (node path is %v)", ap.Name, ap.Path)
	}
	nodeListener := n.Listener
	m.mu.Unlock()

	return nodeListener.Action(ctx, ap.Path, ap.Name)
}

func (m *NodeManager) getNodeByPath(nodePath string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getNodeByPathLocked(nodePath)
}

func (m *NodeManager) getNodeByPathLocked(nodePath string) (*Node, error) {
	// Find the starting node. Path is something like:
	// /XXXX-XXXX-XXX
	// /XXXX-XXXX-XXX/child1
	// /XXXX-XXXX-XXX/child1/grandchild1
	// So parts[0] will always be empty, and parts[1] will always
	// be the UUID.
	parts := strings.Split(nodePath, "/")
	n, ok := m.roots[parts[1]]
	if !ok {
		return nil, fmt.Errorf("no root node with path /%v", parts[1])
	}

	// Find the subnode if needed.
	for i := 1; i < len(parts)-1; i++ {
		childPathName := parts[i+1]
		found := false
		for _, sn := range n.Children {
			if sn.PathName == childPathName {
				found = true
				n = sn
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("node %v has no children named %v", n.Path, childPathName)
		}
	}

	return n, nil
}
