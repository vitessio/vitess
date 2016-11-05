package workflow

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
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

	// workflow is the workflow which owns this node.
	workflow Workflow

	// insideModify is a marker set by the Modify method.
	// It is checked by methods that need to be called inside Modify.
	insideModify bool

	// Next are all the attributes that are exported to node.ts.

	Name            string                   `json:"name"`
	Path            string                   `json:"path"`
	Children        []*Node                  `json:"children,omitempty"`
	LastChanged     int64                    `json:"lastChanged"`
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

// NewToplevelNode returns a UI Node matching the toplevel workflow.
func NewToplevelNode(wi *topo.WorkflowInfo, w Workflow) *Node {
	return &Node{
		workflow: w,

		Name:        wi.Name,
		Path:        "/" + wi.Uuid,
		Children:    []*Node{},
		LastChanged: time.Now().Unix(),
	}
}

// Modify executes the makeChanges function while the node is locked.
// FIXME(alainjobart) if the Children change, we need to notify the
// watchers by sending the new children.
func (n *Node) Modify(makeChanges func()) {
	n.nodeManager.mu.Lock()
	defer n.nodeManager.mu.Unlock()
	n.insideModify = true
	makeChanges()
	n.insideModify = false
	n.LastChanged = time.Now().Unix()
	n.broadcastLocked()
}

// broadcastLocked sends the new value of the node to the watchers.
// Has to be called with the manager lock.
func (n *Node) broadcastLocked() {
	u := &Update{
		Nodes: []*Node{n},
	}
	n.nodeManager.broadcastUpdateLocked(u)
}

// AddChild is a helper method to add a child to the current Node.
// It must be called within Modify's makeChanges method.
func (n *Node) AddChild(relativePath string) (*Node, error) {
	if !n.insideModify {
		return nil, fmt.Errorf("AddChild must be called within Modify")
	}
	fullPath := path.Join(n.Path, relativePath)

	for _, child := range n.Children {
		if child.Path == fullPath {
			return nil, fmt.Errorf("node %v already has a child name %v", n.Path, relativePath)
		}
	}

	child := &Node{
		nodeManager: n.nodeManager,
		workflow:    n.workflow,
		Path:        fullPath,
	}
	n.Children = append(n.Children, child)
	return child, nil
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
	if strings.Contains(n.Path[1:], "/") {
		return fmt.Errorf("node %v is not a root node", n.Path)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.roots[n.Path]; ok {
		return fmt.Errorf("toplevel node %v already exists", n.Name)
	}
	n.nodeManager = m
	m.roots[n.Path] = n
	n.broadcastLocked()

	return nil
}

// RemoveRootNode removes a toplevel Node from the NodeManager,
// and broadcasts the change to the listeners.
func (m *NodeManager) RemoveRootNode(n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.roots, n.Path)

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
	return n.workflow.Action(ctx, ap.Path, ap.Name)
}

func (m *NodeManager) getNodeByPath(nodePath string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the starting node. Path is something like:
	// /XXXX-XXXX-XXX
	// /XXXX-XXXX-XXX/child1
	// /XXXX-XXXX-XXX/child1/grandchild1
	// So parts[0] will always be empty, and parts[1] will always
	// be the UUID.
	parts := strings.Split(nodePath, "/")
	n, ok := m.roots["/"+parts[1]]
	if !ok {
		return nil, fmt.Errorf("no root node named /%v", parts[1])
	}

	// Find the subnode if needed.
	for i := 1; i < len(parts)-1; i++ {
		subPath := "/" + path.Join(parts[1:i+2]...)
		found := false
		for _, sn := range n.Children {
			if sn.Path == subPath {
				found = true
				n = sn
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("node %v has no children named %v", n.Path, subPath)
		}
	}

	return n, nil
}
