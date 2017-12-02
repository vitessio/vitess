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
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	log "github.com/golang/glog"
	gouuid "github.com/pborman/uuid"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

var (
	// factories has all the factories we know about.
	factories = make(map[string]Factory)
)

// Workflow is a running instance of a job.
type Workflow interface {
	// Run runs the Workflow within the provided WorkflowManager.
	// It should return ctx.Err() if ctx.Done() is closed.  The
	// Run method can alter the provided WorkflowInfo to save its
	// state (and it can checkpoint that new value by saving it
	// into the manager's topo Server).
	Run(ctx context.Context, manager *Manager, wi *topo.WorkflowInfo) error
}

// Factory can create the initial version of a Workflow, or
// instantiate them from a serialized version.
type Factory interface {
	// Init initializes the private parts of the workflow object.
	// The passed in workflow will have its Uuid, FactoryName and State
	// variable filled it. This Init method should fill in the
	// Name and Data attributes, based on the provided args.
	// This is called during the Manager.Create phase and will initially
	// checkpoint the workflow in the topology.
	// The Manager object is passed to Init method since the resharding workflow
	// will use the topology server in Manager.
	Init(m *Manager, w *workflowpb.Workflow, args []string) error

	// Instantiate loads a workflow from the proto representation
	// into an in-memory Workflow object. rootNode is the root UI node
	// representing the workflow.
	Instantiate(m *Manager, w *workflowpb.Workflow, rootNode *Node) (Workflow, error)
}

// Manager is the main Workflow manager object.
// Its management API allows it to create, start and stop workflows.
type Manager struct {
	// ts is the topo server to use for all topo operations.
	ts *topo.Server

	// nodeManager is the NodeManager for UI display.
	nodeManager *NodeManager

	// redirectFunc is the function to use to redirect web traffic
	// to the serving Manager, when this manager is not
	// running. If it is not set, HTTP handlers will return an error.
	redirectFunc func() (string, error)

	// mu protects the next fields.
	mu sync.Mutex
	// ctx is the context passed in the run function. It is only
	// set if the manager is inside the Run() method. Outside of
	// the Run method, ctx will be nil. It means the Manager is
	// shut down, either at startup or shutdown, or is not the
	// elected master.
	ctx context.Context
	// started is used to signal that the manager is running i.e. Run() has been
	// successfully called and the manager can start workflows.
	// This channel is closed and re-created every time Run() is called.
	started chan struct{}
	// workflows is a map from job UUID to runningWorkflow.
	workflows map[string]*runningWorkflow
}

// runningWorkflow holds information about a running workflow.
type runningWorkflow struct {
	// wi is the WorkflowInfo used to store the Workflow in the
	// topo server.
	wi *topo.WorkflowInfo

	// rootNode is the root UI node corresponding to this workflow.
	rootNode *Node

	// cancel is the method associated with the context that runs
	// the workflow.
	cancel context.CancelFunc

	// workflow is the running Workflow object.
	workflow Workflow

	// done is the channel to close when the workflow is done.
	// Used for synchronization.
	done chan struct{}

	// stopped is true if the workflow was explicitly stopped (by
	// calling Manager.Stop(ctx, uuid)).  Otherwise, it remains
	// false e.g. if the Manager and its workflows are shut down
	// by canceling the context.
	stopped bool
}

// NewManager creates an initialized Manager.
func NewManager(ts *topo.Server) *Manager {
	return &Manager{
		ts:          ts,
		nodeManager: NewNodeManager(),
		started:     make(chan struct{}),
		workflows:   make(map[string]*runningWorkflow),
	}
}

// SetRedirectFunc sets the redirect function to use.
func (m *Manager) SetRedirectFunc(rf func() (string, error)) {
	m.redirectFunc = rf
}

// TopoServer returns the topo.Server used by the Manager.
// It is meant to be used by the running workflows.
func (m *Manager) TopoServer() *topo.Server {
	return m.ts
}

// NodeManager returns the NodeManager used by the Manager.
// It is meant to be used by the running workflows.
func (m *Manager) NodeManager() *NodeManager {
	return m.nodeManager
}

// Run is the main entry point for the Manager. It will read each
// checkpoint from the topo Server, and for the ones that are in the
// Running state, will load them in memory and run them.
// It will not return until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	// Save the context for all other jobs usage, and to indicate
	// the manager is running.
	m.mu.Lock()
	if m.ctx != nil {
		m.mu.Unlock()
		panic("Manager is already running")
	}
	m.ctx = ctx
	m.loadAndStartJobsLocked()
	// Signal the successful startup.
	close(m.started)
	m.started = make(chan struct{})
	m.mu.Unlock()

	// Wait for the context to be canceled.
	select {
	case <-ctx.Done():
	}

	// Clear context and get a copy of the running jobs.
	m.mu.Lock()
	m.ctx = nil
	runningWorkflows := m.workflows
	m.workflows = make(map[string]*runningWorkflow)
	m.mu.Unlock()

	// Abort the running jobs. They won't save their state as
	// m.ctx is nil and they know it means we're shutting down.
	for _, rw := range runningWorkflows {
		rw.cancel()
	}
	for _, rw := range runningWorkflows {
		select {
		case <-rw.done:
		}
	}
}

// WaitUntilRunning blocks until Run() has progressed to a state where the
// manager can start workflows. It is mainly used by tests.
func (m *Manager) WaitUntilRunning() {
	for {
		m.mu.Lock()

		if m.ctx != nil {
			m.mu.Unlock()
			return
		}

		started := m.started
		m.mu.Unlock()

		// Block until we have been started.
		<-started
	}
}

// loadAndStartJobsLocked will try to load and start all existing jobs
// in the topo Server.  It needs to be run holding m.mu.
func (m *Manager) loadAndStartJobsLocked() {
	uuids, err := m.ts.GetWorkflowNames(m.ctx)
	if err != nil {
		log.Errorf("GetWorkflowNames failed to find existing workflows: %v", err)
		return
	}

	for _, uuid := range uuids {
		// Load workflows from the topo server, only look at
		// 'Running' ones.
		wi, err := m.ts.GetWorkflow(m.ctx, uuid)
		if err != nil {
			log.Errorf("Failed to load workflow %v, will not start it: %v", uuid, err)
			continue
		}

		rw, err := m.instantiateWorkflow(wi.Workflow)
		if err != nil {
			log.Errorf("Failed to instantiate workflow %v from factory %v, will not start it: %v", uuid, wi.FactoryName, err)
			continue
		}
		rw.wi = wi

		if rw.wi.State == workflowpb.WorkflowState_Running {
			m.runWorkflow(rw)
		}
	}
}

// Create creates a workflow from the given factory name with the
// provided args.  Returns the unique UUID of the workflow. The
// workflowpb.Workflow object is saved in the topo server after
// creation.
func (m *Manager) Create(ctx context.Context, factoryName string, args []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the factory.
	factory, ok := factories[factoryName]
	if !ok {
		return "", fmt.Errorf("no factory named %v is registered", factoryName)
	}

	// Create the initial workflowpb.Workflow object.
	w := &workflowpb.Workflow{
		Uuid:        gouuid.NewUUID().String(),
		FactoryName: factoryName,
		State:       workflowpb.WorkflowState_NotStarted,
	}

	// Let the factory parse the parameters and initialize the
	// object.
	if err := factory.Init(m, w, args); err != nil {
		return "", err
	}
	rw, err := m.instantiateWorkflow(w)
	if err != nil {
		return "", err
	}

	// Now save the workflow in the topo server.
	rw.wi, err = m.ts.CreateWorkflow(ctx, w)
	if err != nil {
		return "", err
	}

	// And we're done.
	log.Infof("Created workflow %s (%s, %s)", w.Uuid, factoryName, w.Name)
	return w.Uuid, nil
}

func (m *Manager) instantiateWorkflow(w *workflowpb.Workflow) (*runningWorkflow, error) {
	rw := &runningWorkflow{
		wi: &topo.WorkflowInfo{
			Workflow: w,
		},
		rootNode: NewNode(),
		done:     make(chan struct{}),
	}
	rw.rootNode.Name = w.Name
	rw.rootNode.PathName = w.Uuid
	rw.rootNode.Path = "/" + rw.rootNode.PathName
	rw.rootNode.State = w.State

	factory, ok := factories[w.FactoryName]
	if !ok {
		return nil, fmt.Errorf("no factory named %v is registered", w.FactoryName)
	}
	var err error
	rw.workflow, err = factory.Instantiate(m, w, rw.rootNode)
	if err != nil {
		return nil, err
	}

	m.workflows[w.Uuid] = rw
	if err := m.nodeManager.AddRootNode(rw.rootNode); err != nil {
		return nil, err
	}

	return rw, nil
}

// Start will start a Workflow. It will load it in memory, update its
// status to Running, and call its Run() method.
func (m *Manager) Start(ctx context.Context, uuid string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check the manager is running.
	if m.ctx == nil {
		return fmt.Errorf("manager not running")
	}

	rw, ok := m.workflows[uuid]
	if !ok {
		return fmt.Errorf("Cannot find workflow %v in the workflow list", uuid)
	}

	if rw.wi.State != workflowpb.WorkflowState_NotStarted {
		return fmt.Errorf("workflow with uuid %v is in state %v", uuid, rw.wi.State)
	}

	// Change its state in the topo server. Note we do that first,
	// so if the running part fails, we will retry next time.
	rw.wi.State = workflowpb.WorkflowState_Running
	rw.wi.StartTime = time.Now().Unix()
	if err := m.ts.SaveWorkflow(ctx, rw.wi); err != nil {
		return err
	}

	rw.rootNode.State = workflowpb.WorkflowState_Running
	rw.rootNode.BroadcastChanges(false /* updateChildren */)

	m.runWorkflow(rw)
	return nil
}

func (m *Manager) runWorkflow(rw *runningWorkflow) {
	// Create a context to run it.
	var ctx context.Context
	ctx, rw.cancel = context.WithCancel(m.ctx)

	// And run it in the background.
	go m.executeWorkflowRun(ctx, rw)
}

func (m *Manager) executeWorkflowRun(ctx context.Context, rw *runningWorkflow) {
	defer close(rw.done)

	// Run will block until one of three things happen:
	//
	// 1. The workflow is stopped (calling Stop(uuid)). At this
	// point, err is context.Canceled (because Stop cancels the
	// individual context). We want to save our workflow state as
	// Done in the topology, using context.Canceled as the error.
	//
	// 2. The Manager is stopped (by canceling the context that was
	// passed to Run()). At this point, err is context.Canceled
	// (because the umbrella context was canceled). We do not want
	// to save our state there, so that when the Manager restarts
	// next time, it restarts the workflow where it left of.
	//
	// 3. The workflow is done (with a valid context). err can be
	// anything (including nil), we just need to save it.
	log.Infof("Running workflow %s (%s, %s)",
		rw.wi.Workflow.Uuid, rw.wi.Workflow.FactoryName, rw.wi.Workflow.Name)
	err := rw.workflow.Run(ctx, m, rw.wi)
	if err == nil {
		log.Infof("Workflow %s (%s, %s) finished successfully",
			rw.wi.Workflow.Uuid, rw.wi.Workflow.FactoryName, rw.wi.Workflow.Name)
	} else {
		log.Infof("Workflow %s (%s, %s) finished with error %v",
			rw.wi.Workflow.Uuid, rw.wi.Workflow.FactoryName, rw.wi.Workflow.Name, err)
	}

	// Change the Manager state.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for manager stoppage (case 2. above).
	if err == context.Canceled && !rw.stopped {
		return
	}

	// We are not shutting down, but this workflow is done, or
	// canceled. In any case, change its topo Server state.
	rw.wi.State = workflowpb.WorkflowState_Done
	if err != nil {
		rw.wi.Error = err.Error()
	}
	rw.wi.EndTime = time.Now().Unix()
	if err := m.ts.SaveWorkflow(m.ctx, rw.wi); err != nil {
		log.Errorf("Could not save workflow %v after completion: %v", rw.wi, err)
	}

	rw.rootNode.State = workflowpb.WorkflowState_Done
	rw.rootNode.BroadcastChanges(false /* updateChildren */)
}

// Stop stops the running workflow. It will cancel its context and
// wait for it to exit.
func (m *Manager) Stop(ctx context.Context, uuid string) error {
	// Find the workflow, mark it as stopped.
	m.mu.Lock()
	rw, ok := m.workflows[uuid]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no running workflow with uuid %v", uuid)
	}
	rw.stopped = true
	m.mu.Unlock()

	// Cancel the running guy, and waits for it.
	rw.cancel()
	select {
	case <-rw.done:
		break
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Delete deletes the finished or not started workflow.
func (m *Manager) Delete(ctx context.Context, uuid string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rw, ok := m.workflows[uuid]
	if !ok {
		return fmt.Errorf("No workflow with uuid %v", uuid)
	}
	if rw.wi.State == workflowpb.WorkflowState_Running {
		return fmt.Errorf("Cannot delete running workflow")
	}
	if err := m.ts.DeleteWorkflow(m.ctx, rw.wi); err != nil {
		log.Errorf("Could not delete workflow %v: %v", rw.wi, err)
	}
	m.nodeManager.RemoveRootNode(rw.rootNode)
	delete(m.workflows, uuid)
	return nil
}

// Wait waits for the provided workflow to end.
func (m *Manager) Wait(ctx context.Context, uuid string) error {
	// Find the workflow.
	rw, err := m.runningWorkflow(uuid)
	if err != nil {
		return err
	}

	// Just wait for it.
	select {
	case <-rw.done:
		break
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// WorkflowForTesting returns the Workflow object of the running workflow
// identified by uuid. The method is used in unit tests to inject mocks.
func (m *Manager) WorkflowForTesting(uuid string) (Workflow, error) {
	rw, err := m.runningWorkflow(uuid)
	if err != nil {
		return nil, err
	}
	return rw.workflow, nil
}

// WorkflowInfoForTesting returns the WorkflowInfo object of the running
// workflow identified by uuid. The method is used in unit tests to manipulate
// checkpoint.
func (m *Manager) WorkflowInfoForTesting(uuid string) (*topo.WorkflowInfo, error) {
	rw, err := m.runningWorkflow(uuid)
	if err != nil {
		return nil, err
	}
	return rw.wi, nil
}

// runningWorkflow returns a runningWorkflow by uuid.
func (m *Manager) runningWorkflow(uuid string) (*runningWorkflow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rw, ok := m.workflows[uuid]
	if !ok {
		return nil, fmt.Errorf("no running workflow with uuid %v", uuid)
	}
	return rw, nil
}

func (m *Manager) isRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ctx != nil
}

// getAndWatchFullTree returns the initial tree and a channel to watch
// the changes.
// If this manager is not the master, and we have a redirectFunc
// defined, the initial bytes will be set, but the channel will be nil,
// and the index is undefined.
// So return can have one of three combinations:
// 1. error case: <undefined>, <undefined>, <undefined>, error
// 2. redirect case: update, nil, <undefined>, nil
// 3. working case: update, notifications, index, nil
func (m *Manager) getAndWatchFullTree(servedURL *url.URL) ([]byte, chan []byte, int, error) {
	if !m.isRunning() {
		if m.redirectFunc == nil {
			return nil, nil, 0, fmt.Errorf("WorkflowManager is not running")
		}

		host, err := m.redirectFunc()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("WorkflowManager cannot redirect to proper Manager: %v", err)
		}

		cu := *servedURL
		cu.Host = host
		cu.Path = "/app2/workflows"
		redirect := cu.String()

		u := &Update{
			Redirect: redirect,
		}
		b, err := json.Marshal(u)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("WorkflowManager cannot JSON-encode update: %v", err)
		}

		return b, nil, -1, nil
	}

	notifications := make(chan []byte, 10)
	tree, i, err := m.NodeManager().GetAndWatchFullTree(notifications)
	if err != nil {
		log.Warningf("GetAndWatchFullTree failed: %v", err)
		return nil, nil, 0, err
	}
	return tree, notifications, i, nil
}

// Register lets implementations register Factory objects.
// Typically called at init() time.
func Register(factoryName string, factory Factory) {
	// Check for duplicates.
	if _, ok := factories[factoryName]; ok {
		panic(fmt.Errorf("duplicate factory name: %v", factoryName))
	}
	factories[factoryName] = factory
}

// Unregister removes a factory object.
// Typically called from a flag to remove dangerous workflows.
func Unregister(name string) {
	if _, ok := factories[name]; !ok {
		log.Warningf("workflow %v doesn't exist, cannot remove it", name)
	} else {
		delete(factories, name)
	}
}

// AvailableFactories returns a map with the names of the available
// factories as keys and 'true' as value.
func AvailableFactories() map[string]bool {
	result := make(map[string]bool)
	for n := range factories {
		result[n] = true
	}
	return result
}
