package workflow

import (
	"fmt"
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

	// Action is called when the user requests an action on a node.
	Action(ctx context.Context, path, name string) error
}

// Factory can create the initial version of a Workflow, or
// instantiate them from a serialized version.
type Factory interface {
	// Init initializes the private parts of the workflow object.
	// The passed in workflow will have its Uuid, FactoryName and State
	// variable filled it. This Init method should fill in the
	// Name and Data attributes, based on the provided args.
	// This is called during the Manager.Create phase.
	Init(w *workflowpb.Workflow, args []string) error

	// Instantiate loads a workflow from the proto representation
	// into an in-memory Workflow object.
	Instantiate(w *workflowpb.Workflow) (Workflow, error)
}

// Manager is the main Workflow manager object.
// Its management API allows it to create, start and stop workflows.
type Manager struct {
	// ts is the topo server to use for all topo operations.
	ts topo.Server

	// nodeManager is the NodeManager for UI display.
	nodeManager *NodeManager

	// mu protects the next fields.
	mu sync.Mutex
	// ctx is the context passed in the run function. It is only
	// set if the manager is inside the Run() method. Outside of
	// the Run method, ctx will be nil. It means the Manager is
	// shut down, either at startup or shutdown, or is not the
	// elected master.
	ctx context.Context
	// workflows is a map from job UUID to runningWorkflow.
	workflows map[string]*runningWorkflow
}

// runningWorkflow holds information about a running workflow.
type runningWorkflow struct {
	// wi is the WorkflowInfo used to store the Workflow in the
	// topo server.
	wi *topo.WorkflowInfo

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
func NewManager(ts topo.Server) *Manager {
	return &Manager{
		ts:          ts,
		nodeManager: NewNodeManager(),
		workflows:   make(map[string]*runningWorkflow),
	}
}

// TopoServer returns the topo.Server used by the Manager.
// It is meant to be used by the running workflows.
func (m *Manager) TopoServer() topo.Server {
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
		panic("Manager is already running")
	}
	m.ctx = ctx
	m.loadAndStartJobsLocked()
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
		if wi.State != workflowpb.WorkflowState_Running {
			continue
		}

		// Ask the factory to create the running workflow.
		factory, ok := factories[wi.FactoryName]
		if !ok {
			log.Errorf("Saved workflow %v is using factory name %v but no such factory registered, will not start it", uuid, wi.FactoryName)
			continue
		}
		w, err := factory.Instantiate(wi.Workflow)
		if err != nil {
			log.Errorf("Failed to instantiate workflow %v from factory %v, will not start it: %v", uuid, wi.FactoryName, err)
			continue
		}

		// Create a context to run it, and save the runningWorkflow.
		ctx, cancel := context.WithCancel(m.ctx)
		rw := &runningWorkflow{
			wi:       wi,
			cancel:   cancel,
			workflow: w,
			done:     make(chan struct{}),
		}
		m.workflows[uuid] = rw

		// And run it in the background.
		go m.runWorkflow(ctx, uuid, rw)
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
	if err := factory.Init(w, args); err != nil {
		return "", err
	}

	// Now save the workflow in the topo server.
	_, err := m.ts.CreateWorkflow(ctx, w)
	if err != nil {
		return "", err
	}

	// And we're done.
	return w.Uuid, nil
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

	// Make sure it is not running already.
	if _, ok := m.workflows[uuid]; ok {
		return fmt.Errorf("workflow %v is already running", uuid)
	}

	// Load it from the topo server, make sure it has the right state.
	wi, err := m.ts.GetWorkflow(ctx, uuid)
	if err != nil {
		return err
	}
	if wi.State != workflowpb.WorkflowState_NotStarted {
		return fmt.Errorf("workflow with uuid %v is in state %v", uuid, wi.State)
	}
	factory, ok := factories[wi.FactoryName]
	if !ok {
		return fmt.Errorf("workflow %v is using factory name %v but no such factory registered", uuid, wi.FactoryName)
	}

	// Change its state in the topo server. Note we do that first,
	// so if the running part fails, we will retry next time.
	wi.State = workflowpb.WorkflowState_Running
	wi.StartTime = time.Now().Unix()
	if err := m.ts.SaveWorkflow(ctx, wi); err != nil {
		return err
	}

	// Ask the factory to create the running workflow.
	w, err := factory.Instantiate(wi.Workflow)
	if err != nil {
		return err
	}

	// Create a context to run it, and save the runningWorkflow.
	ctx, cancel := context.WithCancel(m.ctx)
	rw := &runningWorkflow{
		wi:       wi,
		cancel:   cancel,
		workflow: w,
		done:     make(chan struct{}),
	}
	m.workflows[uuid] = rw

	// And run it in the background.
	go m.runWorkflow(ctx, uuid, rw)

	return nil
}

func (m *Manager) runWorkflow(ctx context.Context, uuid string, rw *runningWorkflow) {
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
	err := rw.workflow.Run(ctx, m, rw.wi)

	// Change the Manager state.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for manager stoppage (case 2. above).
	if err == context.Canceled && !rw.stopped {
		return
	}

	// We are not shutting down, but this workflow is done, or
	// canceled. In any case, delete it from the running
	// workflows, and change its topo Server state.
	delete(m.workflows, uuid)
	rw.wi.State = workflowpb.WorkflowState_Done
	if err != nil {
		rw.wi.Error = err.Error()
	}
	rw.wi.EndTime = time.Now().Unix()
	if err := m.ts.SaveWorkflow(m.ctx, rw.wi); err != nil {
		log.Errorf("Could not save workflow %v after completion: %v", rw.wi, err)
	}
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

// Wait waits for the provided workflow to end.
func (m *Manager) Wait(ctx context.Context, uuid string) error {
	// Find the workflow.
	rw, err := m.getRunningWorkflow(uuid)
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

// getRunningWorkflow returns a runningWorkflow by uuid.
func (m *Manager) getRunningWorkflow(uuid string) (*runningWorkflow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rw, ok := m.workflows[uuid]
	if !ok {
		return nil, fmt.Errorf("no running workflow with uuid %v", uuid)
	}
	return rw, nil
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
