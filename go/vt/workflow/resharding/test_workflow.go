package resharding

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	testWorkflowFactoryName           = "test_workflow"
	phaseSimple             PhaseType = "simple"
	errMessage                        = "fake error for testing retry"
)

func createTestTaskID(phase PhaseType, count int) string {
	return fmt.Sprintf("%s/%v", phase, count)
}

func init() {
	workflow.Register(testWorkflowFactoryName, &TestWorkflowFactory{})
}

// TestWorkflow is created to simplfy the unit test of ParallelRunner.
type TestWorkflow struct {
	ctx        context.Context
	manager    *workflow.Manager
	topoServer topo.Server
	wi         *topo.WorkflowInfo
	logger     *logutil.MemoryLogger

	retryMu sync.Mutex
	// retryFlags stores the retry flag for all the tasks.
	retryFlags map[string]bool

	rootUINode *workflow.Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter
}

// Run implements the worklfow.Workflow interface.
func (tw *TestWorkflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	tw.ctx = ctx
	tw.topoServer = manager.TopoServer()
	tw.manager = manager
	tw.wi = wi
	tw.checkpointWriter = NewCheckpointWriter(tw.topoServer, tw.checkpoint, tw.wi)
	tw.rootUINode.Display = workflow.NodeDisplayDeterminate
	tw.rootUINode.BroadcastChanges(true /* updateChildren */)

	simpleTasks := tw.getTasks(phaseSimple)
	simpleRunner := NewParallelRunner(tw.ctx, tw.rootUINode, tw.checkpointWriter, simpleTasks, tw.runSimple, Parallel)
	simpleRunner.reportTaskStatus = true
	if err := simpleRunner.Run(); err != nil {
		return err
	}

	log.Infof("Horizontal resharding is finished successfully.")
	return nil
}

func (tw *TestWorkflow) getTasks(phaseName PhaseType) []*workflowpb.Task {
	count, err := strconv.Atoi(tw.checkpoint.Settings["count"])
	if err != nil {
		log.Info("converting count in checkpoint.Settings to int fails: %v \n", tw.checkpoint.Settings["count"])
		return nil
	}
	var tasks []*workflowpb.Task
	for i := 0; i < count; i++ {
		taskID := createTestTaskID(phaseName, i)
		tasks = append(tasks, tw.checkpoint.Tasks[taskID])
	}
	return tasks
}

func (tw *TestWorkflow) runSimple(ctx context.Context, t *workflowpb.Task) error {
	log.Info("The number passed to me is %v \n", t.Attributes["number"])

	tw.retryMu.Lock()
	defer tw.retryMu.Unlock()
	if tw.retryFlags[t.Id] {
		log.Info("I will fail at this time since retry flag is true.")
		tw.retryFlags[t.Id] = false
		return errors.New(errMessage)
	}
	return nil
}

// TestWorkflowFactory is the factory to create a test workflow.
type TestWorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (*TestWorkflowFactory) Init(w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(testWorkflowFactoryName, flag.ContinueOnError)
	retryFlag := subFlags.Bool("retry", false, "The retry flag should be true if the retry action need to be tested")
	count := subFlags.Int("count", 0, "The number of simple tasks")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	// Initialize the checkpoint.
	taskMap := make(map[string]*workflowpb.Task)
	for i := 0; i < *count; i++ {
		taskID := createTestTaskID(phaseSimple, i)
		taskMap[taskID] = &workflowpb.Task{
			Id:         taskID,
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: map[string]string{"number": fmt.Sprintf("%v", i)},
		}
	}
	checkpoint := &workflowpb.WorkflowCheckpoint{
		CodeVersion: 0,
		Tasks:       taskMap,
		Settings:    map[string]string{"count": fmt.Sprintf("%v", *count), "retry": fmt.Sprintf("%v", *retryFlag)},
	}
	var err error
	w.Data, err = proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part the workflow.Factory interface.
func (*TestWorkflowFactory) Instantiate(w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}
	// Get the retry flags for all tasks from the checkpoint.
	retry, err := strconv.ParseBool(checkpoint.Settings["retry"])
	if err != nil {
		log.Errorf("converting retry in checkpoint.Settings to bool fails: %v \n", checkpoint.Settings["retry"])
		return nil, err
	}
	retryFlags := make(map[string]bool)
	for _, task := range checkpoint.Tasks {
		retryFlags[task.Id] = retry
	}

	tw := &TestWorkflow{
		checkpoint: checkpoint,
		rootUINode: rootNode,
		logger:     logutil.NewMemoryLogger(),
		retryFlags: retryFlags,
	}

	count, err := strconv.Atoi(checkpoint.Settings["count"])
	if err != nil {
		log.Errorf("converting count in checkpoint.Settings to int fails: %v \n", checkpoint.Settings["count"])
		return nil, err
	}

	phaseNode := &workflow.Node{
		Name:     string(phaseSimple),
		PathName: string(phaseSimple),
	}
	tw.rootUINode.Children = append(tw.rootUINode.Children, phaseNode)

	for i := 0; i < count; i++ {
		taskName := fmt.Sprintf("%v", i)
		taskUINode := &workflow.Node{
			Name:     taskName,
			PathName: taskName,
		}
		phaseNode.Children = append(phaseNode.Children, taskUINode)
	}
	return tw, nil
}
