// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package automation contains code to execute high-level cluster operations
(e.g. resharding) as a series of low-level operations
(e.g. vtctl, shell commands, ...).
*/
package automation

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	pb "github.com/youtube/vitess/go/vt/proto/automation"
	"golang.org/x/net/context"
)

type schedulerState int32

const (
	stateNotRunning schedulerState = iota
	stateRunning
	stateShuttingDown
	stateShutdown
)

type taskCreator func(string) Task

// Scheduler executes automation tasks and maintains the execution state.
type Scheduler struct {
	idGenerator IDGenerator

	mu sync.Mutex
	// Guarded by "mu".
	registeredClusterOperations map[string]bool
	// Guarded by "mu".
	toBeScheduledClusterOperations chan *ClusterOperationInstance
	// Guarded by "mu".
	state schedulerState

	// Guarded by "taskCreatorMu". May be overriden by testing code.
	taskCreator   taskCreator
	taskCreatorMu sync.Mutex

	pendingOpsWg *sync.WaitGroup

	muOpList sync.Mutex
	// Guarded by "muOpList".
	activeClusterOperations map[string]*ClusterOperationInstance
	// Guarded by "muOpList".
	finishedClusterOperations map[string]*ClusterOperationInstance
}

// NewScheduler creates a new instance.
func NewScheduler() (*Scheduler, error) {
	defaultClusterOperations := map[string]bool{
		"HorizontalReshardingTask": true,
	}

	s := &Scheduler{
		registeredClusterOperations:    defaultClusterOperations,
		idGenerator:                    IDGenerator{},
		toBeScheduledClusterOperations: make(chan *ClusterOperationInstance, 10),
		state:                     stateNotRunning,
		taskCreator:               defaultTaskCreator,
		pendingOpsWg:              &sync.WaitGroup{},
		activeClusterOperations:   make(map[string]*ClusterOperationInstance),
		finishedClusterOperations: make(map[string]*ClusterOperationInstance),
	}

	return s, nil
}

func (s *Scheduler) registerClusterOperation(clusterOperationName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.registeredClusterOperations[clusterOperationName] = true
}

// Run processes queued cluster operations.
func (s *Scheduler) Run() {
	s.mu.Lock()
	s.state = stateRunning
	s.mu.Unlock()

	s.startProcessRequestsLoop()
}

func (s *Scheduler) startProcessRequestsLoop() {
	// Use a WaitGroup instead of just a done channel, because we want
	// to be able to shut down the scheduler even if Run() was never executed.
	s.pendingOpsWg.Add(1)
	go s.processRequestsLoop()
}

func (s *Scheduler) processRequestsLoop() {
	defer s.pendingOpsWg.Done()

	for op := range s.toBeScheduledClusterOperations {
		s.processClusterOperation(op)
	}
	log.Infof("Stopped processing loop for ClusterOperations.")
}

func (s *Scheduler) processClusterOperation(clusterOp *ClusterOperationInstance) {
	if clusterOp.State == pb.ClusterOperationState_CLUSTER_OPERATION_DONE {
		log.Infof("ClusterOperation: %v skipping because it is already done. Details: %v", clusterOp.Id, clusterOp)
		return
	}

	log.Infof("ClusterOperation: %v running. Details: %v", clusterOp.Id, clusterOp)

	var lastTaskError string
	for i := 0; i < len(clusterOp.SerialTasks); i++ {
		taskContainer := clusterOp.SerialTasks[i]
		for _, taskProto := range taskContainer.ParallelTasks {
			if taskProto.State == pb.TaskState_DONE {
				if taskProto.Error != "" {
					log.Errorf("Task: %v (%v/%v) failed before. Aborting the ClusterOperation. Error: %v Details: %v", taskProto.Name, clusterOp.Id, taskProto.Id, taskProto.Error, taskProto)
					lastTaskError = taskProto.Error
					break
				} else {
					log.Infof("Task: %v (%v/%v) skipped because it is already done. Full Details: %v", taskProto.Name, clusterOp.Id, taskProto.Id, taskProto)
				}
			}

			task, err := s.createTaskInstance(taskProto.Name)
			if err != nil {
				log.Errorf("Task: %v (%v/%v) could not be instantiated. Error: %v Details: %v", taskProto.Name, clusterOp.Id, taskProto.Id, err, taskProto)
				MarkTaskFailed(taskProto, "", err)
				lastTaskError = err.Error()
				break
			}

			taskProto.State = pb.TaskState_RUNNING
			log.Infof("Task: %v (%v/%v) running. Details: %v", taskProto.Name, clusterOp.Id, taskProto.Id, taskProto)
			newTaskContainers, output, errRun := task.run(taskProto.Parameters)
			log.Infof("Task: %v (%v/%v) finished. newTaskContainers: %v, output: %v, error: %v", taskProto.Name, clusterOp.Id, taskProto.Id, newTaskContainers, output, errRun)

			if errRun != nil {
				MarkTaskFailed(taskProto, output, errRun)
				lastTaskError = errRun.Error()
				break
			}
			MarkTaskSucceeded(taskProto, output)

			if newTaskContainers != nil {
				// Make sure all new tasks do not miss any required parameters.
				for _, newTaskContainer := range newTaskContainers {
					for _, newTaskProto := range newTaskContainer.ParallelTasks {
						err := s.validateTaskSpecification(newTaskProto.Name, newTaskProto.Parameters)
						if err != nil {
							log.Errorf("Task: %v (%v/%v) emitted a new task which is not valid. Error: %v Details: %v", taskProto.Name, clusterOp.Id, taskProto.Id, err, newTaskProto)
							MarkTaskFailed(taskProto, output, err)
							lastTaskError = err.Error()
							break
						}
					}
				}

				if lastTaskError == "" {
					clusterOp.InsertTaskContainers(newTaskContainers, i+1)
					log.Infof("ClusterOperation: %v %d new task containers added by %v (%v/%v). Updated ClusterOperation: %v",
						clusterOp.Id, len(newTaskContainers), taskProto.Name, clusterOp.Id, taskProto.Id, clusterOp)
				}
			}
		}
	}

	clusterOp.State = pb.ClusterOperationState_CLUSTER_OPERATION_DONE
	if lastTaskError != "" {
		clusterOp.Error = lastTaskError
	}
	log.Infof("ClusterOperation: %v finished. Details: %v", clusterOp.Id, clusterOp)

	// Move operation from active to finished.
	s.muOpList.Lock()
	if s.activeClusterOperations[clusterOp.Id] != clusterOp {
		panic("Pending ClusterOperation was not recorded as active, but should have.")
	}
	delete(s.activeClusterOperations, clusterOp.Id)
	s.finishedClusterOperations[clusterOp.Id] = clusterOp
	s.muOpList.Unlock()
}

func defaultTaskCreator(taskName string) Task {
	switch taskName {
	case "HorizontalReshardingTask":
		return &HorizontalReshardingTask{}
	default:
		return nil
	}
}

func (s *Scheduler) setTaskCreator(creator taskCreator) {
	s.taskCreatorMu.Lock()
	defer s.taskCreatorMu.Unlock()

	s.taskCreator = creator
}

func (s *Scheduler) validateTaskSpecification(taskName string, parameters map[string]string) error {
	taskInstanceForParametersCheck, err := s.createTaskInstance(taskName)
	if err != nil {
		return err
	}
	errParameters := checkRequiredParameters(taskInstanceForParametersCheck, parameters)
	if errParameters != nil {
		return errParameters
	}
	return nil
}

func (s *Scheduler) createTaskInstance(taskName string) (Task, error) {
	s.taskCreatorMu.Lock()
	taskCreator := s.taskCreator
	s.taskCreatorMu.Unlock()

	task := taskCreator(taskName)
	if task == nil {
		return nil, fmt.Errorf("No implementation found for: %v", taskName)
	}
	return task, nil
}

// checkRequiredParameters returns an error if not all required parameters are provided in "parameters".
func checkRequiredParameters(task Task, parameters map[string]string) error {
	for _, requiredParameter := range task.requiredParameters() {
		if _, ok := parameters[requiredParameter]; !ok {
			return fmt.Errorf("Parameter %v is required, but not provided", requiredParameter)
		}
	}
	return nil
}

// EnqueueClusterOperation can be used to start a new cluster operation.
func (s *Scheduler) EnqueueClusterOperation(ctx context.Context, req *pb.EnqueueClusterOperationRequest) (*pb.EnqueueClusterOperationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != stateRunning {
		return nil, fmt.Errorf("Scheduler is not running. State: %v", s.state)
	}

	if s.registeredClusterOperations[req.Name] != true {
		return nil, fmt.Errorf("No ClusterOperation with name: %v is registered", req.Name)
	}

	err := s.validateTaskSpecification(req.Name, req.Parameters)
	if err != nil {
		return nil, err
	}

	clusterOpID := s.idGenerator.GetNextID()
	taskIDGenerator := IDGenerator{}
	initialTask := NewTaskContainerWithSingleTask(req.Name, req.Parameters)
	clusterOp := NewClusterOperationInstance(clusterOpID, initialTask, &taskIDGenerator)

	s.muOpList.Lock()
	s.toBeScheduledClusterOperations <- clusterOp
	s.activeClusterOperations[clusterOpID] = clusterOp
	s.muOpList.Unlock()

	return &pb.EnqueueClusterOperationResponse{
		Id: clusterOp.Id,
	}, nil
}

// findClusterOp checks for a given ClusterOperation ID if it's in the list of active or finished operations.
func (s *Scheduler) findClusterOp(id string) (*ClusterOperationInstance, error) {
	var ok bool
	var clusterOp *ClusterOperationInstance

	s.muOpList.Lock()
	defer s.muOpList.Unlock()
	clusterOp, ok = s.activeClusterOperations[id]
	if !ok {
		clusterOp, ok = s.finishedClusterOperations[id]
	}
	if !ok {
		return nil, fmt.Errorf("ClusterOperation with id: %v not found", id)
	}
	return clusterOp, nil
}

// GetClusterOperationDetails can be used to query the full details of active or finished operations.
func (s *Scheduler) GetClusterOperationDetails(ctx context.Context, req *pb.GetClusterOperationDetailsRequest) (*pb.GetClusterOperationDetailsResponse, error) {
	clusterOp, err := s.findClusterOp(req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.GetClusterOperationDetailsResponse{
		ClusterOp: &clusterOp.ClusterOperation,
	}, nil
}

// ShutdownAndWait shuts down the scheduler and waits infinitely until all pending cluster operations have finished.
func (s *Scheduler) ShutdownAndWait() {
	s.mu.Lock()
	if s.state != stateShuttingDown {
		s.state = stateShuttingDown
		close(s.toBeScheduledClusterOperations)
	}
	s.mu.Unlock()

	log.Infof("Scheduler was shut down. Waiting for pending ClusterOperations to finish.")
	s.pendingOpsWg.Wait()

	s.mu.Lock()
	s.state = stateShutdown
	s.mu.Unlock()
	log.Infof("All pending ClusterOperations finished.")
}
