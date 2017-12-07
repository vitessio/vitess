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

// Package topovalidator contains a workflow that validates the
// topology data. It is meant to detect and propose fixes for topology
// problems. Ideally, the topology should always be
// consistent. However, with tasks dying or network problems, there is
// a risk some bad data is left in the topology. Most data problems
// should be self-healing, so it is important to only add new
// validations steps only for corner cases that would be too costly or
// time-consuming to address in the first place.
package topovalidator

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	topoValidatorFactoryName = "topo_validator"
)

var (
	validators = make(map[string]Validator)
)

// Register needs to be called to register the topovalidator factory.
func Register() {
	workflow.Register(topoValidatorFactoryName, &WorkflowFactory{})
}

// Validator is an individual process that validates an aspect of the topology.
// Typically, it looks for something wrong, and if it finds anything, it registers a Fixer in the workflow.
type Validator interface {
	// Audit is called by the Workflow. It can add Fixer objects to the Workflow.
	Audit(ctx context.Context, ts *topo.Server, w *Workflow) error
}

// Fixer is the interface to implement to register a job capable of
// fixing something in the topology. It is given the action that the
// user chose.
type Fixer interface {
	Action(ctx context.Context, name string) error
}

// RegisterValidator adds a Validator to our list. Typically called at
// init() time.
func RegisterValidator(name string, v Validator) {
	if _, ok := validators[name]; ok {
		log.Fatalf("Registering duplicate Validator with name %v", name)
	}
	validators[name] = v
}

// Workflow is the workflow that runs the validation.
// It implements workflow.Workflow.
type Workflow struct {
	// fixers is the array of possible fixes.
	fixers []*workflowFixer

	// runCount is the number of validators we already ran.
	runCount int

	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger

	// rootUINode is the root node representing the workflow in the UI.
	rootUINode *workflow.Node

	// wg is a wait group that will be used to wait for all fixers to be done.
	wg sync.WaitGroup
}

// workflowFixer contains all the information about a fixer.
type workflowFixer struct {
	name    string
	message string
	fixer   Fixer
	actions []string
	node    *workflow.Node
	wg      *sync.WaitGroup
}

// AddFixer adds a Fixer to the Workflow. It will end up displaying a
// sub-task for the user to act on. When the user presses one action,
// the Fixer.Action() callback will be called.
func (w *Workflow) AddFixer(name, message string, fixer Fixer, actions []string) {
	w.fixers = append(w.fixers, &workflowFixer{
		name:    name,
		message: message,
		fixer:   fixer,
		actions: actions,
		wg:      &w.wg,
	})
}

// Run is part of the workflow.Workflow interface.
func (w *Workflow) Run(ctx context.Context, manager *workflow.Manager, wi *topo.WorkflowInfo) error {
	w.uiUpdate()
	w.rootUINode.Display = workflow.NodeDisplayDeterminate
	w.rootUINode.BroadcastChanges(false /* updateChildren */)

	// Run all the validators. They may add fixers.
	for name, v := range validators {
		w.logger.Infof("Running validator: %v", name)
		w.uiUpdate()
		w.rootUINode.BroadcastChanges(false /* updateChildren */)
		err := v.Audit(ctx, manager.TopoServer(), w)
		if err != nil {
			w.logger.Errorf("Validator %v failed: %v", name, err)
		} else {
			w.logger.Infof("Validator %v successfully finished", name)
		}
		w.runCount++
	}

	// Now for each Fixer, add a sub node.
	if len(w.fixers) == 0 {
		w.logger.Infof("No problem found")
	}
	for i, f := range w.fixers {
		w.wg.Add(1)
		f.node = workflow.NewNode()
		w.rootUINode.Children = append(w.rootUINode.Children, f.node)
		f.node.PathName = fmt.Sprintf("%v", i)
		f.node.Name = f.name
		f.node.Message = f.message
		f.node.Display = workflow.NodeDisplayIndeterminate
		for _, action := range f.actions {
			f.node.Actions = append(f.node.Actions, &workflow.Action{
				Name:  action,
				State: workflow.ActionStateEnabled,
				Style: workflow.ActionStyleNormal,
			})
		}
		f.node.Listener = f
	}
	w.uiUpdate()
	w.rootUINode.BroadcastChanges(true /* updateChildren */)

	// And wait for the workflow to be done.
	fixersChan := make(chan struct{})
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(fixersChan)
	}(&w.wg)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-fixersChan:
		return nil
	}
}

// uiUpdate updates the computed parts of the Node, based on the
// current state.
func (w *Workflow) uiUpdate() {
	c := len(validators)
	w.rootUINode.Progress = 100 * w.runCount / c
	w.rootUINode.ProgressMessage = fmt.Sprintf("%v/%v", w.runCount, c)
	w.rootUINode.Log = w.logger.String()
}

// Action is part of the workflow.ActionListener interface.
func (f *workflowFixer) Action(ctx context.Context, path, name string) error {
	if len(f.node.Actions) == 0 {
		// Action was already run.
		return nil
	}
	err := f.fixer.Action(ctx, name)
	if err != nil {
		f.node.Log = fmt.Sprintf("action %v failed: %v", name, err)
	} else {
		f.node.Log = fmt.Sprintf("action %v successful", name)
	}
	f.node.Actions = f.node.Actions[:0]
	f.node.State = workflowpb.WorkflowState_Done
	f.node.Message = "Addressed(" + name + "): " + f.node.Message
	f.node.Display = workflow.NodeDisplayNone
	f.node.BroadcastChanges(false /* updateChildren */)
	f.wg.Done()
	return nil
}

// WorkflowFactory is the factory to register the topo validator
// workflow.
type WorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (f *WorkflowFactory) Init(_ *workflow.Manager, w *workflowpb.Workflow, args []string) error {
	// No parameters to parse.
	if len(args) > 0 {
		return fmt.Errorf("%v doesn't take any parameter", topoValidatorFactoryName)
	}
	w.Name = "Topology Validator"
	return nil
}

// Instantiate is part of the workflow.Factory interface.
func (f *WorkflowFactory) Instantiate(_ *workflow.Manager, w *workflowpb.Workflow, rootNode *workflow.Node) (workflow.Workflow, error) {
	rootNode.Message = "Validates the Topology and proposes fixes for known issues."

	return &Workflow{
		logger:     logutil.NewMemoryLogger(),
		rootUINode: rootNode,
	}, nil
}
