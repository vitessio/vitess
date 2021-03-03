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

package worker

import (
	"html/template"

	"context"

	"vitess.io/vitess/go/vt/wrangler"
)

// PanicWorker will run panic() when executed. For internal tests only.
type PanicWorker struct {
	StatusWorker

	// We use the Wrangler's logger to print the message.
	wr *wrangler.Wrangler
}

// NewPanicWorker returns a new PanicWorker object.
func NewPanicWorker(wr *wrangler.Wrangler) (Worker, error) {
	return &PanicWorker{
		StatusWorker: NewStatusWorker(),
		wr:           wr,
	}, nil
}

// StatusAsHTML implements the Worker interface
func (pw *PanicWorker) StatusAsHTML() template.HTML {
	state := pw.State()

	result := "<b>Panic Command</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		result += "panic() should have been executed and logged by the vtworker framework.</br>\n"
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface.
func (pw *PanicWorker) StatusAsText() string {
	state := pw.State()

	result := "Panic Command\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateDone:
		result += "panic() should have been executed and logged by the vtworker framework.\n"
	}
	return result
}

// Run implements the Worker interface.
func (pw *PanicWorker) Run(ctx context.Context) error {
	resetVars()
	err := pw.run(ctx)

	pw.SetState(WorkerStateCleanUp)
	if err != nil {
		pw.SetState(WorkerStateError)
		return err
	}
	pw.SetState(WorkerStateDone)
	return nil
}

func (pw *PanicWorker) run(ctx context.Context) error {
	panic("Panic command was called. This should be caught by the vtworker framework and logged as an error.")
}
