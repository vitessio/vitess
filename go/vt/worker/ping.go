/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/wrangler"
)

// PingWorker will log a message with level CONSOLE.
type PingWorker struct {
	StatusWorker

	// We use the Wrangler's logger to print the message.
	wr      *wrangler.Wrangler
	message string
}

// NewPingWorker returns a new PingWorker object.
func NewPingWorker(wr *wrangler.Wrangler, message string) (Worker, error) {
	return &PingWorker{
		StatusWorker: NewStatusWorker(),
		wr:           wr,
		message:      message,
	}, nil
}

// StatusAsHTML implements the Worker interface
func (pw *PingWorker) StatusAsHTML() template.HTML {
	state := pw.State()

	result := "<b>Ping Command with message:</b> '" + pw.message + "'</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDebugRunning:
		result += "<b>Running</b>:</br>\n"
		result += "Logging message: '" + pw.message + "'</br>\n"
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		result += "Logged message: '" + pw.message + "'</br>\n"
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface.
func (pw *PingWorker) StatusAsText() string {
	state := pw.State()

	result := "Ping Command with message: '" + pw.message + "'\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateDebugRunning:
		result += "Logging message: '" + pw.message + "'\n"
	case WorkerStateDone:
		result += "Logged message: '" + pw.message + "'\n"
	}
	return result
}

// Run implements the Worker interface.
func (pw *PingWorker) Run(ctx context.Context) error {
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

func (pw *PingWorker) run(ctx context.Context) error {
	// We reuse the Copy state to reflect that the logging is in progress.
	pw.SetState(WorkerStateDebugRunning)
	pw.wr.Logger().Printf("Ping command was called with message: '%v'.\n", pw.message)
	pw.SetState(WorkerStateDone)

	return nil
}
