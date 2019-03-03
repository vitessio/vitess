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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/wrangler"
)

// BlockWorker will block infinitely until its context is canceled.
type BlockWorker struct {
	StatusWorker

	// We use the Wrangler's logger to print the message.
	wr *wrangler.Wrangler
}

// NewBlockWorker returns a new BlockWorker object.
func NewBlockWorker(wr *wrangler.Wrangler) (Worker, error) {
	return &BlockWorker{
		StatusWorker: NewStatusWorker(),
		wr:           wr,
	}, nil
}

// StatusAsHTML implements the Worker interface.
func (bw *BlockWorker) StatusAsHTML() template.HTML {
	state := bw.State()

	result := "<b>Block Command</b> (blocking infinitely until context is canceled)</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDebugRunning:
		result += "<b>Running (blocking)</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success (unblocked)</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface.
func (bw *BlockWorker) StatusAsText() string {
	state := bw.State()

	result := "Block Command\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateDebugRunning:
		result += "Running (blocking)\n"
	case WorkerStateDone:
		result += "Success (unblocked)\n"
	}
	return result
}

// Run implements the Worker interface.
func (bw *BlockWorker) Run(ctx context.Context) error {
	resetVars()
	err := bw.run(ctx)

	bw.SetState(WorkerStateCleanUp)
	if err != nil {
		bw.SetState(WorkerStateError)
		return err
	}
	bw.SetState(WorkerStateDone)
	return nil
}

func (bw *BlockWorker) run(ctx context.Context) error {
	// We reuse the Copy state to reflect that the blocking is in progress.
	bw.SetState(WorkerStateDebugRunning)
	bw.wr.Logger().Printf("Block command was called and will block infinitely until the RPC context is canceled.\n")
	select {
	case <-ctx.Done():
	}
	bw.wr.Logger().Printf("Block command finished because the context is done: '%v'.\n", ctx.Err())
	bw.SetState(WorkerStateDone)

	return vterrors.New(vtrpc.Code_CANCELED, "command 'Block' was canceled")
}
