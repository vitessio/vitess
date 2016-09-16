// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"html/template"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/wrangler"
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

	return errors.New("command 'Block' was canceled")
}
