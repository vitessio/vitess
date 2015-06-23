// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

// WorkerInstance encapsulate the execution state of vtworker.
type WorkerInstance struct {
	// global wrangler object we'll use
	Wr *wrangler.Wrangler

	// mutex is protecting all the following variables
	// 3 states here:
	// - no job ever ran (or reset was run): currentWorker is nil,
	// currentContext/currentCancelFunc is nil, lastRunError is nil
	// - one worker running: currentWorker is set,
	//   currentContext/currentCancelFunc is set, lastRunError is nil
	// - (at least) one worker already ran, none is running atm:
	//   currentWorker is set, currentContext is nil, lastRunError
	//   has the error returned by the worker.
	currentWorkerMutex  sync.Mutex
	currentWorker       Worker
	currentMemoryLogger *logutil.MemoryLogger
	currentContext      context.Context
	currentCancelFunc   context.CancelFunc
	lastRunError        error

	TopoServer             topo.Server
	cell                   string
	LockTimeout            time.Duration
	commandDisplayInterval time.Duration
}

// NewWorkerInstance creates a new WorkerInstance.
func NewWorkerInstance(ts topo.Server, cell string, lockTimeout, commandDisplayInterval time.Duration) *WorkerInstance {
	return &WorkerInstance{TopoServer: ts, cell: cell, LockTimeout: lockTimeout, commandDisplayInterval: commandDisplayInterval}
}

// setAndStartWorker will set the current worker.
// We always log to both memory logger (for display on the web) and
// console logger (for records / display of command line worker).
func (wi *WorkerInstance) setAndStartWorker(wrk Worker) (chan struct{}, error) {
	wi.currentWorkerMutex.Lock()
	defer wi.currentWorkerMutex.Unlock()
	if wi.currentWorker != nil {
		return nil, fmt.Errorf("A worker is already in progress: %v", wi.currentWorker)
	}

	wi.currentWorker = wrk
	wi.currentMemoryLogger = logutil.NewMemoryLogger()
	wi.currentContext, wi.currentCancelFunc = context.WithCancel(context.Background())
	wi.lastRunError = nil
	done := make(chan struct{})
	wi.Wr.SetLogger(logutil.NewTeeLogger(wi.currentMemoryLogger, logutil.NewConsoleLogger()))

	// one go function runs the worker, changes state when done
	go func() {
		// run will take a long time
		log.Infof("Starting worker...")
		err := wrk.Run(wi.currentContext)

		// it's done, let's save our state
		wi.currentWorkerMutex.Lock()
		wi.currentContext = nil
		wi.currentCancelFunc = nil
		wi.lastRunError = err
		wi.currentWorkerMutex.Unlock()
		close(done)
	}()

	return done, nil
}

// InstallSignalHandlers installs signal handler which exit vtworker gracefully.
func (wi *WorkerInstance) InstallSignalHandlers() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigChan
		// we got a signal, notify our modules
		wi.currentWorkerMutex.Lock()
		defer wi.currentWorkerMutex.Unlock()
		if wi.currentCancelFunc != nil {
			wi.currentCancelFunc()
		} else {
			fmt.Printf("Shutting down idle worker after receiving signal: %v", s)
			os.Exit(0)
		}
	}()
}
