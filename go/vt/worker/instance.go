// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

// Instance encapsulate the execution state of vtworker.
type Instance struct {
	// Default wrangler for all operations.
	// Users can specify their own in RunCommand() e.g. the gRPC server does this.
	wr *wrangler.Wrangler

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

	// backgroundContext is context.Background() from main() which has to be plumbed through.
	backgroundContext      context.Context
	topoServer             topo.Server
	cell                   string
	commandDisplayInterval time.Duration
}

// NewInstance creates a new Instance.
func NewInstance(ctx context.Context, ts topo.Server, cell string, commandDisplayInterval time.Duration) *Instance {
	wi := &Instance{backgroundContext: ctx, topoServer: ts, cell: cell, commandDisplayInterval: commandDisplayInterval}
	// Note: setAndStartWorker() also adds a MemoryLogger for the webserver.
	wi.wr = wi.CreateWrangler(logutil.NewConsoleLogger())
	return wi
}

// CreateWrangler creates a new wrangler using the instance specific configuration.
func (wi *Instance) CreateWrangler(logger logutil.Logger) *wrangler.Wrangler {
	return wrangler.New(logger, wi.topoServer, tmclient.NewTabletManagerClient())
}

// setAndStartWorker will set the current worker.
// We always log to both memory logger (for display on the web) and
// console logger (for records / display of command line worker).
func (wi *Instance) setAndStartWorker(wrk Worker, wr *wrangler.Wrangler) (chan struct{}, error) {
	wi.currentWorkerMutex.Lock()
	defer wi.currentWorkerMutex.Unlock()
	if wi.currentWorker != nil {
		return nil, fmt.Errorf("A worker is already in progress: %v", wi.currentWorker)
	}

	wi.currentWorker = wrk
	wi.currentMemoryLogger = logutil.NewMemoryLogger()
	wi.currentContext, wi.currentCancelFunc = context.WithCancel(wi.backgroundContext)
	wi.lastRunError = nil
	done := make(chan struct{})
	wranglerLogger := wr.Logger()
	if wr == wi.wr {
		// If it's the default wrangler, do not reuse its logger because it may have been set before.
		// Resuing it would result into an endless recursion.
		wranglerLogger = logutil.NewConsoleLogger()
	}
	wr.SetLogger(logutil.NewTeeLogger(wi.currentMemoryLogger, wranglerLogger))

	// one go function runs the worker, changes state when done
	go func() {
		log.Infof("Starting worker...")
		var err error

		// Catch all panics and always save the execution state at the end.
		defer func() {
			// The recovery code is a copy of servenv.HandlePanic().
			if x := recover(); x != nil {
				err = fmt.Errorf("uncaught %v panic: %v", "vtworker", x)
			}

			wi.currentWorkerMutex.Lock()
			wi.currentContext = nil
			wi.currentCancelFunc = nil
			wi.lastRunError = err
			wi.currentWorkerMutex.Unlock()
			close(done)
		}()

		// run will take a long time
		err = wrk.Run(wi.currentContext)
	}()

	return done, nil
}

// InstallSignalHandlers installs signal handler which exit vtworker gracefully.
func (wi *Instance) InstallSignalHandlers() {
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
			log.Infof("Shutting down idle worker after receiving signal: %v", s)
			os.Exit(0)
		}
	}()
}

// Reset resets the state of a finished worker. It returns an error if the worker is still running.
func (wi *Instance) Reset() error {
	wi.currentWorkerMutex.Lock()
	defer wi.currentWorkerMutex.Unlock()

	if wi.currentWorker == nil {
		return nil
	}

	// check the worker is really done
	if wi.currentContext == nil {
		wi.currentWorker = nil
		wi.currentMemoryLogger = nil
		return nil
	}

	return errors.New("worker still executing")
}

// Cancel calls the cancel function of the current vtworker job.
// It returns true, if a job was running. False otherwise.
// NOTE: Cancel won't reset the state as well. Use Reset() to do so.
func (wi *Instance) Cancel() bool {
	wi.currentWorkerMutex.Lock()

	if wi.currentWorker == nil || wi.currentCancelFunc == nil {
		wi.currentWorkerMutex.Unlock()
		return false
	}

	cancel := wi.currentCancelFunc
	wi.currentWorkerMutex.Unlock()

	cancel()

	return true
}
