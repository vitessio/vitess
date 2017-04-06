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

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/logutil"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"
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
	lastRunStopTime     time.Time

	topoServer             topo.Server
	cell                   string
	commandDisplayInterval time.Duration
}

// NewInstance creates a new Instance.
func NewInstance(ts topo.Server, cell string, commandDisplayInterval time.Duration) *Instance {
	wi := &Instance{topoServer: ts, cell: cell, commandDisplayInterval: commandDisplayInterval}
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
func (wi *Instance) setAndStartWorker(ctx context.Context, wrk Worker, wr *wrangler.Wrangler) (chan struct{}, error) {
	wi.currentWorkerMutex.Lock()
	defer wi.currentWorkerMutex.Unlock()

	if wi.currentContext != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "A worker job is already in progress: %v", wi.currentWorker.StatusAsText())
	}

	if wi.currentWorker != nil {
		// During the grace period, we answer with a retryable error.
		const gracePeriod = 1 * time.Minute
		gracePeriodEnd := time.Now().Add(gracePeriod)
		if wi.lastRunStopTime.Before(gracePeriodEnd) {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "A worker job was recently stopped (%f seconds ago): %v", time.Now().Sub(wi.lastRunStopTime).Seconds(), wi.currentWorker)
		}

		// QUERY_NOT_SERVED = FailedPrecondition => manual resolution required.
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"The worker job was stopped %.1f minutes ago, but not reset. You have to reset it manually. Job: %v",
			time.Now().Sub(wi.lastRunStopTime).Minutes(),
			wi.currentWorker)
	}

	wi.currentWorker = wrk
	wi.currentMemoryLogger = logutil.NewMemoryLogger()
	wi.currentContext, wi.currentCancelFunc = context.WithCancel(ctx)
	wi.lastRunError = nil
	wi.lastRunStopTime = time.Unix(0, 0)
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
				log.Errorf("uncaught vtworker panic: %v\n%s", x, tb.Stack(4))
				err = fmt.Errorf("uncaught vtworker panic: %v", x)
			}

			wi.currentWorkerMutex.Lock()
			wi.currentContext = nil
			wi.currentCancelFunc = nil
			wi.lastRunError = err
			wi.lastRunStopTime = time.Now()
			wi.currentWorkerMutex.Unlock()
			close(done)
		}()

		// run will take a long time
		err = wrk.Run(wi.currentContext)

		// If the context was canceled, include the respective error code.
		select {
		case <-wi.currentContext.Done():
			// Context is done i.e. probably canceled.
			if wi.currentContext.Err() == context.Canceled {
				err = vterrors.Errorf(vtrpcpb.Code_CANCELED, "vtworker command was canceled: %v", err)
			}
		default:
		}
	}()

	return done, nil
}

// InstallSignalHandlers installs signal handler which exit vtworker gracefully.
func (wi *Instance) InstallSignalHandlers() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for s := range sigChan {
			// We got a signal, notify our modules.
			// Use an extra function to properly unlock using defer.
			func() {
				wi.currentWorkerMutex.Lock()
				defer wi.currentWorkerMutex.Unlock()
				if wi.currentCancelFunc != nil {
					log.Infof("Trying to cancel current worker after receiving signal: %v", s)
					wi.currentCancelFunc()
				} else {
					log.Infof("Shutting down idle worker after receiving signal: %v", s)
					os.Exit(0)
				}
			}()
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
