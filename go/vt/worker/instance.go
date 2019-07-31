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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
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

	topoServer             *topo.Server
	cell                   string
	commandDisplayInterval time.Duration
}

// NewInstance creates a new Instance.
func NewInstance(ts *topo.Server, cell string, commandDisplayInterval time.Duration) *Instance {
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
		// This way any automation can retry to issue 'Reset' and then the original
		// command. We can end up in this situation when the automation job was
		// restarted and therefore the previously running vtworker command was
		// canceled.
		// TODO(mberlin): This can be simplified when we move to a model where
		// vtworker runs commands independent of an RPC and the automation polls for
		// the current status, based on an assigned unique id, instead.
		const gracePeriod = 1 * time.Minute
		sinceLastStop := time.Since(wi.lastRunStopTime)
		if sinceLastStop <= gracePeriod {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
				"A worker job was recently stopped (%f seconds ago): If you run commands manually, run the 'Reset' command to clear the vtworker state. Job: %v",
				sinceLastStop.Seconds(),
				wi.currentWorker)
		}

		// We return FAILED_PRECONDITION to signal that a manual resolution is required.
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"The worker job was stopped %.1f minutes ago, but not reset. Run the 'Reset' command to clear it manually. Job: %v",
			time.Since(wi.lastRunStopTime).Minutes(),
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
		span, ctx := trace.NewSpan(wi.currentContext, "work")
		span.Annotate("extra", wrk.State().String())
		defer span.Finish()
		var err error

		// Catch all panics and always save the execution state at the end.
		defer func() {
			// The recovery code is a copy of servenv.HandlePanic().
			if x := recover(); x != nil {
				log.Errorf("uncaught vtworker panic: %v\n%s", x, tb.Stack(4))
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "uncaught vtworker panic: %v", x)
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
		err = wrk.Run(ctx)

		// If the context was canceled, include the respective error c ode.
		select {
		case <-ctx.Done():
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

	return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "worker still executing")
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
