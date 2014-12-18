// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package wrangler contains the Wrangler object to manage complex
// topology actions.
package wrangler

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	// DefaultActionTimeout is a good default for interactive
	// remote actions. We usually take a lock then do an action,
	// so basing this to be greater than DefaultLockTimeout is good.
	DefaultActionTimeout = actionnode.DefaultLockTimeout * 4
)

// Wrangler manages complex actions on the topology, like reparents,
// snapshots, restores, ...
// It is not a thread safe structure. Two go routines cannot usually
// call the same Wrangler object methods at the same time (they
// probably would not have the same logger, and definitely not the
// same context).
type Wrangler struct {
	logger      logutil.Logger
	ts          topo.Server
	tmc         tmclient.TabletManagerClient
	lockTimeout time.Duration

	// the following fields are protected by the mutex
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	deadline time.Time
}

// New creates a new Wrangler object.
//
// actionTimeout: how long should we wait for an action to complete?
// - if using wrangler for just one action, this is set properly
//   upon wrangler creation.
// - if re-using wrangler multiple times, call ResetActionTimeout before
//   every action. Do not use this too much, just for corner cases.
//   It is just much easier to create a new Wrangler object per action.
//
// lockTimeout: how long should we wait for the initial lock to start
// a complex action?  This is distinct from actionTimeout because most
// of the time, we want to immediately know that our action will
// fail. However, automated action will need some time to arbitrate
// the locks.
func New(logger logutil.Logger, ts topo.Server, actionTimeout, lockTimeout time.Duration) *Wrangler {
	ctx, cancel := context.WithTimeout(context.Background(), actionTimeout)
	return &Wrangler{
		logger:      logger,
		ts:          ts,
		tmc:         tmclient.NewTabletManagerClient(),
		ctx:         ctx,
		cancel:      cancel,
		deadline:    time.Now().Add(actionTimeout),
		lockTimeout: lockTimeout,
	}
}

// ActionTimeout returns the timeout to use so the action finishes before
// the deadline.
func (wr *Wrangler) ActionTimeout() time.Duration {
	return wr.deadline.Sub(time.Now())
}

// Context returns the context associated with this Wrangler.
// It is replaced if ResetActionTimeout is called on the Wrangler.
func (wr *Wrangler) Context() context.Context {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	return wr.ctx
}

// Cancel calls the CancelFunc on our Context and therefore interrupts the call.
func (wr *Wrangler) Cancel() {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	wr.cancel()
}

// TopoServer returns the topo.Server this wrangler is using.
func (wr *Wrangler) TopoServer() topo.Server {
	return wr.ts
}

// TabletManagerClient returns the tmclient.TabletManagerClient this
// wrangler is using.
func (wr *Wrangler) TabletManagerClient() tmclient.TabletManagerClient {
	return wr.tmc
}

// SetLogger can be used to change the current logger. Not synchronized,
// no calls to this wrangler should be in progress.
func (wr *Wrangler) SetLogger(logger logutil.Logger) {
	wr.logger = logger
}

// Logger returns the logger associated with this wrangler.
func (wr *Wrangler) Logger() logutil.Logger {
	return wr.logger
}

// ResetActionTimeout should be used before every action on a wrangler
// object that is going to be re-used:
// - vtctl will not call this, as it does one action.
// - vtctld will not call this, as it creates a new Wrangler every time.
// However, some actions may need to do a cleanup phase where the
// original Context may have expired or been cancelled, but still do
// the action.  Wrangler cleaner module is one of these, or the vt
// worker in some corner cases,
func (wr *Wrangler) ResetActionTimeout(actionTimeout time.Duration) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	wr.ctx, wr.cancel = context.WithTimeout(context.Background(), actionTimeout)
	wr.deadline = time.Now().Add(actionTimeout)
}
