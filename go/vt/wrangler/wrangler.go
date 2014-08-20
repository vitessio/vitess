// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// wrangler contains the Wrangler object to manage complex topology actions.
package wrangler

import (
	"flag"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/initiator"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	// DefaultActionTimeout is a good default for interactive
	// remote actions. We usually take a lock then do an action,
	// so basing this to be greater than DefaultLockTimeout is good.
	DefaultActionTimeout = actionnode.DefaultLockTimeout * 4
)

var tabletManagerProtocol = flag.String("tablet_manager_protocol", "bson", "the protocol to use to talk to vttablet")

// Wrangler manages complex actions on the topology, like reparents,
// snapshots, restores, ...
type Wrangler struct {
	logger      logutil.Logger
	ts          topo.Server
	ai          *initiator.ActionInitiator
	deadline    time.Time
	lockTimeout time.Duration

	// Configuration parameters, mostly for tests.

	// UseRPCs makes the wrangler use RPCs to trigger short live
	// remote actions. It is faster in production, as we don't
	// fork a vtaction. However, unit tests don't support it.
	UseRPCs bool
}

// New creates a new Wrangler object.
//
// actionTimeout: how long should we wait for an action to complete?
// - if using wrangler for just one action, this is set properly
//   upon wrangler creation.
// - if re-using wrangler multiple times, call ResetActionTimeout before
//   every action.
//
// lockTimeout: how long should we wait for the initial lock to start
// a complex action?  This is distinct from actionTimeout because most
// of the time, we want to immediately know that our action will
// fail. However, automated action will need some time to arbitrate
// the locks.
func New(logger logutil.Logger, ts topo.Server, actionTimeout, lockTimeout time.Duration) *Wrangler {
	return &Wrangler{logger, ts, initiator.NewActionInitiator(ts, *tabletManagerProtocol), time.Now().Add(actionTimeout), lockTimeout, true}
}

// ActionTimeout returns the timeout to use so the action finishes before
// the deadline.
func (wr *Wrangler) ActionTimeout() time.Duration {
	return wr.deadline.Sub(time.Now())
}

// TopoServer returns the topo.Server this wrangler is using.
func (wr *Wrangler) TopoServer() topo.Server {
	return wr.ts
}

// ActionInitiator returns the initiator.ActionInitiator this wrangler is using.
func (wr *Wrangler) ActionInitiator() *initiator.ActionInitiator {
	return wr.ai
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
// - vtctl will not call this, as it does one action
// - vtctld will call this, as it re-uses the same wrangler for actions
func (wr *Wrangler) ResetActionTimeout(actionTimeout time.Duration) {
	wr.deadline = time.Now().Add(actionTimeout)
}

// WaitForCompletion will wait for the actionPath to complete, using the
// wrangler default action timeout.
func (wr *Wrangler) WaitForCompletion(actionPath string) error {
	return wr.ai.WaitForCompletion(actionPath, wr.ActionTimeout())
}

// WaitForCompletionReply will wait for the actionPath to complete, using the
// wrangler default action timeout, and return the result
func (wr *Wrangler) WaitForCompletionReply(actionPath string) (interface{}, error) {
	return wr.ai.WaitForCompletionReply(actionPath, wr.ActionTimeout())
}

// signal handling
var interrupted = make(chan struct{})

// SignalInterrupt needs to be called when a signal interrupts the current
// process.
func SignalInterrupt() {
	close(interrupted)
}
