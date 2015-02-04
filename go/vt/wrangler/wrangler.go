// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package wrangler contains the Wrangler object to manage complex
// topology actions.
package wrangler

import (
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	// DefaultActionTimeout is a good default for interactive
	// remote actions. We usually take a lock then do an action,
	// so basing this to be greater than DefaultLockTimeout is good.
	// Use this as the default value for Context that need a deadline.
	DefaultActionTimeout = actionnode.DefaultLockTimeout * 4
)

// Wrangler manages complex actions on the topology, like reparents,
// snapshots, restores, ...
//
// FIXME(alainjobart) take the context out of this structure.
// We want the context to come from the outside on every call.
//
// Multiple go routines can use the same Wrangler at the same time,
// provided they want to share the same logger / topo server / lock timeout.
type Wrangler struct {
	logger      logutil.Logger
	ts          topo.Server
	tmc         tmclient.TabletManagerClient
	lockTimeout time.Duration
}

// New creates a new Wrangler object.
//
// lockTimeout: how long should we wait for the initial lock to start
// a complex action?  This is distinct from the context timeout because most
// of the time, we want to immediately know that our action will
// fail. However, automated action will need some time to arbitrate
// the locks.
func New(logger logutil.Logger, ts topo.Server, tmc tmclient.TabletManagerClient, lockTimeout time.Duration) *Wrangler {
	return &Wrangler{
		logger:      logger,
		ts:          ts,
		tmc:         tmc,
		lockTimeout: lockTimeout,
	}
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
