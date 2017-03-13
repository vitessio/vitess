// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package wrangler contains the Wrangler object to manage complex
// topology actions.
package wrangler

import (
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
)

var (
	// DefaultActionTimeout is a good default for interactive
	// remote actions. We usually take a lock then do an action,
	// so basing this to be greater than DefaultLockTimeout is good.
	// Use this as the default value for Context that need a deadline.
	DefaultActionTimeout = topo.DefaultLockTimeout * 4
)

// Wrangler manages complex actions on the topology, like reparents,
// backups, resharding, ...
//
// Multiple go routines can use the same Wrangler at the same time,
// provided they want to share the same logger / topo server / lock timeout.
type Wrangler struct {
	logger logutil.Logger
	ts     topo.Server
	tmc    tmclient.TabletManagerClient
}

// New creates a new Wrangler object.
func New(logger logutil.Logger, ts topo.Server, tmc tmclient.TabletManagerClient) *Wrangler {
	return &Wrangler{
		logger: logger,
		ts:     ts,
		tmc:    tmc,
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
