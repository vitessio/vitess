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

// Package wrangler contains the Wrangler object to manage complex
// topology actions.
package wrangler

import (
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	// DefaultActionTimeout is a good default for interactive
	// remote actions. We usually take a lock then do an action,
	// lock actions use RemoteOperationTimeout,
	// so basing this to be greater than RemoteOperationTimeout is good.
	// Use this as the default value for Context that need a deadline.
	DefaultActionTimeout = *topo.RemoteOperationTimeout * 4
)

// Wrangler manages complex actions on the topology, like reparents,
// backups, resharding, ...
//
// Multiple go routines can use the same Wrangler at the same time,
// provided they want to share the same logger / topo server / lock timeout.
type Wrangler struct {
	logger logutil.Logger
	ts     *topo.Server
	tmc    tmclient.TabletManagerClient
}

// New creates a new Wrangler object.
func New(logger logutil.Logger, ts *topo.Server, tmc tmclient.TabletManagerClient) *Wrangler {
	return &Wrangler{
		logger: logger,
		ts:     ts,
		tmc:    tmc,
	}
}

// TopoServer returns the topo.Server this wrangler is using.
func (wr *Wrangler) TopoServer() *topo.Server {
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
