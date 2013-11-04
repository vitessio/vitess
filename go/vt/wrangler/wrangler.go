// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"time"

	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	DefaultActionTimeout = 30 * time.Second
	DefaultLockTimeout   = 30 * time.Second
)

type Wrangler struct {
	ts          topo.Server
	ai          *tm.ActionInitiator
	deadline    time.Time
	lockTimeout time.Duration

	// Configuration parameters, mostly for tests.

	// UseRPCs makes the wrangler use RPCs to trigger short live
	// remote actions. It is faster in production, as we don't
	// fork a vtaction. However, unit tests don't support it.
	UseRPCs bool
}

// actionTimeout: how long should we wait for an action to complete?
// lockTimeout: how long should we wait for the initial lock to start a complex action?
//   This is distinct from actionTimeout because most of the time, we want to immediately
//   know that out action will fail. However, automated action will need some time to
//   arbitrate the locks.
func New(ts topo.Server, actionTimeout, lockTimeout time.Duration) *Wrangler {
	return &Wrangler{ts, tm.NewActionInitiator(ts), time.Now().Add(actionTimeout), lockTimeout, true}
}

func (wr *Wrangler) actionTimeout() time.Duration {
	return wr.deadline.Sub(time.Now())
}

func (wr *Wrangler) TopoServer() topo.Server {
	return wr.ts
}

func (wr *Wrangler) ActionInitiator() *tm.ActionInitiator {
	return wr.ai
}

// ResetActionTimeout should be used before every action on a wrangler
// object that is going to be re-used:
// - vtctl will not call this, as it does one action
// - vtctld will call this, as it re-uses the same wrangler for actions
func (wr *Wrangler) ResetActionTimeout(actionTimeout time.Duration) {
	wr.deadline = time.Now().Add(actionTimeout)
}

// signal handling
var interrupted = make(chan struct{})

func SignalInterrupt() {
	close(interrupted)
}
