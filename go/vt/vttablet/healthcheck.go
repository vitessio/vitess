// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

// This file handles the health check. It is enabled by passing a
// target_tablet_type command line parameter. The tablet will then go
// to the target tablet type if healthy, and to 'spare' if not.

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	healthCheckInterval = flag.Duration("health_check_interval", 20*time.Second, "Interval between health checks")
	targetTabletType    = flag.String("target_tablet_type", "", "The tablet type we are thriving to be when healthy. When not healthy, we'll go to spare.")
	lockTimeout         = flag.Duration("lock_timeout", actionnode.DefaultLockTimeout, "lock time for wrangler/topo operations")
)

func initHeathCheck(agent *tabletmanager.ActionAgent) {
	if *targetTabletType == "" {
		log.Infof("No target_tablet_type specified, disabling any health check")
		return
	}

	log.Infof("Starting periodic health check every %v with target_tablet_type=%v", *healthCheckInterval, *targetTabletType)
	t := timer.NewTimer(*healthCheckInterval)
	servenv.OnTerm(func() {
		// When we enter lameduck mode, we want to not call
		// the health check any more. After this returns, we
		// are guaranteed to not call it.
		t.Stop()

		// TODO(alainjobart) Now we can finish up and force
		// ourselves to not healthy.
		// agent.WeAreDoneWithServingMakeItSo(topo.TabletType(*targetTabletType), *lockTimeout)
	})
	t.Start(func() {
		agent.RunHealthCheck(topo.TabletType(*targetTabletType), *lockTimeout)
	})
}
