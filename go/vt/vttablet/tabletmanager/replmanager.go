/*
Copyright 2020 The Vitess Authors.

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

package tabletmanager

import (
	"os"
	"path"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

const (
	// replicationStoppedFile is the name of the file whose existence informs
	// vttablet to NOT try to repair replication.
	replicationStoppedFile = "do_not_replicate"
)

// replManager manages runs a poller to ensure mysql is replicating from
// the master. If necessary, it invokes tm.repairReplication to get it
// fixed. On state change, SetTabletType must be called before changing
// the tabletserver state. This will ensure that replication is fixed
// upfront, allowing tabletserver to start off healthy.
type replManager struct {
	ctx        context.Context
	tm         *TabletManager
	markerFile string
	ticks      *timer.Timer

	// replStopped is tri-state.
	// A nil value signifies that the value is not set.
	mu          sync.Mutex
	replStopped *bool
}

func newReplManager(ctx context.Context, tm *TabletManager, interval time.Duration) *replManager {
	return &replManager{
		ctx:        ctx,
		tm:         tm,
		markerFile: markerFile(tm.Cnf),
		ticks:      timer.NewTimer(interval),
	}
}

func (rm *replManager) SetTabletType(tabletType topodatapb.TabletType) {
	if *mysqlctl.DisableActiveReparents {
		return
	}
	if !topo.IsReplicaType(tabletType) {
		rm.ticks.Stop()
		return
	}
	if rm.replicationStopped() {
		// Stop just to be safe.
		rm.ticks.Stop()
		return
	}
	// Do an explicit check and then start the timer.
	rm.check()
	rm.ticks.Start(rm.check)
}

func (rm *replManager) check() {
	status, err := rm.tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		if err != mysql.ErrNotReplica {
			return
		}
	} else {
		// TODO(sougou): this was ported from previous behavior.
		// Need to check if this is should be &&.
		if status.SQLThreadRunning || status.IOThreadRunning {
			return
		}
	}

	log.Infof("Replication is stopped. Trying to reconnect to master...")
	ctx, cancel := context.WithTimeout(rm.ctx, 5*time.Second)
	defer cancel()
	if err := rm.tm.repairReplication(ctx); err != nil {
		log.Infof("Failed to reconnect to master: %v", err)
	}
}

// setReplicationStopped performs a best effort attempt of
// remembering a decision to stop replication.
func (rm *replManager) setReplicationStopped(stopped bool) {
	if *mysqlctl.DisableActiveReparents {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.replStopped = &stopped

	if rm.markerFile == "" {
		return
	}
	if stopped {
		rm.ticks.Stop()
		if file, err := os.Create(rm.markerFile); err == nil {
			file.Close()
		}
	} else {
		rm.ticks.Start(rm.check)
		os.Remove(rm.markerFile)
	}
}

func (rm *replManager) replicationStopped() bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.replStopped != nil {
		// Return cached value.
		return *rm.replStopped
	}
	if rm.markerFile == "" {
		return false
	}

	_, err := os.Stat(rm.markerFile)
	replicationStopped := err == nil
	rm.replStopped = &replicationStopped
	return replicationStopped
}

func markerFile(cnf *mysqlctl.Mycnf) string {
	if cnf == nil {
		return ""
	}
	return path.Join(cnf.TabletDir(), replicationStoppedFile)
}
