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
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"context"

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

// replManager runs a poller to ensure mysql is replicating from
// the primary. If necessary, it invokes tm.repairReplication to get it
// fixed. On state change, SetTabletType must be called before changing
// the tabletserver state. This will ensure that replication is fixed
// upfront, allowing tabletserver to start off healthy.
type replManager struct {
	ctx        context.Context
	tm         *TabletManager
	markerFile string
	ticks      *timer.Timer
	failed     bool

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

// SetTabletType starts/stops the replication manager ticks based on the tablet type provided.
// It stops the ticks if the tablet type is not a replica type, starts the ticks otherwise.
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
		log.Info("Replication Manager: stopped")
		return
	}
	if rm.ticks.Running() {
		return
	}
	log.Info("Replication Manager: starting")
	// Run an immediate check to fix replication if it was broken.
	// A higher caller may already have te action lock. So, we use
	// a code path that avoids it.
	rm.checkActionLocked()
	rm.ticks.Start(rm.check)
}

func (rm *replManager) check() {
	// We need to obtain the action lock if we're going to fix
	// replication, but only if the lock is available to take.
	if !rm.tm.tryLock() {
		return
	}
	defer rm.tm.unlock()
	rm.checkActionLocked()
}

func (rm *replManager) checkPrimaryIsSource(status mysql.ReplicationStatus) (bool, error) {
	tablet := rm.tm.Tablet()

	si, err := rm.tm.TopoServer.GetShard(rm.ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return false, err
	}

	if !si.HasPrimary() {
		return false, fmt.Errorf("no primary tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	primary, err := rm.tm.TopoServer.GetTablet(rm.ctx, si.PrimaryAlias)
	if err != nil {
		return false, err
	}
	host := primary.Tablet.MysqlHostname
	port := int(primary.Tablet.MysqlPort)

	return status.SourceHost == host && status.SourcePort == port, nil
}

func (rm *replManager) checkActionLocked() {
	status, err := rm.tm.MysqlDaemon.ReplicationStatus()
	if err != nil {
		if err != mysql.ErrNotReplica {
			return
		}
	} else {
		// If only one of the threads is stopped, it's probably
		// intentional. So, we don't repair replication.
		if status.SQLHealthy() || status.IOHealthy() {
			// Check if the Primary is still the source though
			// We need this check only when one thread is healthy, since we expect this failure to only cause the IOThread to go unhealthy
			// and if both the threads are unhealthy, we want to repair replication irrespective of having the correct source port of the primary
			if primaryIsSource, err := rm.checkPrimaryIsSource(status); err == nil && primaryIsSource {
				return
			}
		}
	}

	if !rm.failed {
		log.Infof("Replication is stopped, reconnecting to primary.")
	}
	ctx, cancel := context.WithTimeout(rm.ctx, 5*time.Second)
	defer cancel()
	if err := rm.tm.repairReplication(ctx); err != nil {
		if !rm.failed {
			rm.failed = true
			log.Infof("Failed to reconnect to primary: %v, will keep retrying.", err)
		}
		return
	}
	log.Info("Successfully reconnected to primary.")
	rm.failed = false
}

// reset the replication manager state and deleting the marker-file.
// it does not start or stop the ticks. Use setReplicationStopped instead to change that.
func (rm *replManager) reset() {
	if *mysqlctl.DisableActiveReparents {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.replStopped = nil
	if rm.markerFile == "" {
		return
	}
	os.Remove(rm.markerFile)
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

	if stopped {
		rm.ticks.Stop()
	} else {
		rm.ticks.Start(rm.check)
	}

	if rm.markerFile == "" {
		return
	}
	if stopped {
		if file, err := os.Create(rm.markerFile); err == nil {
			file.Close()
		}
	} else {
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
