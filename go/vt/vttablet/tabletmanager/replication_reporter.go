/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"flag"
	"fmt"
	"html/template"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/health"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	enableReplicationReporter = flag.Bool("enable_replication_reporter", false, "Register the health check module that monitors MySQL replication")
)

// replicationReporter implements health.Reporter
type replicationReporter struct {
	// set at construction time
	tm  *TabletManager
	now func() time.Time

	// store the last time we successfully got the lag, so if we
	// can't get the lag any more, we can extrapolate.
	lastKnownValue time.Duration
	lastKnownTime  time.Time
}

// Report is part of the health.Reporter interface
func (r *replicationReporter) Report(isReplicaType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	if !isReplicaType {
		return 0, nil
	}

	status, statusErr := r.tm.MysqlDaemon.ReplicationStatus()
	if statusErr == mysql.ErrNotReplica ||
		(statusErr == nil && !status.SQLThreadRunning && !status.IOThreadRunning) {
		// MySQL is up, but replica is either not configured or not running.
		// Both SQL and IO threads are stopped, so it's probably either
		// stopped on purpose, or stopped because of a mysqld restart.
		if !r.tm.replicationStopped() {
			// As far as we've been told, it isn't stopped on purpose,
			// so let's try to start it.
			if *mysqlctl.DisableActiveReparents {
				log.Infof("replication is stopped. Running with --disable_active_reparents so will not try to reconnect to master...")
			} else {
				log.Infof("replication is stopped. Trying to reconnect to master...")
				ctx, cancel := context.WithTimeout(r.tm.BatchCtx, 5*time.Second)
				if err := repairReplication(ctx, r.tm); err != nil {
					log.Infof("Failed to reconnect to master: %v", err)
				}
				cancel()
				// Check status again.
				status, statusErr = r.tm.MysqlDaemon.ReplicationStatus()
			}
		}
	}
	if statusErr != nil {
		// mysqld is not running or replication is not configured.
		// We can't report healthy.
		return 0, statusErr
	}
	if !status.ReplicationRunning() {
		// mysqld is running, but replication is not replicating (most likely,
		// replication has been stopped). See if we can extrapolate.
		if r.lastKnownTime.IsZero() {
			// we can't.
			return 0, health.ErrReplicationNotRunning
		}

		// we can extrapolate with the worst possible
		// value (that is we made no replication
		// progress since last time, and just fell more behind).
		elapsed := r.now().Sub(r.lastKnownTime)
		return elapsed + r.lastKnownValue, nil
	}

	// we got a real value, save it.
	r.lastKnownValue = time.Duration(status.SecondsBehindMaster) * time.Second
	r.lastKnownTime = r.now()
	return r.lastKnownValue, nil
}

// HTMLName is part of the health.Reporter interface
func (r *replicationReporter) HTMLName() template.HTML {
	return template.HTML("MySQLReplicationLag")
}

// repairReplication tries to connect this server to whoever is
// the current master of the shard, and start replicating.
func repairReplication(ctx context.Context, tm *TabletManager) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("can't repair replication with --disable_active_reparents")
	}

	ts := tm.TopoServer
	tablet := tm.Tablet()

	si, err := ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	if !si.HasMaster() {
		return fmt.Errorf("no master tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}

	if topoproto.TabletAliasEqual(si.MasterAlias, tablet.Alias) {
		// The shard record says we are master, but we disagree; we wouldn't
		// reach this point unless we were told to check replication.
		// Hopefully someone is working on fixing that, but in any case,
		// we should not try to reparent to ourselves.
		return fmt.Errorf("shard %v/%v record claims tablet %v is master, but its type is %v", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(tablet.Alias), tablet.Type)
	}

	// If Orchestrator is configured and if Orchestrator is actively reparenting, we should not repairReplication
	if tm.orc != nil {
		re, err := tm.orc.InActiveShardRecovery(tablet)
		if err != nil {
			return err
		}
		if re {
			return fmt.Errorf("orchestrator actively reparenting shard %v, skipping repairReplication", si)
		}

		// Before repairing replication, tell Orchestrator to enter maintenance mode for this tablet and to
		// lock any other actions on this tablet by Orchestrator.
		if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to StopReplication"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
			return vterrors.Wrap(err, "orchestrator BeginMaintenance failed, skipping repairReplication")
		}
	}

	return tm.setMasterRepairReplication(ctx, si.MasterAlias, 0, "", true)
}

func registerReplicationReporter(tm *TabletManager) {
	if *enableReplicationReporter {
		health.DefaultAggregator.Register("replication_reporter",
			&replicationReporter{
				tm:  tm,
				now: time.Now,
			})
	}
}
