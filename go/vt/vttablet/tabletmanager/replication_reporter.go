package tabletmanager

import (
	"flag"
	"fmt"
	"html/template"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
)

var (
	enableReplicationReporter = flag.Bool("enable_replication_reporter", false, "Register the health check module that monitors MySQL replication")
)

// replicationReporter implements health.Reporter
type replicationReporter struct {
	// set at construction time
	agent *ActionAgent
	now   func() time.Time

	// store the last time we successfully got the lag, so if we
	// can't get the lag any more, we can extrapolate.
	lastKnownValue time.Duration
	lastKnownTime  time.Time
}

// Report is part of the health.Reporter interface
func (r *replicationReporter) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	if !isSlaveType {
		return 0, nil
	}

	status, statusErr := r.agent.MysqlDaemon.SlaveStatus()
	if statusErr == mysqlctl.ErrNotSlave ||
		(statusErr == nil && !status.SlaveSQLRunning && !status.SlaveIORunning) {
		// MySQL is up, but slave is either not configured or not running.
		// Both SQL and IO threads are stopped, so it's probably either
		// stopped on purpose, or stopped because of a mysqld restart.
		if !r.agent.slaveStopped() {
			// As far as we've been told, it isn't stopped on purpose,
			// so let's try to start it.
			log.Infof("Slave is stopped. Trying to reconnect to master...")
			ctx, cancel := context.WithTimeout(r.agent.batchCtx, 5*time.Second)
			if err := repairReplication(ctx, r.agent); err != nil {
				log.Infof("Failed to reconnect to master: %v", err)
			}
			cancel()
			// Check status again.
			status, statusErr = r.agent.MysqlDaemon.SlaveStatus()
		}
	}
	if statusErr != nil {
		// mysqld is not running or slave is not configured.
		// We can't report healthy.
		return 0, statusErr
	}
	if !status.SlaveRunning() {
		// mysqld is running, but slave is not replicating (most likely,
		// replication has been stopped). See if we can extrapolate.
		if r.lastKnownTime.IsZero() {
			// we can't.
			return 0, health.ErrSlaveNotRunning
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

// repairReplication tries to connect this slave to whoever is
// the current master of the shard, and start replicating.
func repairReplication(ctx context.Context, agent *ActionAgent) error {
	ts := agent.TopoServer
	tablet := agent.Tablet()
	si, err := ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}
	if !si.HasMaster() {
		return fmt.Errorf("no master tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	return agent.setMasterLocked(ctx, si.MasterAlias, 0, true)
}

func registerReplicationReporter(agent *ActionAgent) {
	if *enableReplicationReporter {
		health.DefaultAggregator.Register("replication_reporter",
			&replicationReporter{
				agent: agent,
				now:   time.Now,
			})
	}
}
