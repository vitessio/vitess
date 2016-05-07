package mysqlctl

import (
	"html/template"
	"time"

	"github.com/youtube/vitess/go/vt/health"
)

// mysqlReplicationLag implements health.Reporter
type mysqlReplicationLag struct {
	mysqld *Mysqld
}

// Report is part of the health.Reporter interface
func (mrl *mysqlReplicationLag) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	if !isSlaveType {
		return 0, nil
	}

	slaveStatus, err := mrl.mysqld.SlaveStatus()
	if err != nil {
		return 0, err
	}
	// If the slave is not running, mysqld.SlaveStatus() will set
	// SecondsBehindMaster to 0. We allow this because we don't want
	// a stopped slave to be forced unhealthy.
	return time.Duration(slaveStatus.SecondsBehindMaster) * time.Second, nil
}

// HTMLName is part of the health.Reporter interface
func (mrl *mysqlReplicationLag) HTMLName() template.HTML {
	return template.HTML("MySQLReplicationLag")
}

// MySQLReplicationLag lag returns a reporter that reports the MySQL
// replication lag.
func MySQLReplicationLag(mysqld *Mysqld) health.Reporter {
	return &mysqlReplicationLag{mysqld}
}
