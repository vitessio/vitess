package mysqlctl

import (
	"html/template"
	"time"

	"github.com/youtube/vitess/go/vt/health"
)

// mysqlReplicationLag implements health.Reporter
type mysqlReplicationLag struct {
	// set at construction time
	mysqld MysqlDaemon
	now    func() time.Time

	// store the last time we successfully got the lag, so if we
	// can't get the lag any more, we can extrapolate.
	lastKnownValue time.Duration
	lastKnownTime  time.Time
}

// Report is part of the health.Reporter interface
func (mrl *mysqlReplicationLag) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	if !isSlaveType {
		return 0, nil
	}

	slaveStatus, err := mrl.mysqld.SlaveStatus()
	if err == nil && !slaveStatus.SlaveRunning() {
		err = health.ErrSlaveNotRunning
	}
	if err != nil {
		if !mrl.lastKnownTime.IsZero() {
			// we can extrapolate with the worst possible
			// value (that is we made no replication
			// progress since last time, and just fell more behind)
			elapsed := mrl.now().Sub(mrl.lastKnownTime)
			return elapsed + mrl.lastKnownValue, nil
		}
		return 0, err
	}
	mrl.lastKnownValue = time.Duration(slaveStatus.SecondsBehindMaster) * time.Second
	mrl.lastKnownTime = mrl.now()
	return mrl.lastKnownValue, nil
}

// HTMLName is part of the health.Reporter interface
func (mrl *mysqlReplicationLag) HTMLName() template.HTML {
	return template.HTML("MySQLReplicationLag")
}

// MySQLReplicationLag lag returns a reporter that reports the MySQL
// replication lag.
func MySQLReplicationLag(mysqld MysqlDaemon) health.Reporter {
	return &mysqlReplicationLag{
		mysqld: mysqld,
		now:    time.Now,
	}
}
