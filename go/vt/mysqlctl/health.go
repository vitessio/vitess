package mysqlctl

import (
	"fmt"
	"html/template"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/topo"
)

// mySQLReplicationLag implements health.Reporter
type mysqlReplicationLag struct {
	mysqld              *Mysqld
	allowedLagInSeconds int
}

func (mrl *mysqlReplicationLag) Report(typ topo.TabletType) (status map[string]string, err error) {
	if !topo.IsSlaveType(typ) {
		return nil, nil
	}

	slaveStatus, err := mrl.mysqld.SlaveStatus()
	if err != nil {
		return nil, err
	}
	if !slaveStatus.SlaveRunning() || int(slaveStatus.SecondsBehindMaster) > mrl.allowedLagInSeconds {
		return map[string]string{health.ReplicationLag: health.ReplicationLagHigh}, nil
	}

	return nil, nil
}

func (mrl *mysqlReplicationLag) HTMLName() template.HTML {
	return template.HTML(fmt.Sprintf("MySQLReplicationLag(allowedLag=%v)", mrl.allowedLagInSeconds))
}

// MySQLReplication lag returns a reporter that reports the MySQL
// replication lag. It uses the key "replication_lag".
func MySQLReplicationLag(mysqld *Mysqld, allowedLagInSeconds int) health.Reporter {
	return &mysqlReplicationLag{mysqld, allowedLagInSeconds}
}
