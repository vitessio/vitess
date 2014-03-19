package mysqlctl

import (
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/topo"
)

// MySQLReplication lag returns a reporter that reports the MySQL
// replication lag. It uses the key "replication_lag".
func MySQLReplicationLag(mysqld *Mysqld, allowedLagInSeconds int) health.Reporter {
	return health.FunctionReporter(func(typ topo.TabletType) (map[string]string, error) {
		if !topo.IsSlaveType(typ) {
			return nil, nil
		}

		rp, err := mysqld.SlaveStatus()
		if err != nil {
			return nil, err
		}
		if int(rp.SecondsBehindMaster) > allowedLagInSeconds {
			return map[string]string{health.ReplicationLag: health.ReplicationLagHigh}, nil
		}

		return nil, nil
	})

}
