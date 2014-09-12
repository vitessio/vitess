package main

import (
	"flag"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
)

var (
	allowedReplicationLag = flag.Int("allowed_replication_lag", 0, "how many seconds of replication lag will make this tablet unhealthy (ignored if the value is 0)")
)

func init() {
	servenv.OnRun(func() {
		if *allowedReplicationLag > 0 {
			health.Register("replication_reporter", mysqlctl.MySQLReplicationLag(agent.Mysqld, *allowedReplicationLag))
		}
	})
}
