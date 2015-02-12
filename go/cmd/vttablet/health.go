package main

import (
	"flag"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	enableReplicationLagCheck = flag.Bool("enable_replication_lag_check", false, "will register the mysql health check module that directly calls mysql")
)

func registerHealthReporter(qsc tabletserver.QueryServiceControl) {
	if *enableReplicationLagCheck {
		health.DefaultAggregator.Register("replication_reporter", mysqlctl.MySQLReplicationLag(agent.Mysqld))
	}
}
