package main

import (
	"flag"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
)

var (
	enableReplicationLagCheck = flag.Bool("enable_replication_lag_check", false, "will register the mysql health check module that directly calls mysql")
)

func registerHealthReporter(mysqld *mysqlctl.Mysqld) {
	if *enableReplicationLagCheck {
		health.DefaultAggregator.Register("replication_reporter", mysqlctl.MySQLReplicationLag(mysqld))
	}
}
