package main

import (
	"flag"
	"fmt"
	"html/template"
	"time"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	enableReplicationLagCheck = flag.Bool("enable_replication_lag_check", false, "will register the mysql health check module that directly calls mysql")
)

// queryServiceRunning implements health.Reporter
type queryServiceRunning struct {
	qsc tabletserver.QueryServiceControl
}

// Report is part of the health.Reporter interface
func (qsr *queryServiceRunning) Report(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	isQueryServiceRunning := qsr.qsc.IsServing()
	if shouldQueryServiceBeRunning != isQueryServiceRunning {
		return 0, fmt.Errorf("QueryService running=%v, expected=%v", isQueryServiceRunning, shouldQueryServiceBeRunning)
	}
	if isQueryServiceRunning {
		if err := qsr.qsc.IsHealthy(); err != nil {
			return 0, fmt.Errorf("QueryService is running, but not healthy: %v", err)
		}
	}
	return 0, nil
}

// HTMLName is part of the health.Reporter interface
func (qsr *queryServiceRunning) HTMLName() template.HTML {
	return template.HTML("QueryServiceRunning")
}

func registerHealthReporters(qsc tabletserver.QueryServiceControl) {
	if *enableReplicationLagCheck {
		health.Register("replication_reporter", mysqlctl.MySQLReplicationLag(agent.Mysqld))
	}
	health.Register("query_service_reporter", &queryServiceRunning{qsc})
}
