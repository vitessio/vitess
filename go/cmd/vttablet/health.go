package main

import (
	"flag"
	"fmt"
	"html/template"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	allowedReplicationLag = flag.Int("allowed_replication_lag", 0, "how many seconds of replication lag will make this tablet unhealthy (ignored if the value is 0)")
)

// queryServiceRunning implements health.Reporter
type queryServiceRunning struct{}

// Report is part of the health.Reporter interface
func (qsr *queryServiceRunning) Report(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (status map[string]string, err error) {
	isQueryServiceRunning := tabletserver.SqlQueryRpcService.GetState() == "SERVING"
	if shouldQueryServiceBeRunning != isQueryServiceRunning {
		return nil, fmt.Errorf("QueryService running=%v, expected=%v", isQueryServiceRunning, shouldQueryServiceBeRunning)
	}
	if isQueryServiceRunning {
		if err := tabletserver.IsHealthy(); err != nil {
			return nil, fmt.Errorf("QueryService is running, but not healthy: %v", err)
		}
	}
	return nil, nil
}

// HTMLName is part of the health.Reporter interface
func (qsr *queryServiceRunning) HTMLName() template.HTML {
	return template.HTML("QueryServiceRunning")
}

func init() {
	servenv.OnRun(func() {
		if *allowedReplicationLag > 0 {
			health.Register("replication_reporter", mysqlctl.MySQLReplicationLag(agent.Mysqld, *allowedReplicationLag))
		}
		health.Register("query_service_reporter", &queryServiceRunning{})
	})
}
