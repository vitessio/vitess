package heartbeat

import (
	"html/template"
	"time"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
)

// Reporter is a wrapper around a heartbeat Reader, to be used as an interface from
// the health check system
type Reporter struct {
	*Reader
}

// RegisterReporter registers the heartbeat reader as a healthcheck reporter so that its
// measurements will be picked up in healthchecks.
func RegisterReporter(topoServer topo.Server, mysqld mysqlctl.MysqlDaemon, tablet *topodata.Tablet, dbc dbconfigs.DBConfigs) *Reporter {
	if !*enableHeartbeat {
		return nil
	}

	reporter := &Reporter{NewReader(topoServer, mysqld, tablet)}
	reporter.Open(dbc)
	health.DefaultAggregator.Register("heartbeat_reporter", reporter)

	return reporter
}

// HTMLName is part of the health.Reporter interface.
func (r *Reporter) HTMLName() template.HTML {
	return template.HTML("MySQLHeartbeat")
}

// Report is part of the health.Reporter interface. It returns the last reported value
// written by the watchHeartbeat goroutine. If we're the master, it just returns 0.
func (r *Reporter) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	if !isSlaveType {
		return 0, nil
	}
	return r.GetLatest()
}
