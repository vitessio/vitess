package tabletmanager

import (
	"html/template"
	"time"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
)

// Reporter is a wrapper around a heartbeat Reader, to be used as an interface from
// the health check system.
type Reporter struct {
	controller tabletserver.Controller
}

// RegisterReporter registers the heartbeat reader as a healthcheck reporter so that its
// measurements will be picked up in healthchecks.
func registerHeartbeatReporter(controller tabletserver.Controller) {
	if !tabletenv.Config.HeartbeatEnable {
		return
	}

	reporter := &Reporter{controller}
	health.DefaultAggregator.Register("heartbeat_reporter", reporter)
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
	return r.controller.HeartbeatLag()
}
