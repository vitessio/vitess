/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"html/template"
	"time"

	"vitess.io/vitess/go/vt/health"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
