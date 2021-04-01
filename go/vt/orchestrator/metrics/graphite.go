/*
   Copyright 2014 Outbrain Inc.

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

package metrics

import (
	"net"
	"strings"
	"time"

	graphite "github.com/cyberdelia/go-metrics-graphite"
	"github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/process"
)

// InitGraphiteMetrics is called once in the lifetime of the app, after config has been loaded
func InitGraphiteMetrics() error {
	if config.Config.GraphiteAddr == "" {
		return nil
	}
	if config.Config.GraphitePollSeconds <= 0 {
		return nil
	}
	if config.Config.GraphitePath == "" {
		return log.Errorf("No graphite path provided (see GraphitePath config variable). Will not log to graphite")
	}
	addr, err := net.ResolveTCPAddr("tcp", config.Config.GraphiteAddr)
	if err != nil {
		return log.Errore(err)
	}
	graphitePathHostname := process.ThisHostname
	if config.Config.GraphiteConvertHostnameDotsToUnderscores {
		graphitePathHostname = strings.Replace(graphitePathHostname, ".", "_", -1)
	}
	graphitePath := config.Config.GraphitePath
	graphitePath = strings.Replace(graphitePath, "{hostname}", graphitePathHostname, -1)

	log.Debugf("Will log to graphite on %+v, %+v", config.Config.GraphiteAddr, graphitePath)

	go graphite.Graphite(metrics.DefaultRegistry, 1*time.Minute, graphitePath, addr)

	return nil
}
