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

/*
 * A human readable streaming log of vreplication events for vttablets available at /debug/vrlog
 */

package vreplication

import (
	"net/http"
	"strconv"
	"text/template"
	"time"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/log"
)

var (
	vrLogStatsLogger   = streamlog.New("VReplication", 50)
	vrLogStatsTemplate = template.Must(template.New("vrlog").
				Parse("{{.Type}} Event	{{.Detail}}	{{.LogTime}}	{{.DurationNs}}\n"))
)

//VrLogStats collects attributes of a vreplication event for logging
type VrLogStats struct {
	Type       string
	Detail     string
	StartTime  time.Time
	LogTime    string
	DurationNs int64
}

//NewVrLogStats should be called at the start of the event to be logged
func NewVrLogStats(eventType string) *VrLogStats {
	return &VrLogStats{Type: eventType, StartTime: time.Now()}
}

//Send records the log event, should be called on a stats object constructed by NewVrLogStats()
func (stats *VrLogStats) Send(detail string) {
	if stats.StartTime.IsZero() {
		stats.Type = "Error: Type not specified"
		stats.StartTime = time.Now()
	}
	stats.LogTime = stats.StartTime.Format("2006-01-02T15:04:05")
	stats.Detail = detail
	stats.DurationNs = time.Since(stats.StartTime).Nanoseconds()
	vrLogStatsLogger.Send(stats)
}

func init() {
	http.HandleFunc("/debug/vrlog", func(w http.ResponseWriter, r *http.Request) {
		ch := vrLogStatsLogger.Subscribe("vrlogstats")
		defer vrLogStatsLogger.Unsubscribe(ch)
		vrlogStatsHandler(ch, w, r)
	})
}

func vrlogStatsHandler(ch chan interface{}, w http.ResponseWriter, r *http.Request) {
	timeout, limit := parseTimeoutLimitParams(r)
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for i := 0; i < limit; i++ {
		select {
		case out := <-ch:
			select {
			case <-tmr.C:
				return
			default:
			}
			stats, ok := out.(*VrLogStats)
			if !ok {
				log.Error("Log received is not of type VrLogStats")
				continue
			}
			if err := vrLogStatsTemplate.Execute(w, stats); err != nil {
				log.Errorf("vrlog: couldn't execute template: %v", err)
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-tmr.C:
			return
		}
	}
}

func parseTimeoutLimitParams(req *http.Request) (time.Duration, int) {
	timeout := 1000
	limit := 300
	if ts, ok := req.URL.Query()["timeout"]; ok {
		if t, err := strconv.Atoi(ts[0]); err == nil {
			timeout = adjustValue(t, 0, 60)
		}
	}
	if l, ok := req.URL.Query()["limit"]; ok {
		if lim, err := strconv.Atoi(l[0]); err == nil {
			limit = adjustValue(lim, 1, 200000)
		}
	}
	return time.Duration(timeout) * time.Second, limit
}

func adjustValue(val int, lower int, upper int) int {
	if val < lower {
		return lower
	} else if val > upper {
		return upper
	}
	return val
}
