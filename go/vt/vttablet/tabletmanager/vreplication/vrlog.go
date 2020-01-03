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

package vreplication

import (
	"net/http"
	"strconv"
	"text/template"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/log"
)

var (
	vrLogStatsLogger   = streamlog.New("VReplication", 50)
	vrLogStatsTemplate = template.Must(template.New("vrlog").
				Parse("{{.Type}} Event	{{.Detail}}	{{.Time}}	{{.DurationNs}}\n"))
)

type VrLogStats struct {
	Ctx        context.Context
	Type       string
	Detail     string
	Time       time.Time
	DurationNs int64
}

func NewVrLogStats(ctx context.Context, eventType string) *VrLogStats {
	return &VrLogStats{Ctx: ctx, Type: eventType, Time: time.Now()}
}

func (stats *VrLogStats) Send() {
	vrLogStatsLogger.Send(stats)
}

func (stats *VrLogStats) Record(detail string) bool {
	if stats.Ctx == nil || stats.Time.IsZero() {
		log.Error("VrLogStats not initialized for %s", detail)
		return false
	}
	stats.Detail = detail
	stats.DurationNs = time.Since(stats.Time).Nanoseconds()
	stats.Send()
	return true
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
