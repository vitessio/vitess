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

package tabletserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the status web page export for tabletserver

const (
	healthyClass   = "healthy"
	unhealthyClass = "unhealthy"
	unhappyClass   = "unhappy"
)

const (
	// This template is a slight duplicate of the one in go/cmd/vttablet/status.go.
	headerTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.5rem;
  }
  .time {
    width: 15%;
  }
  .healthy {
    background-color: LightGreen;
  }
  .unhealthy {
    background-color: Salmon;
  }
  .unhappy {
    background-color: Khaki;
  }
</style>
<table width="100%" border="" frame="">
  <tr border="">
    <td width="25%" border="">
      Alias: {{.Alias}}<br>
      Keyspace: {{.Target.Keyspace}}<br>
      Shard: {{.Target.Shard}}<br>
      TabletType: {{.Target.TabletType}}<br>
    </td>
    <td width="25%" border="">
      <a href="{{.Prefix}}/schemaz">Schema</a></br>
      <a href="{{.Prefix}}/debug/tablet_plans">Schema&nbsp;Query&nbsp;Plans</a></br>
      <a href="{{.Prefix}}/debug/query_stats">Schema&nbsp;Query&nbsp;Stats</a></br>
      <a href="{{.Prefix}}/queryz">Query&nbsp;Stats</a></br>
    </td>
    <td width="25%" border="">
      <a href="{{.Prefix}}/debug/consolidations">Consolidations</a></br>
      <a href="{{.Prefix}}/querylogz">Current&nbsp;Query&nbsp;Log</a></br>
      <a href="{{.Prefix}}/txlogz">Current&nbsp;Transaction&nbsp;Log</a></br>
      <a href="{{.Prefix}}/twopcz">In-flight&nbsp;2PC&nbsp;Transactions</a></br>
    </td>
    <td width="25%" border="">
      <a href="{{.Prefix}}/healthz">Health Check</a></br>
      <a href="{{.Prefix}}/debug/health">Query Service Health Check</a></br>
      <a href="{{.Prefix}}/livequeryz/">Real-time Queries</a></br>
      <a href="{{.Prefix}}/debug/status_details">JSON Status Details</a></br>
      <a href="{{.Prefix}}/debug/env">View/Change Environment variables</a></br>
    </td>
  </tr>
</table>
`

	queryserviceStatusTemplate = `
<div style="font-size: x-large">Current status: <span style="padding-left: 0.5em; padding-right: 0.5em; padding-bottom: 0.5ex; padding-top: 0.5ex;" class="{{.Latest.Class}}">{{.Latest.Status}}</span></div>
<h2>Health Details</h2>
<table>
  {{range .Details}}
  <tr class="{{.Class}}">
    <td>{{.Key}}</td>
    <td>{{.Value}}</td>
  </tr>
  {{end}}
</table>
<h2>Health History</h2>
<table>
  <tr>
    <th>Time</th>
    <th>Status</th>
    <th>Tablet Type</th>
  </tr>
  {{range .History}}
  <tr class="{{.Class}}">
    <td>{{.Time.Format "Jan 2, 2006 at 15:04:05 (MST)"}}</td>
    <td>{{.Status}}</td>
    <td>{{.TabletType}}</td>
  </tr>
  {{end}}
</table>
<dl style="font-size: small;">
  <dt><span class="healthy">healthy</span></dt>
  <dd>serving traffic.</dd>

  <dt><span class="unhappy">unhappy</span></dt>
  <dd>will serve traffic only if there are no fully healthy tablets.</dd>

  <dt><span class="unhealthy">unhealthy</span></dt>
  <dd>will not serve traffic.</dd>
</dl>
<!-- The div in the next line will be overwritten by the JavaScript graph. -->
<div id="qps_chart" style="height: 500px; width: 900px">QPS: {{.CurrentQPS}}</div>
<script src="https://www.gstatic.com/charts/loader.js"></script>
<script type="text/javascript">

google.load("visualization", "1", {packages:["corechart"]});

function sampleDate(d, i) {
  var copy = new Date(d);
  copy.setTime(copy.getTime() - i*60/5*1000);
  return copy
}

function drawQPSChart() {
  var div = document.getElementById("qps_chart")
  var chart = new google.visualization.LineChart(div);

  var options = {
    title: "QPS",
    focusTarget: 'category',
    vAxis: {
      viewWindow: {min: 0},
    }
  };

  // If we're accessing status through a proxy that requires a URL prefix,
  // add the prefix to the vars URL.
  var vars_url = '/debug/vars';
  var pos = window.location.pathname.lastIndexOf('/debug/status');
  if (pos > 0) {
    vars_url = window.location.pathname.substring(0, pos) + vars_url;
  }

  const redraw = () => fetch(vars_url)
  .then(async (response) => {
      const input_data = await response.json();
      var now = new Date();
      var qps = input_data.QPS;
      var planTypes = Object.keys(qps);
      if (planTypes.length === 0) {
        planTypes = ["All"];
        qps["All"] = [];
      }
      var data = [["Time"].concat(planTypes)];
      // Create data points, starting with the most recent timestamp.
      // (On the graph this means going from right to left.)
      // Time span: 15 minutes in 5 second intervals.
      for (var i = 0; i < 15*60/5; i++) {
        var datum = [sampleDate(now, i)];
        for (var j = 0; j < planTypes.length; j++) {
          if (i < qps[planTypes[j]].length) {
          	// Rates are ordered from least recent to most recent.
          	// Therefore, we have to start reading from the end of the array.
          	var idx = qps[planTypes[j]].length - i - 1;
            datum.push(+qps[planTypes[j]][idx].toFixed(2));
          } else {
            // Assume 0.0 QPS for older, non-existent data points.
            datum.push(0);
          }
        }
        data.push(datum)
      }
      chart.draw(google.visualization.arrayToDataTable(data), options);
  })

  redraw();

  // redraw every 2.5 seconds.
  window.setInterval(redraw, 2500);
}
google.setOnLoadCallback(drawQPSChart);
</script>
`
)

type queryserviceStatus struct {
	Latest     *historyRecord
	Details    []*kv
	History    []any
	CurrentQPS float64
}

type kv struct {
	Key   string
	Class string
	Value string
}

// AddStatusHeader registers a standlone header for the status page.
func (tsv *TabletServer) AddStatusHeader() {
	tsv.exporter.AddStatusPart("Tablet Server", headerTemplate, func() any {
		return map[string]any{
			"Alias":  tsv.exporter.Name(),
			"Prefix": tsv.exporter.URLPrefix(),
			"Target": tsv.sm.Target(),
		}
	})
}

// AddStatusPart registers the status part for the status page.
func (tsv *TabletServer) AddStatusPart() {
	// Save the threshold values for reporting.
	degradedThreshold.Store(tsv.config.Healthcheck.DegradedThreshold.Nanoseconds())
	unhealthyThreshold.Store(tsv.config.Healthcheck.UnhealthyThreshold.Nanoseconds())

	tsv.exporter.AddStatusPart("Health", queryserviceStatusTemplate, func() any {
		status := queryserviceStatus{
			History: tsv.hs.history.Records(),
		}
		latest := tsv.hs.history.Latest()
		if latest != nil {
			status.Latest = latest.(*historyRecord)
		} else {
			status.Latest = &historyRecord{}
		}
		status.Details = tsv.sm.AppendDetails(nil)
		status.Details = tsv.hs.AppendDetails(status.Details)
		rates := tsv.stats.QPSRates.Get()
		if qps, ok := rates["All"]; ok && len(qps) > 0 {
			status.CurrentQPS = qps[0]
		}
		return status
	})

	tsv.exporter.HandleFunc("/debug/status_details", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		details := tsv.sm.AppendDetails(nil)
		details = tsv.hs.AppendDetails(details)
		b, err := json.MarshalIndent(details, "", " ")
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		buf := bytes.NewBuffer(nil)
		json.HTMLEscape(buf, b)
		w.Write(buf.Bytes())
	})
}

var degradedThreshold atomic.Int64
var unhealthyThreshold atomic.Int64

type historyRecord struct {
	Time       time.Time
	serving    bool
	tabletType topodatapb.TabletType
	lag        time.Duration
	err        error
}

func (r *historyRecord) Class() string {
	if r.serving {
		if r.lag > time.Duration(degradedThreshold.Load()) {
			return unhappyClass
		}
		return healthyClass
	}
	return unhealthyClass
}

func (r *historyRecord) Status() string {
	if r.serving {
		if r.lag > time.Duration(degradedThreshold.Load()) {
			return fmt.Sprintf("replication delayed: %v", r.lag)
		}
		return "healthy"
	}
	if r.lag > time.Duration(unhealthyThreshold.Load()) {
		return fmt.Sprintf("not serving: replication delay %v", r.lag)
	}
	if r.err != nil {
		return fmt.Sprintf("not serving: %v", r.err)
	}
	return "not serving"
}

func (r *historyRecord) TabletType() string {
	return strings.ToLower(r.tabletType.String())
}

// IsDuplicate implements history.Deduplicable
func (r *historyRecord) IsDuplicate(other any) bool {
	rother, ok := other.(*historyRecord)
	if !ok {
		return false
	}
	return r.tabletType == rother.tabletType && r.serving == rother.serving && r.err == rother.err
}
