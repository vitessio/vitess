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
	"time"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// This file contains the status web page export for tabletserver

var queryserviceStatusTemplate = `
<h2>State: {{.State}}</h2>
<h2>Queryservice History</h2>
<table>
  <tr>
    <th>Time</th>
    <th>Target Tablet Type</th>
    <th>Serving State</th>
  </tr>
  {{range .History}}
  <tr>
    <td>{{.Time.Format "Jan 2, 2006 at 15:04:05 (MST)"}}</td>
    <td>{{.TabletType}}</td>
    <td>{{.ServingState}}</td>
  </tr>
  {{end}}
</table>
<!-- The div in the next line will be overwritten by the JavaScript graph. -->
<div id="qps_chart">QPS: {{.CurrentQPS}}</div>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript">

google.load("jquery", "1.4.0");
google.load("visualization", "1", {packages:["corechart"]});

function sampleDate(d, i) {
  var copy = new Date(d);
  copy.setTime(copy.getTime() - i*60/5*1000);
  return copy
}

function drawQPSChart() {
  var div = $('#qps_chart').height(500).width(900).unwrap()[0]
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

  var redraw = function() {
    $.getJSON(vars_url, function(input_data) {
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
  };

  redraw();

  // redraw every 2.5 seconds.
  window.setInterval(redraw, 2500);
}
google.setOnLoadCallback(drawQPSChart);
</script>

`

type queryserviceStatus struct {
	State      string
	History    []interface{}
	CurrentQPS float64
}

// AddStatusPart registers the status part for the status page.
func (tsv *TabletServer) AddStatusPart() {
	servenv.AddStatusPart("Queryservice", queryserviceStatusTemplate, func() interface{} {
		status := queryserviceStatus{
			State:   tsv.GetState(),
			History: tsv.history.Records(),
		}
		rates := tabletenv.QPSRates.Get()
		if qps, ok := rates["All"]; ok && len(qps) > 0 {
			status.CurrentQPS = qps[0]
		}
		return status
	})
}

type historyRecord struct {
	Time         time.Time
	TabletType   string
	ServingState string
}

// IsDuplicate implements history.Deduplicable
func (r *historyRecord) IsDuplicate(other interface{}) bool {
	rother, ok := other.(*historyRecord)
	if !ok {
		return false
	}
	return r.TabletType == rother.TabletType && r.ServingState == rother.ServingState
}
