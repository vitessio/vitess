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
	"fmt"
	"sort"
	"sync"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	globalStats = &vrStats{}
)

func init() {
	globalStats.register()
}

// StatusSummary returns the summary status of vreplication.
func StatusSummary() (maxSecondsBehindMaster int64, binlogPlayersCount int32) {
	return globalStats.maxSecondsBehindMaster(), int32(globalStats.numControllers())
}

// AddStatusPart adds the vreplication status to the status page.
func AddStatusPart() {
	servenv.AddStatusPart("VReplication", vreplicationTemplate, func() interface{} {
		return globalStats.status()
	})
}

// vrStats exports the stats for Engine. It's a separate structure to
// prevent deadlocks with the mutex in Engine. The Engine pushes changes
// to this struct whenever there is a relevant change.
// This is a singleton.
type vrStats struct {
	mu          sync.Mutex
	isOpen      bool
	controllers map[int]*controller
}

func (st *vrStats) register() {
	stats.NewGaugeFunc("VReplicationStreamCount", "Number of vreplication streams", st.numControllers)
	stats.NewGaugeFunc("VReplicationSecondsBehindMasterMax", "Max vreplication seconds behind master", st.maxSecondsBehindMaster)
	stats.NewCountersFuncWithMultiLabels(
		"VReplicationSecondsBehindMaster",
		"vreplication seconds behind master per stream",
		// CAUTION: Always keep this label as "counts" because the Google
		//          internal monitoring depends on this specific value.
		[]string{"counts"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				result[fmt.Sprintf("%v", ct.id)] = ct.blpStats.SecondsBehindMaster.Get()
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationTotalSecondsBehindMaster",
		"vreplication seconds behind master aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				result += ct.blpStats.SecondsBehindMaster.Get()
			}
			return result
		})

	stats.NewRateFunc(
		"VReplicationQPS",
		"vreplication operations per second aggregated across all streams",
		func() map[string][]float64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string][]float64)
			for _, ct := range st.controllers {
				for k, v := range ct.blpStats.Rates.Get() {
					result[k] = v
				}
			}
			return result
		})

	stats.Publish("VReplicationSource", stats.StringMapFunc(func() map[string]string {
		st.mu.Lock()
		defer st.mu.Unlock()
		result := make(map[string]string, len(st.controllers))
		for _, ct := range st.controllers {
			result[fmt.Sprintf("%v", ct.id)] = ct.source.Keyspace + "/" + ct.source.Shard
		}
		return result
	}))
	stats.Publish("VReplicationSourceTablet", stats.StringMapFunc(func() map[string]string {
		st.mu.Lock()
		defer st.mu.Unlock()
		result := make(map[string]string, len(st.controllers))
		for _, ct := range st.controllers {
			result[fmt.Sprintf("%v", ct.id)] = ct.sourceTablet.Get()
		}
		return result
	}))
}

func (st *vrStats) numControllers() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return int64(len(st.controllers))
}

func (st *vrStats) maxSecondsBehindMaster() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	max := int64(0)
	for _, ct := range st.controllers {
		if cur := ct.blpStats.SecondsBehindMaster.Get(); cur > max {
			max = cur
		}
	}
	return max
}

func (st *vrStats) status() *EngineStatus {
	st.mu.Lock()
	defer st.mu.Unlock()

	status := &EngineStatus{}
	status.IsOpen = st.isOpen

	status.Controllers = make([]*ControllerStatus, len(st.controllers))
	i := 0
	for _, ct := range st.controllers {
		state := "Running"
		select {
		case <-ct.done:
			state = "Stopped"
		default:
		}
		status.Controllers[i] = &ControllerStatus{
			Index:               ct.id,
			Source:              ct.source.String(),
			StopPosition:        ct.stopPos,
			LastPosition:        ct.blpStats.LastPosition().String(),
			SecondsBehindMaster: ct.blpStats.SecondsBehindMaster.Get(),
			Counts:              ct.blpStats.Timings.Counts(),
			Rates:               ct.blpStats.Rates.Get(),
			State:               state,
			SourceTablet:        ct.sourceTablet.Get(),
			Messages:            ct.blpStats.MessageHistory(),
		}
		i++
	}
	sort.Slice(status.Controllers, func(i, j int) bool { return status.Controllers[i].Index < status.Controllers[j].Index })
	return status
}

// EngineStatus contains a renderable status of the Engine.
type EngineStatus struct {
	IsOpen      bool
	Controllers []*ControllerStatus
}

// ControllerStatus contains a renderable status of a controller.
type ControllerStatus struct {
	Index               uint32
	Source              string
	SourceShard         string
	StopPosition        string
	LastPosition        string
	SecondsBehindMaster int64
	Counts              map[string]int64
	Rates               map[string][]float64
	State               string
	SourceTablet        string
	Messages            []string
}

var vreplicationTemplate = `
{{if .IsOpen}}VReplication state: Open</br>
<table>
  <tr>
    <th>Index</th>
    <th>Source</th>
    <th>Source Tablet</th>
    <th>State</th>
    <th>Stop Position</th>
    <th>Last Position</th>
    <th>Seconds Behind Master</th>
    <th>Counts</th>
    <th>Rates</th>
    <th>Last Message</th>
  </tr>
  {{range .Controllers}}<tr>
      <td>{{.Index}}</td>
      <td>{{.Source}}</td>
      <td>{{.SourceTablet}}</td>
      <td>{{.State}}</td>
      <td>{{.StopPosition}}</td>
      <td>{{.LastPosition}}</td>
      <td>{{.SecondsBehindMaster}}</td>
      <td>{{range $key, $value := .Counts}}<b>{{$key}}</b>: {{$value}}<br>{{end}}</td>
      <td>{{range $key, $values := .Rates}}<b>{{$key}}</b>: {{range $values}}{{.}} {{end}}<br>{{end}}</td>
      <td>{{range $index, $value := .Messages}}{{$value}}<br>{{end}}</td>
    </tr>{{end}}
<div id="vreplication_qps_chart">QPS All Streams </div>

<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript">

function drawVReplicationQPSChart() {
  var div = $('#vreplication_qps_chart').height(500).width(900).unwrap()[0]
  var chart = new google.visualization.LineChart(div);

  var options = {
    title: "VReplication QPS across all streams",
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
      var qps = input_data.VReplicationQPS;
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
google.setOnLoadCallback(drawVReplicationQPSChart);
</script>
</table>{{else}}VReplication is closed.{{end}}
`
