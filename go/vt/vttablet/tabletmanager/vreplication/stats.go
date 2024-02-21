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
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	globalStats = &vrStats{}
)

func init() {
	globalStats.register()
}

// StatusSummary returns the summary status of vreplication.
func StatusSummary() (maxReplicationLagSeconds int64, binlogPlayersCount int32) {
	return globalStats.maxReplicationLagSeconds(), int32(globalStats.numControllers())
}

// AddStatusPart adds the vreplication status to the status page.
func AddStatusPart() {
	servenv.AddStatusPart("VReplication", vreplicationTemplate, func() any {
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
	controllers map[int32]*controller

	ThrottledCount *stats.Counter
}

func (st *vrStats) register() {
	st.ThrottledCount = stats.NewCounter("", "")
	stats.NewGaugeFunc("VReplicationStreamCount", "Number of vreplication streams", st.numControllers)
	stats.NewGaugeFunc("VReplicationLagSecondsMax", "Max vreplication seconds behind primary", st.maxReplicationLagSeconds)
	stats.NewStringMapFuncWithMultiLabels(
		"VReplicationStreamState",
		"State of vreplication workflow",
		[]string{"workflow", "counts"},
		"state",
		func() map[string]string {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]string, len(st.controllers))
			for _, ct := range st.controllers {
				state := ct.blpStats.State.Load()
				if state != nil {
					result[ct.workflow+"."+fmt.Sprintf("%v", ct.id)] = state.(string)
				}
			}
			return result
		},
	)
	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationLagSeconds",
		"vreplication seconds behind primary per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)] = ct.blpStats.ReplicationLagSeconds.Load()
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationLagSecondsTotal",
		"vreplication seconds behind primary aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				result += ct.blpStats.ReplicationLagSeconds.Load()
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

	stats.NewRateFunc(
		"VReplicationLag",
		"vreplication lag per stream",
		func() map[string][]float64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string][]float64)
			for _, ct := range st.controllers {
				for k, v := range ct.blpStats.VReplicationLagRates.Get() {
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
			ta := ct.sourceTablet.Load()
			if ta != nil {
				result[fmt.Sprintf("%v", ct.id)] = ta.(*topodatapb.TabletAlias).String()
			}
		}
		return result
	}))
	stats.Publish("VReplicationMessages", stats.StringMapFunc(func() map[string]string {
		st.mu.Lock()
		defer st.mu.Unlock()
		result := make(map[string]string, len(st.controllers))
		for _, ct := range st.controllers {
			var messages []string
			for _, rec := range ct.blpStats.History.Records() {
				hist := rec.(*binlogplayer.StatsHistoryRecord)
				messages = append(messages, fmt.Sprintf("%s:%s", hist.Time.Format(time.RFC3339Nano), hist.Message))
			}
			result[fmt.Sprintf("%v", ct.id)] = strings.Join(messages, "; ")
		}
		return result
	}))
	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationPhaseTimings",
		"vreplication per phase timings per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "phase"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for phase, t := range ct.blpStats.PhaseTimings.Histograms() {
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+phase] = t.Total()
				}
			}
			return result
		})
	stats.NewCounterFunc(
		"VReplicationPhaseTimingsTotal",
		"vreplication per phase timings aggregated across all phases and streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				for _, t := range ct.blpStats.PhaseTimings.Histograms() {
					result += t.Total()
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationPhaseTimingsCounts",
		"vreplication per phase count of timings per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "phase"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for phase, t := range ct.blpStats.PhaseTimings.Counts() {
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+phase] = t
				}
			}
			return result
		})
	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationQueryCount",
		"vreplication query counts per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "phase"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for label, count := range ct.blpStats.QueryCount.Counts() {
					if label == "" {
						continue
					}
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+label] = count
				}
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationQueryCountTotal",
		"vreplication query counts aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				for _, count := range ct.blpStats.QueryCount.Counts() {
					result += count
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationBulkQueryCount",
		"vreplication vplayer queries with consolidated row events counts per DML type per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "dml_type"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for label, count := range ct.blpStats.BulkQueryCount.Counts() {
					if label == "" {
						continue
					}
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+label] = count
				}
			}
			return result
		})
	stats.NewCounterFunc(
		"VReplicationBulkQueryCountTotal",
		"vreplication vplayer queries with consolidated row events counts aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				for _, count := range ct.blpStats.BulkQueryCount.Counts() {
					result += count
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationNoopQueryCount",
		"vreplication noop query counts per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "phase"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for label, count := range ct.blpStats.NoopQueryCount.Counts() {
					if label == "" {
						continue
					}
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+label] = count
				}
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationNoopQueryCountTotal",
		"vreplication query noop counts aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				for _, count := range ct.blpStats.NoopQueryCount.Counts() {
					result += count
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationTrxQueryBatchCount",
		"vreplication vplayer transaction query batch counts per type per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "commit_or_not"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for label, count := range ct.blpStats.TrxQueryBatchCount.Counts() {
					if label == "" {
						continue
					}
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+label] = count
				}
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationTrxQueryBatchCountTotal",
		"vreplication vplayer transaction query batch counts aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				for _, count := range ct.blpStats.TrxQueryBatchCount.Counts() {
					result += count
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationCopyRowCount",
		"vreplication rows copied in copy phase per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)] = ct.blpStats.CopyRowCount.Get()
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationCopyRowCountTotal",
		"vreplication rows copied in copy phase aggregated across all streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				result += ct.blpStats.CopyRowCount.Get()
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationCopyLoopCount",
		"Number of times the copy phase looped per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)] = ct.blpStats.CopyLoopCount.Get()
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationCopyLoopCountTotal",
		"Number of times the copy phase looped aggregated across streams",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := int64(0)
			for _, ct := range st.controllers {
				result += ct.blpStats.CopyLoopCount.Get()
			}
			return result
		})
	stats.NewCountersFuncWithMultiLabels(
		"VReplicationErrors",
		"Errors during vreplication",
		[]string{"workflow", "id", "type"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64)
			for _, ct := range st.controllers {
				for key, val := range ct.blpStats.ErrorCounts.Counts() {
					result[fmt.Sprintf("%s.%d.%s", ct.workflow, ct.id, key)] = val
				}
			}
			return result
		})
	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationHeartbeat",
		"Time when last heartbeat was received from a vstreamer",
		[]string{"source_keyspace", "source_shard", "workflow", "time"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)] = ct.blpStats.Heartbeat()
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationTableCopyRowCounts",
		"vreplication rows copied in copy phase per table per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "table"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for table, count := range ct.blpStats.TableCopyRowCounts.Counts() {
					if table == "" {
						continue
					}
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+table] = count
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VReplicationTableCopyTimings",
		"vreplication copy phase timings per table per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "counts", "table"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for table, t := range ct.blpStats.TableCopyTimings.Histograms() {
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+table] = t.Total()
				}
			}
			return result
		})
	stats.NewCountersFuncWithMultiLabels(
		"VReplicationPartialQueryCount",
		"count of partial queries per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "type"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for typ, t := range ct.blpStats.PartialQueryCount.Counts() {
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+typ] = t
				}
			}
			return result
		})
	stats.NewCountersFuncWithMultiLabels(
		"VReplicationPartialQueryCacheSize",
		"cache size for partial queries per stream",
		[]string{"source_keyspace", "source_shard", "workflow", "type"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for typ, t := range ct.blpStats.PartialQueryCacheSize.Counts() {
					result[ct.source.Keyspace+"."+ct.source.Shard+"."+ct.workflow+"."+fmt.Sprintf("%v", ct.id)+"."+typ] = t
				}
			}
			return result
		})

	stats.NewCounterFunc(
		"VReplicationThrottledCountTotal",
		"The total number of times that vreplication has been throttled",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return st.ThrottledCount.Get()
		})
	stats.NewCountersFuncWithMultiLabels(
		"VReplicationThrottledCounts",
		"The number of times vreplication was throttled by workflow, id, throttler (trx or tablet), and the sub-component that was throttled",
		[]string{"workflow", "id", "throttler", "component"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64)
			for _, ct := range st.controllers {
				for key, val := range ct.blpStats.ThrottledCounts.Counts() {
					result[fmt.Sprintf("%s.%d.%s", ct.workflow, ct.id, key)] = val
				}
			}
			return result
		})
}

func (st *vrStats) numControllers() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return int64(len(st.controllers))
}

func (st *vrStats) maxReplicationLagSeconds() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	max := int64(0)
	for _, ct := range st.controllers {
		if cur := ct.blpStats.ReplicationLagSeconds.Load(); cur > max {
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
		status.Controllers[i] = &ControllerStatus{
			Index:                 ct.id,
			Source:                ct.source.String(),
			StopPosition:          ct.stopPos,
			LastPosition:          ct.blpStats.LastPosition().String(),
			Heartbeat:             ct.blpStats.Heartbeat(),
			ReplicationLagSeconds: ct.blpStats.ReplicationLagSeconds.Load(),
			Counts:                ct.blpStats.Timings.Counts(),
			Rates:                 ct.blpStats.Rates.Get(),
			SourceTablet:          ct.sourceTablet.Load().(*topodatapb.TabletAlias),
			Messages:              ct.blpStats.MessageHistory(),
			QueryCounts:           ct.blpStats.QueryCount.Counts(),
			BulkQueryCounts:       ct.blpStats.BulkQueryCount.Counts(),
			TrxQueryBatchCounts:   ct.blpStats.TrxQueryBatchCount.Counts(),
			PhaseTimings:          ct.blpStats.PhaseTimings.Counts(),
			CopyRowCount:          ct.blpStats.CopyRowCount.Get(),
			CopyLoopCount:         ct.blpStats.CopyLoopCount.Get(),
			NoopQueryCounts:       ct.blpStats.NoopQueryCount.Counts(),
			TableCopyTimings:      ct.blpStats.TableCopyTimings.Counts(),
		}
		state := ct.blpStats.State.Load()
		if state != nil {
			status.Controllers[i].State = state.(string)
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
	Index                 int32
	Source                string
	SourceShard           string
	StopPosition          string
	LastPosition          string
	Heartbeat             int64
	ReplicationLagSeconds int64
	Counts                map[string]int64
	Rates                 map[string][]float64
	State                 string
	SourceTablet          *topodatapb.TabletAlias
	Messages              []string
	QueryCounts           map[string]int64
	BulkQueryCounts       map[string]int64
	TrxQueryBatchCounts   map[string]int64
	PhaseTimings          map[string]int64
	CopyRowCount          int64
	CopyLoopCount         int64
	NoopQueryCounts       map[string]int64
	TableCopyTimings      map[string]int64
}

const vreplicationTemplate = `
{{if .IsOpen}}VReplication state: Open</br>
<table>
  <tr>
    <th>Index</th>
    <th>Source</th>
    <th>Source Tablet</th>
    <th>State</th>
    <th>Stop Position</th>
    <th>Last Position</th>
    <th>VReplication Lag</th>
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
      <td>{{.ReplicationLagSeconds}}</td>
      <td>{{range $key, $value := .Counts}}<b>{{$key}}</b>: {{$value}}<br>{{end}}</td>
      <td>{{range $key, $values := .Rates}}<b>{{$key}}</b>: {{range $values}}{{.}} {{end}}<br>{{end}}</td>
      <td>{{range $index, $value := .Messages}}{{$value}}<br>{{end}}</td>
    </tr>{{end}}
<div id="vreplication_qps_chart" style="height: 500px; width: 900px">QPS All Streams </div>

<script src="https://www.gstatic.com/charts/loader.js"></script>
<script type="text/javascript">

function drawVReplicationQPSChart() {
  var div = document.getElementById('vreplication_qps_chart')
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

  const redraw = () => fetch(vars_url)
  .then(async (response) => {
	const input_data = await response.json();
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

  redraw();

  // redraw every 2.5 seconds.
  window.setInterval(redraw, 2500);
}
google.setOnLoadCallback(drawVReplicationQPSChart);
</script>
</table>{{else}}VReplication is closed.{{end}}
`
