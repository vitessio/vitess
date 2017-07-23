/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"html/template"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
)

var (
	// tabletTemplate contains the style sheet and the tablet itself.
	tabletTemplate = `
<style>
  table {
    width: 100%;
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
      Alias: {{github_com_youtube_vitess_vtctld_tablet .Tablet.AliasString}}<br>
      Keyspace: {{github_com_youtube_vitess_vtctld_keyspace .Tablet.Keyspace}} Shard: {{github_com_youtube_vitess_vtctld_shard .Tablet.Keyspace .Tablet.Shard}} Tablet Type: {{.Tablet.Type}}<br>
      SrvKeyspace: {{github_com_youtube_vitess_vtctld_srv_keyspace .Tablet.Alias.Cell .Tablet.Keyspace}}<br>
      Replication graph: {{github_com_youtube_vitess_vtctld_replication .Tablet.Alias.Cell .Tablet.Keyspace .Tablet.Shard}}<br>
      {{if .BlacklistedTables}}
        BlacklistedTables: {{range .BlacklistedTables}}{{.}} {{end}}<br>
      {{end}}
      {{if .DisallowQueryService}}
        Query Service disabled: {{.DisallowQueryService}}<br>
      {{end}}
      {{if .DisableUpdateStream}}
        Update Stream disabled<br>
      {{end}}
    </td>
    <td width="25%" border="">
      <a href="/schemaz">Schema</a></br>
      <a href="/debug/tablet_plans">Schema&nbsp;Query&nbsp;Plans</a></br>
      <a href="/debug/query_stats">Schema&nbsp;Query&nbsp;Stats</a></br>
      <a href="/debug/table_stats">Schema&nbsp;Table&nbsp;Stats</a></br>
    </td>
    <td width="25%" border="">
      <a href="/queryz">Query&nbsp;Stats</a></br>
      <a href="/streamqueryz">Streaming&nbsp;Query&nbsp;Stats</a></br>
      <a href="/debug/consolidations">Consolidations</a></br>
      <a href="/querylogz">Current&nbsp;Query&nbsp;Log</a></br>
      <a href="/txlogz">Current&nbsp;Transaction&nbsp;Log</a></br>
      <a href="/twopcz">In-flight&nbsp;2PC&nbsp;Transactions</a></br>
    </td>
    <td width="25%" border="">
      <a href="/healthz">Health Check</a></br>
      <a href="/debug/health">Query Service Health Check</a></br>
      <a href="/streamqueryz">Current Stream Queries</a></br>
    </td>
  </tr>
</table>
`

	// healthTemplate is just about the tablet health
	healthTemplate = `
<div style="font-size: x-large">Current status: <span style="padding-left: 0.5em; padding-right: 0.5em; padding-bottom: 0.5ex; padding-top: 0.5ex;" class="{{.CurrentClass}}">{{.CurrentHTML}}</span></div>
<p>Polling health information from {{github_com_youtube_vitess_health_html_name}}. ({{.Config}})</p>
<h2>Health History</h2>
<table>
  <tr>
    <th class="time">Time</th>
    <th>Healthcheck Result</th>
  </tr>
  {{range .Records}}
  <tr class="{{.Class}}">
    <td class="time">{{.Time.Format "Jan 2, 2006 at 15:04:05 (MST)"}}</td>
    <td>{{.HTML}}</td>
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
`

	// binlogTemplate is about the binlog players
	binlogTemplate = `
{{if .Controllers}}
Binlog player state: {{.State}}</br>
<table>
  <tr>
    <th>Index</th>
    <th>SourceShard</th>
    <th>State</th>
    <th>StopPosition</th>
    <th>LastPosition</th>
    <th>SecondsBehindMaster</th>
    <th>Counts</th>
    <th>Rates</th>
    <th>Last Error</th>
  </tr>
  {{range .Controllers}}
    <tr>
      <td>{{.Index}}</td>
      <td>{{.SourceShardAsHTML}}</td>
      <td>{{.State}}
        {{if eq .State "Running"}}
          {{if .SourceTabletAlias}}
            (from {{github_com_youtube_vitess_vtctld_tablet .SourceTabletAlias}})
          {{else}}
            (picking source tablet)
          {{end}}
        {{end}}</td>
      <td>{{if .StopPosition}}{{.StopPosition}}{{end}}</td>
      <td>{{.LastPosition}}</td>
      <td>{{.SecondsBehindMaster}}</td>
      <td>{{range $key, $value := .Counts}}<b>{{$key}}</b>: {{$value}}<br>{{end}}</td>
      <td>{{range $key, $values := .Rates}}<b>{{$key}}</b>: {{range $values}}{{.}} {{end}}<br>{{end}}</td>
      <td>{{.LastError}}</td>
    </tr>
  {{end}}
</table>
{{else}}
No binlog player is running.
{{end}}
`
)

type healthStatus struct {
	Records []interface{}
	Config  template.HTML
	current *tabletmanager.HealthRecord
}

func (hs *healthStatus) CurrentClass() string {
	if hs.current != nil {
		return hs.current.Class()
	}
	return "unknown"
}

func (hs *healthStatus) CurrentHTML() template.HTML {
	if hs.current != nil {
		return hs.current.HTML()
	}
	return template.HTML("unknown")
}

func healthHTMLName() template.HTML {
	return health.DefaultAggregator.HTMLName()
}

// For use by plugins which wish to avoid racing when registering status page parts.
var onStatusRegistered func()

func addStatusParts(qsc tabletserver.Controller) {
	servenv.AddStatusPart("Tablet", tabletTemplate, func() interface{} {
		return map[string]interface{}{
			"Tablet":               topo.NewTabletInfo(agent.Tablet(), -1),
			"BlacklistedTables":    agent.BlacklistedTables(),
			"DisallowQueryService": agent.DisallowQueryService(),
			"DisableUpdateStream":  !agent.EnableUpdateStream(),
		}
	})
	servenv.AddStatusFuncs(template.FuncMap{
		"github_com_youtube_vitess_health_html_name": healthHTMLName,
	})
	servenv.AddStatusPart("Health", healthTemplate, func() interface{} {
		latest, _ := agent.History.Latest().(*tabletmanager.HealthRecord)
		return &healthStatus{
			Records: agent.History.Records(),
			Config:  tabletmanager.ConfigHTML(),
			current: latest,
		}
	})
	qsc.AddStatusPart()
	servenv.AddStatusPart("Binlog Player", binlogTemplate, func() interface{} {
		return agent.BinlogPlayerMap.Status()
	})
	if onStatusRegistered != nil {
		onStatusRegistered()
	}
}
