package main

import (
	"html/template"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	// tabletTemplate contains the style sheet and the tablet itself.
	tabletTemplate = `
<style>
  table {
    width: 70%;
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
Alias: {{github_com_youtube_vitess_vtctld_tablet .Alias.String}}<br>
Keyspace: {{github_com_youtube_vitess_vtctld_keyspace .Keyspace}} Shard: {{github_com_youtube_vitess_vtctld_shard .Keyspace .Shard}}<br>
Serving graph: {{github_com_youtube_vitess_vtctld_srv_keyspace .Alias.Cell .Keyspace}} {{github_com_youtube_vitess_vtctld_srv_shard .Alias.Cell .Keyspace .Shard}} {{github_com_youtube_vitess_vtctld_srv_type .Alias.Cell .Keyspace .Shard .Type}}<br>
Replication graph: {{github_com_youtube_vitess_vtctld_replication .Alias.Cell .Keyspace .Shard}}<br>
State: {{.State}}<br>
`

	// healthTemplate is just about the tablet health
	healthTemplate = `
<div style="font-size: x-large">Current status: <span style="padding-left: 0.5em; padding-right: 0.5em; padding-bottom: 0.5ex; padding-top: 0.5ex;" class="{{.Current}}">{{.Current}}</span></div>
<p>Polling health information from {{github_com_youtube_vitess_health_html_name}}.</p>
<h2>History</h2>
<table>
  <tr>
    <th class="time">Time</th>
    <th>State</th>
  </tr>
  {{range .Records}}
  <tr class="{{.Class}}">
    <td class="time">{{.Time.Format "Jan 2, 2006 at 15:04:05 (MST)"}}</td>
    <td>
    {{ if eq "unhealthy" .Class}}
      unhealthy: {{.Error}}
    {{else if eq "unhappy" .Class}}
      unhappy (reasons: {{range $key, $value := .Result}}{{$key}}: {{$value}} {{end}})
    {{else}}
      healthy
    {{end}}
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
    <th>StopAtGroupId</th>
    <th>LastGroupId</th>
    <th>Counts</th>
    <th>Rates</th>
  </tr>
  {{range .Controllers}}
    <tr>
      <td>{{.Index}}</td>
      <td>{{.SourceShard.AsHTML}}</td>
      <td>{{if .StopAtGroupId}}{{.StopAtGroupId}}{{end}}</td>
      <td>{{.LastGroupId}}</td>
      <td>{{range $key, $value := .Counts}}<b>{{$key}}</b>: {{$value}}<br>{{end}}</td>
      <td>{{range $key, $values := .Rates}}<b>{{$key}}</b>: {{range $values}}{{.}} {{end}}<br>{{end}}</td>
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
}

func (hs *healthStatus) Current() string {
	if len(hs.Records) > 0 {
		return hs.Records[0].(*tabletmanager.HealthRecord).Class()
	}
	return "unknown"
}

func healthHTMLName() template.HTML {
	return health.HTMLName()
}

func init() {
	servenv.OnRun(func() {
		servenv.AddStatusPart("Tablet", tabletTemplate, func() interface{} {
			return agent.Tablet()
		})
		if agent.IsRunningHealthCheck() {
			servenv.AddStatusFuncs(template.FuncMap{
				"github_com_youtube_vitess_health_html_name": healthHTMLName,
			})
			servenv.AddStatusPart("Health", healthTemplate, func() interface{} {
				return &healthStatus{Records: agent.History.Records()}
			})
		}
		tabletserver.AddStatusPart()
		servenv.AddStatusPart("Binlog Player", binlogTemplate, func() interface{} {
			return agent.BinlogPlayerMap.Status()
		})
	})
}
