package main

import (
	"html/template"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
)

var (
	healthTemplate = `
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
<div style="font-size: x-large">Current status: <span style="padding-left: 0.5em; padding-right: 0.5em; padding-bottom: 0.5ex; padding-top: 0.5ex;" class="{{.Current}}">{{.Current}}</span></div>
<p>Polling health information from {{github_com_youtube_vitess_health_html_name}}.</p>
<h2>History</h2>
<table>
  <tr>
    <th class="time">time</th>
    <th>state</th>
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

func healthHTMLName() string {
	return health.HTMLName()
}

func init() {
	servenv.OnRun(func() {
		if agent.IsRunningHealthCheck() {
			servenv.AddStatusFuncs(template.FuncMap{
				"github_com_youtube_vitess_health_html_name": healthHTMLName,
			})
			servenv.AddStatusPart("Health", healthTemplate, func() interface{} {
				return &healthStatus{Records: agent.History.Records()}
			})
		}
	})
}
