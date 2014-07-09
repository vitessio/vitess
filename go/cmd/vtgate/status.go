package main

import (
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/status"
)

var (
	topoTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.5rem;
  }
</style>
<table>
  <tr>
    <th colspan="2">SrvKeyspace Names Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>SrvKeyspace Names</th>
  </tr>
  {{range $i, $skn := .SrvKeyspaceNames}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $skn.Cell}}</td>
    <td>{{if $skn.LastError}}<b>{{$skn.LastError}}</b><br/>Client: {{$skn.LastErrorContext.HTML}}{{else}}{{range $j, $value := $skn.Value}}{{github_com_youtube_vitess_vtctld_srv_keyspace $skn.Cell $value}}&nbsp;{{end}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table>
  <tr>
    <th colspan="3">SrvKeyspace Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>SrvKeyspace</th>
  </tr>
  {{range $i, $sk := .SrvKeyspaces}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $sk.Cell}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_keyspace $sk.Cell $sk.Keyspace}}</td>
    <td>{{if $sk.LastError}}<b>{{$sk.LastError}}</b><br/>Client: {{$sk.LastErrorContext.HTML}}{{else}}{{$sk.StatusAsHTML}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table>
  <tr>
    <th colspan="5">EndPoints Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>Shard</th>
    <th>TabletType</th>
    <th>EndPoints</th>
  </tr>
  {{range $i, $ep := .EndPoints}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $ep.Cell}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_keyspace $ep.Cell $ep.Keyspace}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_shard $ep.Cell $ep.Keyspace $ep.Shard}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_type $ep.Cell $ep.Keyspace $ep.Shard $ep.TabletType}}</td>
    <td>{{if $ep.LastError}}<b>{{$ep.LastError}}</b><br/>Client: {{$ep.LastErrorContext.HTML}}{{else}}{{$ep.StatusAsHTML}}{{end}}</td>
  </tr>
  {{end}}
</table>
<small>This is just a cache, so some data may not be visible here yet.</small>
`
)

func init() {
	servenv.OnRun(func() {
		servenv.AddStatusPart("Topology Cache", topoTemplate, func() interface{} {
			return resilientSrvTopoServer.CacheStatus()
		})
	})
}
