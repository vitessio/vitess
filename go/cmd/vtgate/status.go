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
    <td>{{$skn.Cell}}</td>
    <td>{{if $skn.LastError}}<b>{{$skn.LastError}}</b>{{else}}{{range $j, $value := $skn.Value}}{{$value}}&nbsp;{{end}}{{end}}</td>
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
  {{range $i, $skn := .SrvKeyspaces}}
  <tr>
    <td>{{$skn.Cell}}</td>
    <td>{{$skn.Keyspace}}</td>
    <td>{{if $skn.LastError}}<b>{{$skn.LastError}}</b>{{else}}{{$skn.StatusAsHTML}}{{end}}</td>
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
  {{range $i, $skn := .EndPoints}}
  <tr>
    <td>{{$skn.Cell}}</td>
    <td>{{$skn.Keyspace}}</td>
    <td>{{$skn.Shard}}</td>
    <td>{{$skn.TabletType}}</td>
    <td>{{if $skn.LastError}}<b>{{$skn.LastError}}</b>{{else}}{{$skn.StatusAsHTML}}{{end}}</td>
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
