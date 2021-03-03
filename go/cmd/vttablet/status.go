/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/servenv"
	_ "vitess.io/vitess/go/vt/status"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
)

var (
	// tabletTemplate contains the style sheet and the tablet itself.
	// This template is a slight duplicate of the one in go/vt/vttablet/tabletserver/status.go.
	tabletTemplate = `
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
      Alias: {{github_com_vitessio_vitess_vtctld_tablet .Tablet.AliasString}}<br>
      Keyspace: {{github_com_vitessio_vitess_vtctld_keyspace .Tablet.Keyspace}} Shard: {{github_com_vitessio_vitess_vtctld_shard .Tablet.Keyspace .Tablet.Shard}} Tablet Type: {{.Tablet.Type}}<br>
      SrvKeyspace: {{github_com_vitessio_vitess_vtctld_srv_keyspace .Tablet.Alias.Cell .Tablet.Keyspace}}<br>
      Replication graph: {{github_com_vitessio_vitess_vtctld_replication .Tablet.Alias.Cell .Tablet.Keyspace .Tablet.Shard}}<br>
      {{if .BlacklistedTables}}
        BlacklistedTables: {{range .BlacklistedTables}}{{.}} {{end}}<br>
      {{end}}
    </td>
    <td width="25%" border="">
      <a href="/schemaz">Schema</a></br>
      <a href="/debug/tablet_plans">Schema&nbsp;Query&nbsp;Plans</a></br>
      <a href="/debug/query_stats">Schema&nbsp;Query&nbsp;Stats</a></br>
      <a href="/queryz">Query&nbsp;Stats</a></br>
    </td>
    <td width="25%" border="">
      <a href="/debug/consolidations">Consolidations</a></br>
      <a href="/querylogz">Current&nbsp;Query&nbsp;Log</a></br>
      <a href="/txlogz">Current&nbsp;Transaction&nbsp;Log</a></br>
      <a href="/twopcz">In-flight&nbsp;2PC&nbsp;Transactions</a></br>
    </td>
    <td width="25%" border="">
      <a href="/healthz">Health Check</a></br>
      <a href="/debug/health">Query Service Health Check</a></br>
      <a href="/livequeryz/">Real-time Queries</a></br>
      <a href="/debug/status_details">JSON Status Details</a></br>
      <a href="/debug/env">View/Change Environment variables</a></br>
    </td>
  </tr>
</table>
`
)

func addStatusParts(qsc tabletserver.Controller) {
	servenv.AddStatusPart("Tablet", tabletTemplate, func() interface{} {
		return map[string]interface{}{
			"Tablet":            topo.NewTabletInfo(tm.Tablet(), nil),
			"BlacklistedTables": tm.BlacklistedTables(),
		}
	})
	qsc.AddStatusPart()
	vreplication.AddStatusPart()
}
