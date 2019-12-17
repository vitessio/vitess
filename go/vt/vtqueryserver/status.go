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

package vtqueryserver

import (
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
)

var (
	// proxyTemplate contains the style sheet and the tablet itself.
	proxyTemplate = `
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
      Target Keyspace: {{.Target.Keyspace}}<br>
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
      <a href="/debug/health">Query Service Health Check</a></br>
      <a href="/streamqueryz">Current Stream Queries</a></br>
    </td>
  </tr>
</table>
`
)

// For use by plugins which wish to avoid racing when registering status page parts.
var onStatusRegistered func()

func addStatusParts(qsc tabletserver.Controller) {
	servenv.AddStatusPart("Target", proxyTemplate, func() interface{} {
		return map[string]interface{}{
			"Target": target,
		}
	})
	qsc.AddStatusPart()
	if onStatusRegistered != nil {
		onStatusRegistered()
	}
}
