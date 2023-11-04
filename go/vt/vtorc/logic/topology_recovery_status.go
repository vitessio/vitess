/*
Copyright 2022 The Vitess Authors.

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

package logic

// TopologyRecoveriesTemplate is the HTML to use to display the
// topology recovery steps list object
const TopologyRecoveriesTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table>
  <tr>
    <th colspan="4">Recent Recoveries Performed</th>
  </tr>
  <tr>
    <th>Recovery ID</th>
    <th>Failure Type</th>
    <th>Tablet Alias</th>
    <th>Timestamp</th>
  </tr>
  {{range $i, $recovery := .}}
  <tr>
    <td>{{$recovery.ID}}</td>
    <td>{{$recovery.AnalysisEntry.Analysis}}</td>
    <td>{{$recovery.AnalysisEntry.AnalyzedInstanceAlias}}</td>
    <td>{{$recovery.RecoveryStartTimestamp}}</td>
  </tr>
  {{end}}
</table>
`
