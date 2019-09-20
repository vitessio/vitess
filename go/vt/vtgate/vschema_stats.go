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

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"sort"

	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// VSchemaStats contains a rollup of the VSchema stats.
type VSchemaStats struct {
	Error     string
	Keyspaces []*VSchemaKeyspaceStats
}

// VSchemaKeyspaceStats contains a rollup of the VSchema stats for a keyspace.
// It is used to display a table with the information in the status page.
type VSchemaKeyspaceStats struct {
	Keyspace    string
	Sharded     bool
	TableCount  int
	VindexCount int
	Error       string
}

// NewVSchemaStats returns a new VSchemaStats from a VSchema.
func NewVSchemaStats(vschema *vindexes.VSchema, errorMessage string) *VSchemaStats {
	stats := &VSchemaStats{
		Error:     errorMessage,
		Keyspaces: make([]*VSchemaKeyspaceStats, 0, len(vschema.Keyspaces)),
	}
	for n, k := range vschema.Keyspaces {
		s := &VSchemaKeyspaceStats{
			Keyspace: n,
		}
		if k.Keyspace != nil {
			s.Sharded = k.Keyspace.Sharded
			s.TableCount += len(k.Tables)
			for _, t := range k.Tables {
				s.VindexCount += len(t.ColumnVindexes) + len(t.Ordered) + len(t.Owned)
			}
		}
		if k.Error != nil {
			s.Error = k.Error.Error()
		}
		stats.Keyspaces = append(stats.Keyspaces, s)
	}
	sort.Slice(stats.Keyspaces, func(i, j int) bool { return stats.Keyspaces[i].Keyspace < stats.Keyspaces[j].Keyspace })

	return stats
}

const (
	// VSchemaTemplate is the HTML template to display VSchemaStats.
	VSchemaTemplate = `
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
    <th colspan="5">VSchema Cache <i><a href="/debug/vschema">in JSON</a></i></th>
  </tr>
{{if .Error}}
  <tr>
    <th>Error</th>
    <td colspan="3">{{$.Error}}</td>
  </tr>
  <tr>
    <td>colspan="4"></td>
  </tr>
{{end}}
  <tr>
    <th>Keyspace</th>
    <th>Sharded</th>
    <th>Table Count</th>
    <th>Vindex Count</th>
    <th>Error</th>
  </tr>
{{range $i, $ks := .Keyspaces}}  <tr>
    <td>{{$ks.Keyspace}}</td>
    <td>{{if $ks.Sharded}}Yes{{else}}No{{end}}</td>
    <td>{{$ks.TableCount}}</td>
    <td>{{$ks.VindexCount}}</td>
    <td style="color:red">{{$ks.Error}}</td>
  </tr>{{end}}
</table>
`
)
