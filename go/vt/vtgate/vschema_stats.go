// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"sort"

	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// VSchemaStats contains a rollup of the VSchema stats.
type VSchemaStats struct {
	Error     string
	Keyspaces VSchemaKeyspaceStatsList
}

// VSchemaKeyspaceStats contains a rollup of the VSchema stats for a keyspace.
// It is used to display a table with the information in the status page.
type VSchemaKeyspaceStats struct {
	Keyspace    string
	Sharded     bool
	TableCount  int
	VindexCount int
}

// VSchemaKeyspaceStatsList is to sort VSchemaKeyspaceStats by keyspace.
type VSchemaKeyspaceStatsList []*VSchemaKeyspaceStats

// Len is part of sort.Interface
func (l VSchemaKeyspaceStatsList) Len() int {
	return len(l)
}

// Less is part of sort.Interface
func (l VSchemaKeyspaceStatsList) Less(i, j int) bool {
	return l[i].Keyspace < l[j].Keyspace
}

// Swap is part of sort.Interface
func (l VSchemaKeyspaceStatsList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
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
		stats.Keyspaces = append(stats.Keyspaces, s)
	}
	sort.Sort(stats.Keyspaces)

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
    <th colspan="4">VSchema{{if not .Error}} <i><a href="/debug/vschema">in JSON</a></i>{{end}}</th>
  </tr>
{{if .Error}}
  <tr>
    <th>Error</th>
    <td colspan="3">{{$.Error}}</td>
  </tr>
{{else}}
  <tr>
    <th>Keyspace</th>
    <th>Sharded</th>
    <th>Table Count</th>
    <th>Vindex Count</th>
  </tr>
{{range $i, $ks := .Keyspaces}}  <tr>
    <td>{{$ks.Keyspace}}</td>
    <td>{{if $ks.Sharded}}Yes{{else}}No{{end}}</td>
    <td>{{$ks.TableCount}}</td>
    <td>{{$ks.VindexCount}}</td>
  </tr>{{end}}
{{end}}
</table>
`
)
