/*
Copyright 2021 The Vitess Authors.

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

package srvtopo

import (
	"context"
	"sort"
	"time"

	"github.com/google/safehtml"
	"github.com/google/safehtml/template"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TopoTemplate is the HTML to use to display the
// ResilientServerCacheStatus object
const TopoTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table class="refreshRequired">
  <tr>
    <th colspan="4">SrvKeyspace Names Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>SrvKeyspace Names</th>
    <th>TTL</th>
    <th>Error</th>
  </tr>
  {{range $i, $skn := .SrvKeyspaceNames}}
  <tr>
    <td>{{$skn.Cell}}</td>
    <td>{{range $j, $value := $skn.Value}}{{$value}}&nbsp;{{end}}</td>
    <td>{{github_com_vitessio_vitess_srvtopo_ttl_time $skn.ExpirationTime}}</td>
    <td>{{if $skn.LastError}}({{github_com_vitessio_vitess_srvtopo_time_since $skn.LastQueryTime}}Ago) {{$skn.LastError}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table class="refreshRequired">
  <tr>
    <th colspan="5">SrvKeyspace Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>SrvKeyspace</th>
    <th>TTL</th>
    <th>Error</th>
  </tr>
  {{range $i, $sk := .SrvKeyspaces}}
  <tr>
    <td>{{$sk.Cell}}</td>
    <td>{{$sk.Keyspace}}</td>
    <td>{{$sk.StatusAsHTML}}</td>
    <td>{{github_com_vitessio_vitess_srvtopo_ttl_time $sk.ExpirationTime}}</td>
    <td>{{if $sk.LastError}}({{github_com_vitessio_vitess_srvtopo_time_since $sk.LastErrorTime}} Ago) {{$sk.LastError}}{{end}}</td>
  </tr>
  {{end}}
</table>
`

// The next few structures and methods are used to get a displayable
// version of the cache in a status page.

// SrvKeyspaceNamesCacheStatus is the current value for SrvKeyspaceNames
type SrvKeyspaceNamesCacheStatus struct {
	Cell           string
	Value          []string
	ExpirationTime time.Time
	LastQueryTime  time.Time
	LastError      error
	LastErrorCtx   context.Context
}

// SrvKeyspaceNamesCacheStatusList is used for sorting
type SrvKeyspaceNamesCacheStatusList []*SrvKeyspaceNamesCacheStatus

// Len is part of sort.Interface
func (skncsl SrvKeyspaceNamesCacheStatusList) Len() int {
	return len(skncsl)
}

// Less is part of sort.Interface
func (skncsl SrvKeyspaceNamesCacheStatusList) Less(i, j int) bool {
	return skncsl[i].Cell < skncsl[j].Cell
}

// Swap is part of sort.Interface
func (skncsl SrvKeyspaceNamesCacheStatusList) Swap(i, j int) {
	skncsl[i], skncsl[j] = skncsl[j], skncsl[i]
}

// SrvKeyspaceCacheStatus is the current value for a SrvKeyspace object
type SrvKeyspaceCacheStatus struct {
	Cell           string
	Keyspace       string
	Value          *topodatapb.SrvKeyspace
	ExpirationTime time.Time
	LastErrorTime  time.Time
	LastError      error
}

var noData = safehtml.HTMLEscaped("No Data")
var partitions = template.Must(template.New("partitions").Parse(`
<b>Partitions:</b><br>
{{ range .Partitions }}
&nbsp;<b>{{ .ServedType }}:</b>
{{ range .ShardReferences }}
&nbsp;{{ .Name }}
{{ end }}
<br>
{{ end }}
`))

// StatusAsHTML returns an HTML version of our status.
// It works best if there is data in the cache.
func (st *SrvKeyspaceCacheStatus) StatusAsHTML() safehtml.HTML {
	if st.Value == nil {
		return noData
	}
	html, err := partitions.ExecuteToHTML(st.Value)
	if err != nil {
		panic(err)
	}
	return html
}

// SrvKeyspaceCacheStatusList is used for sorting
type SrvKeyspaceCacheStatusList []*SrvKeyspaceCacheStatus

// Len is part of sort.Interface
func (skcsl SrvKeyspaceCacheStatusList) Len() int {
	return len(skcsl)
}

// Less is part of sort.Interface
func (skcsl SrvKeyspaceCacheStatusList) Less(i, j int) bool {
	return skcsl[i].Cell+"."+skcsl[i].Keyspace <
		skcsl[j].Cell+"."+skcsl[j].Keyspace
}

// Swap is part of sort.Interface
func (skcsl SrvKeyspaceCacheStatusList) Swap(i, j int) {
	skcsl[i], skcsl[j] = skcsl[j], skcsl[i]
}

// ResilientServerCacheStatus has the full status of the cache
type ResilientServerCacheStatus struct {
	SrvKeyspaceNames SrvKeyspaceNamesCacheStatusList
	SrvKeyspaces     SrvKeyspaceCacheStatusList
}

// CacheStatus returns a displayable version of the cache
func (server *ResilientServer) CacheStatus() *ResilientServerCacheStatus {
	result := &ResilientServerCacheStatus{
		SrvKeyspaceNames: server.srvKeyspaceNamesCacheStatus(),
		SrvKeyspaces:     server.srvKeyspaceCacheStatus(),
	}
	sort.Sort(result.SrvKeyspaceNames)
	sort.Sort(result.SrvKeyspaces)
	return result
}

var expired = template.MustParseAndExecuteToHTML("<b>Expired</b>")

// Returns the ttl for the cached entry or "Expired" if it is in the past
func ttlTime(expirationTime time.Time) safehtml.HTML {
	ttl := time.Until(expirationTime).Round(time.Second)
	if ttl < 0 {
		return expired
	}
	return safehtml.HTMLEscaped(ttl.String())
}

func timeSince(t time.Time) safehtml.HTML {
	return safehtml.HTMLEscaped(time.Since(t).Round(time.Second).String())
}

// StatusFuncs is required for CacheStatus) to work properly.
// We don't register them inside servenv directly so we don't introduce
// a dependency here.
var StatusFuncs = template.FuncMap{
	"github_com_vitessio_vitess_srvtopo_ttl_time":   ttlTime,
	"github_com_vitessio_vitess_srvtopo_time_since": timeSince,
}
