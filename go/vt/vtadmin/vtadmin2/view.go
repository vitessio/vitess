/*
Copyright 2026 The Vitess Authors.

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

package vtadmin2

import (
	"net/http"
	"net/url"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type formOptions struct {
	Clusters         []*vtadminpb.Cluster
	Keyspaces        []*vtadminpb.Keyspace
	SelectedCluster  string
	SelectedKeyspace string
}

func queryValues(r *http.Request, name string) []string { return r.URL.Query()[name] }

func queryValue(r *http.Request, name string) string { return r.URL.Query().Get(name) }

func pathEscape(value string) string { return url.PathEscape(value) }

func urlQueryEscape(value string) string { return url.QueryEscape(value) }

func externalURL(value string) string {
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		return value
	}
	return "http://" + value
}

func selectedClusterID(clusters []*vtadminpb.Cluster, requested string) string {
	if requested != "" {
		return requested
	}
	if len(clusters) == 0 {
		return ""
	}
	return clusters[0].GetId()
}

func selectedKeyspaceName(keyspaces []*vtadminpb.Keyspace, selectedCluster string, requested string) string {
	if requested != "" {
		return requested
	}
	for _, ks := range keyspaces {
		if selectedCluster == "" || clusterID(ks.GetCluster()) == selectedCluster {
			return keyspaceName(ks)
		}
	}
	return ""
}

func protoJSON(v any) string {
	msg, ok := v.(proto.Message)
	if !ok || msg == nil {
		return ""
	}

	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func keyspaceName(ks *vtadminpb.Keyspace) string {
	if ks == nil || ks.GetKeyspace() == nil {
		return ""
	}
	return ks.GetKeyspace().GetName()
}

func clusterID(c *vtadminpb.Cluster) string {
	if c == nil {
		return ""
	}
	return c.GetId()
}

func sortedShardNames(ks *vtadminpb.Keyspace) []string {
	if ks == nil {
		return nil
	}
	names := make([]string, 0, len(ks.GetShards()))
	for name := range ks.GetShards() {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func tabletAlias(alias *topodatapb.TabletAlias) string {
	return topoproto.TabletAliasString(alias)
}

func schemaTableCount(schema *vtadminpb.Schema) int {
	if schema == nil {
		return 0
	}
	return len(schema.GetTableDefinitions())
}
