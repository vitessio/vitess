/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
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
	"slices"
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type (
	topologyTreeData struct {
		ClusterID string
		Cells     []*topologyCell
		Keyspaces []*topologyKeyspace
	}

	topologyCell struct {
		Name        string
		TabletCount int
	}

	topologyKeyspace struct {
		Name   string
		Shards []*topologyShard
	}

	topologyShard struct {
		Name         string
		PrimaryAlias string
		Tablets      []string
	}
)

// topologyTree renders a read-only hierarchy of a cluster's topology: the
// cells with tablet counts, and each keyspace's shards with per-shard tablet
// links. It replaces the SPA's interactive d3 graph with a printable,
// linkable page.
func (s *Server) topologyTree(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster_id")

	keyspacesResp, err := s.api.GetKeyspaces(r.Context(), &vtadminpb.GetKeyspacesRequest{
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Topology", err)
		return
	}

	tabletsResp, err := s.api.GetTablets(r.Context(), &vtadminpb.GetTabletsRequest{
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Topology", err)
		return
	}

	data := buildTopologyTree(clusterID, keyspacesResp.GetKeyspaces(), tabletsResp.GetTablets())

	s.render(w, r, http.StatusOK, "topology_tree.html", PageData{
		Title:  "Topology: " + clusterID,
		Active: "topology",
		Data:   data,
	})
}

func buildTopologyTree(clusterID string, keyspaces []*vtadminpb.Keyspace, tablets []*vtadminpb.Tablet) topologyTreeData {
	data := topologyTreeData{ClusterID: clusterID}

	// Cells, with tablet counts per cell.
	cellTablets := map[string]int{}
	for _, t := range tablets {
		cellTablets[t.GetTablet().GetAlias().GetCell()]++
	}
	for cell := range cellTablets {
		data.Cells = append(data.Cells, &topologyCell{Name: cell, TabletCount: cellTablets[cell]})
	}
	slices.SortFunc(data.Cells, func(a, b *topologyCell) int { return strings.Compare(a.Name, b.Name) })

	tabletsByKeyspaceShard := make(map[string]map[string][]*vtadminpb.Tablet)
	for _, t := range tablets {
		tablet := t.GetTablet()
		if _, ok := tabletsByKeyspaceShard[tablet.GetKeyspace()]; !ok {
			tabletsByKeyspaceShard[tablet.GetKeyspace()] = make(map[string][]*vtadminpb.Tablet)
		}
		tabletsByKeyspaceShard[tablet.GetKeyspace()][tablet.GetShard()] = append(
			tabletsByKeyspaceShard[tablet.GetKeyspace()][tablet.GetShard()], t,
		)
	}

	// Keyspaces with per-shard tablet alias lists.
	for _, ks := range keyspaces {
		tk := &topologyKeyspace{Name: ks.GetKeyspace().GetName()}
		for _, shardName := range sortedShardNames(ks) {
			shard := &topologyShard{Name: shardName}
			for _, t := range tabletsByKeyspaceShard[ks.GetKeyspace().GetName()][shardName] {
				alias := tabletAlias(t.GetTablet().GetAlias())
				shard.Tablets = append(shard.Tablets, alias)
				if t.GetTablet().GetType() == topodatapb.TabletType_PRIMARY {
					shard.PrimaryAlias = alias
				}
			}
			slices.Sort(shard.Tablets)
			tk.Shards = append(tk.Shards, shard)
		}
		data.Keyspaces = append(data.Keyspaces, tk)
	}
	slices.SortFunc(data.Keyspaces, func(a, b *topologyKeyspace) int { return strings.Compare(a.Name, b.Name) })

	return data
}
