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
	"net/url"
	"slices"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

type (
	shardDetailData struct {
		ClusterID string
		Keyspace  string
		Shard     string
		Tablets   []*vtadminpb.Tablet
		Positions *vtadminpb.ClusterShardReplicationPosition
	}

	shardFailoverOptions struct {
		NewPrimary          *topodatapb.TabletAlias
		WaitReplicasTimeout *vttimepb.Duration
		Planned             *vtctldatapb.PlannedReparentShardRequest
	}
)

func shardDetailPath(clusterID, keyspace, shard string) string {
	return "/keyspace/" + url.PathEscape(clusterID) + "/" + url.PathEscape(keyspace) + "/shard/" + url.PathEscape(shard)
}

func shardActionPath(clusterID, keyspace, shard, action string) string {
	return shardDetailPath(clusterID, keyspace, shard) + "/" + url.PathEscape(action)
}

func keyspaceDetailPath(clusterID, keyspace string) string {
	return "/keyspace/" + url.PathEscape(clusterID) + "/" + url.PathEscape(keyspace)
}

func keyspaceActionPath(clusterID, keyspace, action string) string {
	return keyspaceDetailPath(clusterID, keyspace) + "/" + url.PathEscape(action)
}

func (s *Server) shardDetail(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster_id")
	keyspace := r.PathValue("name")
	shard := r.PathValue("shard")

	_, err := s.api.GetKeyspace(r.Context(), &vtadminpb.GetKeyspaceRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
	})
	if err != nil {
		s.renderError(w, r, tabletErrorStatus(err), "Shard", err)
		return
	}

	tabletsResp, err := s.api.GetTablets(r.Context(), &vtadminpb.GetTabletsRequest{
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Shard", err)
		return
	}

	tablets := filterShardTablets(tabletsResp.GetTablets(), keyspace, shard)
	slices.SortFunc(tablets, func(a, b *vtadminpb.Tablet) int {
		return strings.Compare(topoproto.TabletAliasString(a.GetTablet().GetAlias()), topoproto.TabletAliasString(b.GetTablet().GetAlias()))
	})

	positionsResp, err := s.api.GetShardReplicationPositions(r.Context(), &vtadminpb.GetShardReplicationPositionsRequest{
		ClusterIds:     []string{clusterID},
		KeyspaceShards: []string{keyspace + "/" + shard},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Shard", err)
		return
	}

	s.render(w, r, http.StatusOK, "shard.html", PageData{
		Title:     keyspace + "/" + shard,
		Active:    "shards",
		NeedsCSRF: !s.opts.ReadOnly,
		Data: shardDetailData{
			ClusterID: clusterID,
			Keyspace:  keyspace,
			Shard:     shard,
			Tablets:   tablets,
			Positions: findShardPosition(positionsResp.GetReplicationPositions(), clusterID, keyspace, shard),
		},
	})
}

func filterShardTablets(tablets []*vtadminpb.Tablet, keyspace, shard string) []*vtadminpb.Tablet {
	filtered := make([]*vtadminpb.Tablet, 0, len(tablets))
	for _, tablet := range tablets {
		if tablet.GetTablet().GetKeyspace() == keyspace && tablet.GetTablet().GetShard() == shard {
			filtered = append(filtered, tablet)
		}
	}
	return filtered
}

func findShardPosition(positions []*vtadminpb.ClusterShardReplicationPosition, clusterID, keyspace, shard string) *vtadminpb.ClusterShardReplicationPosition {
	for _, pos := range positions {
		if pos.GetCluster().GetId() == clusterID && pos.GetKeyspace() == keyspace && pos.GetShard() == shard {
			return pos
		}
	}
	return nil
}

// beginShardAction performs the shared preflight for shard mutation handlers:
// read-only rejection, form parsing, and CSRF validation. It returns the
// parsed route values; ok is false when the preflight has already rendered an
// error response.
func (s *Server) beginShardAction(w http.ResponseWriter, r *http.Request, title string) (clusterID, keyspace, shard string, ok bool) {
	if !s.beginFormAction(w, r, title) {
		return "", "", "", false
	}
	return r.PathValue("cluster_id"), r.PathValue("name"), r.PathValue("shard"), true
}

func (s *Server) shardDelete(w http.ResponseWriter, r *http.Request) {
	const title = "Delete shard"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.DeleteShards(r.Context(), &vtadminpb.DeleteShardsRequest{
		ClusterId: clusterID,
		Options: &vtctldatapb.DeleteShardsRequest{
			Shards: []*vtctldatapb.Shard{{Keyspace: keyspace, Name: shard}},
		},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, "/keyspaces", Flash{
		Kind:    "success",
		Message: "deleted shard " + keyspace + "/" + shard,
	})
}

func (s *Server) shardReloadSchema(w http.ResponseWriter, r *http.Request) {
	const title = "Reload schema"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.ReloadSchemaShard(r.Context(), &vtadminpb.ReloadSchemaShardRequest{
		ClusterId:      clusterID,
		Keyspace:       keyspace,
		Shard:          shard,
		IncludePrimary: r.Form.Get("include_primary") == "on",
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to reload schema")
		return
	}
	if err := reloadSchemaEventsError(resp.GetEvents()); err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "reloaded schema on shard " + keyspace + "/" + shard,
	})
}

func (s *Server) shardExternallyPromote(w http.ResponseWriter, r *http.Request) {
	const title = "Tablet externally promoted"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	alias, err := parseShardFormAlias(r, "alias")
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if alias == nil {
		s.renderFormError(w, r, title, "promoted tablet alias is required")
		return
	}

	// The page is scoped to one shard, but the API operates on whatever alias
	// it is given. Verify the submitted tablet actually belongs to this shard
	// before mutating it, otherwise an operator could affect another shard
	// while believing they are acting on this one.
	tablet, err := s.api.GetTablet(r.Context(), &vtadminpb.GetTabletRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderError(w, r, tabletErrorStatus(err), title, err)
		return
	}
	if tablet == nil || tablet.GetTablet() == nil {
		s.renderFormError(w, r, title, "tablet not found")
		return
	}
	if tablet.GetTablet().GetKeyspace() != keyspace || tablet.GetTablet().GetShard() != shard {
		s.renderFormErrorErr(w, r, title, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"tablet %s belongs to %s/%s, not %s/%s",
			topoproto.TabletAliasString(alias),
			tablet.GetTablet().GetKeyspace(), tablet.GetTablet().GetShard(), keyspace, shard))
		return
	}

	_, err = s.api.TabletExternallyPromoted(r.Context(), &vtadminpb.TabletExternallyPromotedRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "acknowledged external promotion of " + topoproto.TabletAliasString(alias),
	})
}

func (s *Server) shardPlannedFailover(w http.ResponseWriter, r *http.Request) {
	const title = "Planned failover"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	options, err := parseShardFailoverOptions(r, keyspace, shard)
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	resp, err := s.api.PlannedFailoverShard(r.Context(), &vtadminpb.PlannedFailoverShardRequest{
		ClusterId: clusterID,
		Options:   options.Planned,
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to run planned failover")
		return
	}

	s.redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "planned failover completed for shard " + keyspace + "/" + shard,
	})
}

func (s *Server) shardEmergencyFailover(w http.ResponseWriter, r *http.Request) {
	const title = "Emergency failover"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	options, err := parseShardFailoverOptions(r, keyspace, shard)
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	resp, err := s.api.EmergencyFailoverShard(r.Context(), &vtadminpb.EmergencyFailoverShardRequest{
		ClusterId: clusterID,
		Options: &vtctldatapb.EmergencyReparentShardRequest{
			Keyspace:                  keyspace,
			Shard:                     shard,
			NewPrimary:                options.NewPrimary,
			WaitReplicasTimeout:       options.WaitReplicasTimeout,
			PreventCrossCellPromotion: r.Form.Get("prevent_cross_cell_promotion") == "on",
		},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to run emergency failover")
		return
	}

	s.redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "emergency failover completed for shard " + keyspace + "/" + shard,
	})
}

// parseShardFailoverOptions reads the new_primary tablet alias and
// wait_replicas_timeout form values shared by planned and emergency failover.
// An empty new_primary means vtctld picks the most up-to-date candidate.
func parseShardFailoverOptions(r *http.Request, keyspace, shard string) (*shardFailoverOptions, error) {
	opts := &shardFailoverOptions{}

	newPrimary, err := parseShardFormAlias(r, "new_primary")
	if err != nil {
		return nil, err
	}

	timeout, err := parseShardFormDuration(r, "wait_replicas_timeout")
	if err != nil {
		return nil, err
	}

	opts.NewPrimary = newPrimary
	opts.WaitReplicasTimeout = timeout
	opts.Planned = &vtctldatapb.PlannedReparentShardRequest{
		Keyspace:            keyspace,
		Shard:               shard,
		NewPrimary:          newPrimary,
		WaitReplicasTimeout: timeout,
	}
	return opts, nil
}

func parseShardFormAlias(r *http.Request, field string) (*topodatapb.TabletAlias, error) {
	value := strings.TrimSpace(r.Form.Get(field))
	if value == "" {
		return nil, nil
	}
	alias, err := topoproto.ParseTabletAlias(value)
	if err != nil {
		return nil, vterrors.Wrapf(err, "invalid tablet alias for %s", field)
	}
	return alias, nil
}

func parseShardFormDuration(r *http.Request, field string) (*vttimepb.Duration, error) {
	value := strings.TrimSpace(r.Form.Get(field))
	if value == "" {
		return nil, nil
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds < 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid value for %s: %s (expected non-negative integer seconds)", field, value)
	}
	return &vttimepb.Duration{Seconds: seconds}, nil
}

func (s *Server) shardValidate(w http.ResponseWriter, r *http.Request) {
	const title = "Validate shard"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.ValidateShard(r.Context(), &vtadminpb.ValidateShardRequest{
		ClusterId:   clusterID,
		Keyspace:    keyspace,
		Shard:       shard,
		PingTablets: r.Form.Get("ping_tablets") == "on",
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to validate shard")
		return
	}

	s.redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "validated shard " + keyspace + "/" + shard + ": " + strings.Join(resp.GetResults(), "; "),
	})
}

func (s *Server) shardValidateVersion(w http.ResponseWriter, r *http.Request) {
	const title = "Validate version"
	clusterID, keyspace, shard, ok := s.beginShardAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.ValidateVersionShard(r.Context(), &vtadminpb.ValidateVersionShardRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Shard:     shard,
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to validate version")
		return
	}

	s.redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "validated versions on shard " + keyspace + "/" + shard,
	})
}

func reloadSchemaEventsError(events []*logutilpb.Event) error {
	for _, ev := range events {
		if ev == nil {
			continue
		}
		switch ev.Level {
		case logutilpb.Level_ERROR:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "reload schema failed: %s", ev.GetValue())
		case logutilpb.Level_WARNING:
			if strings.Contains(strings.ToLower(ev.GetValue()), "failed") {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "reload schema failed: %s", ev.GetValue())
			}
		}
	}
	return nil
}
