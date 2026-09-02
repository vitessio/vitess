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
	"strconv"
	"strings"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	createMoveTablesData struct {
		Form            formOptions
		SourceKeyspace  string
		SourceTables    []string
		SelectedCluster string
	}

	createReshardData struct {
		Form            formOptions
		PickCluster     bool
		SelectedCluster string
	}

	createMaterializeData struct {
		Form            formOptions
		TargetKeyspace  string
		ReferenceTables []string
		SelectedCluster string
	}

	createMigrationData struct {
		Form            formOptions
		PickCluster     bool
		SelectedCluster string
	}
)

// beginFormAction performs the shared preflight for mutating form handlers:
// read-only rejection, form parsing, and CSRF validation. ok is false when
// the preflight has already rendered an error response.
func (s *Server) beginFormAction(w http.ResponseWriter, r *http.Request, title string) bool {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return false
	}

	if err := r.ParseForm(); err != nil {
		s.renderFormError(w, r, title, err.Error())
		return false
	}
	if !validCSRFToken(r) {
		s.renderError(w, r, http.StatusForbidden, title, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "invalid CSRF token"))
		return false
	}
	return true
}

func (s *Server) createMoveTablesForm(w http.ResponseWriter, r *http.Request) {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return
	}

	selectedCluster := queryValue(r, "cluster_id")
	sourceKeyspace := queryValue(r, "source_keyspace")

	form, err := s.loadFormOptions(r, selectedCluster, "")
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Create MoveTables workflow", err)
		return
	}

	data := createMoveTablesData{
		Form:            form,
		SourceKeyspace:  sourceKeyspace,
		SelectedCluster: selectedCluster,
	}

	if selectedCluster != "" && sourceKeyspace != "" {
		tables, err := s.fetchSourceTables(r, selectedCluster, sourceKeyspace)
		if err != nil {
			s.renderError(w, r, http.StatusInternalServerError, "Create MoveTables workflow", err)
			return
		}
		data.SourceTables = tables
	}

	s.render(w, r, http.StatusOK, "workflow_movetables_create.html", PageData{
		Title:     "Create MoveTables workflow",
		Active:    "workflows",
		NeedsCSRF: true,
		Data:      data,
	})
}

func (s *Server) fetchSourceTables(r *http.Request, clusterID, keyspace string) ([]string, error) {
	resp, err := s.api.GetSchemas(r.Context(), &vtadminpb.GetSchemasRequest{
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		return nil, err
	}
	for _, schema := range resp.GetSchemas() {
		if schema.GetKeyspace() == keyspace {
			tables := make([]string, 0, len(schema.GetTableDefinitions()))
			for _, def := range schema.GetTableDefinitions() {
				tables = append(tables, def.GetName())
			}
			return tables, nil
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "no schema found for keyspace %s (cluster %s)", keyspace, clusterID)
}

func (s *Server) createReshardForm(w http.ResponseWriter, r *http.Request) {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return
	}

	requestedCluster := queryValue(r, "cluster_id")
	form, err := s.loadFormOptions(r, requestedCluster, "")
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Create Reshard workflow", err)
		return
	}

	// With more than one cluster, a shard list only makes sense per cluster;
	// ask the user to pick one explicitly rather than guessing.
	pickCluster := len(form.Clusters) > 1 && requestedCluster != form.SelectedCluster

	s.render(w, r, http.StatusOK, "workflow_reshard_create.html", PageData{
		Title:     "Create Reshard workflow",
		Active:    "workflows",
		NeedsCSRF: true,
		Data: createReshardData{
			Form:            form,
			PickCluster:     pickCluster,
			SelectedCluster: form.SelectedCluster,
		},
	})
}

func (s *Server) createReshard(w http.ResponseWriter, r *http.Request) {
	const title = "Create Reshard workflow"
	if !s.beginFormAction(w, r, title) {
		return
	}

	clusterID := strings.TrimSpace(r.Form.Get("cluster_id"))
	workflow := strings.TrimSpace(r.Form.Get("workflow"))
	keyspace := strings.TrimSpace(r.Form.Get("keyspace"))
	sourceShards := splitFormList(r.Form.Get("source_shards"))
	targetShards := splitFormList(r.Form.Get("target_shards"))

	if clusterID == "" {
		s.renderFormError(w, r, title, "cluster is required")
		return
	}
	if workflow == "" {
		s.renderFormError(w, r, title, "workflow name is required")
		return
	}
	if keyspace == "" {
		s.renderFormError(w, r, title, "keyspace is required")
		return
	}
	if len(sourceShards) == 0 {
		s.renderFormError(w, r, title, "at least one source shard is required")
		return
	}
	if len(targetShards) == 0 {
		s.renderFormError(w, r, title, "at least one target shard is required")
		return
	}

	tabletTypes, err := parseCreateSourceTabletTypes(r.Form["tablet_type"])
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	onDdl := strings.TrimSpace(r.Form.Get("on_ddl"))
	if onDdl == "" {
		onDdl = "IGNORE"
	}

	selectionPreference := tabletmanagerdatapb.TabletSelectionPreference_ANY
	if r.Form.Get("tablet_selection_preference") == "on" {
		selectionPreference = tabletmanagerdatapb.TabletSelectionPreference_INORDER
	}

	_, err = s.api.ReshardCreate(r.Context(), &vtadminpb.ReshardCreateRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.ReshardCreateRequest{
			Workflow:                  workflow,
			Keyspace:                  keyspace,
			SourceShards:              sourceShards,
			TargetShards:              targetShards,
			Cells:                     splitFormList(r.Form.Get("cells")),
			TabletTypes:               tabletTypes,
			TabletSelectionPreference: selectionPreference,
			SkipSchemaCopy:            r.Form.Get("skip_schema_copy") == "on",
			OnDdl:                     onDdl,
			StopAfterCopy:             r.Form.Get("stop_after_copy") == "on",
			DeferSecondaryKeys:        r.Form.Get("defer_secondary_keys") == "on",
			AutoStart:                 r.Form.Get("auto_start") == "on",
		},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, "/workflow/"+pathEscape(clusterID)+"/"+pathEscape(keyspace)+"/"+pathEscape(workflow), Flash{
		Kind:    "success",
		Message: "created Reshard workflow " + workflow + " on keyspace " + keyspace,
	})
}

func (s *Server) createMaterializeForm(w http.ResponseWriter, r *http.Request) {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return
	}

	selectedCluster := queryValue(r, "cluster_id")
	targetKeyspace := queryValue(r, "target_keyspace")

	form, err := s.loadFormOptions(r, selectedCluster, "")
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Create Materialize workflow", err)
		return
	}

	var referenceTables []string
	if selectedCluster != "" && targetKeyspace != "" {
		// Reference tables live in the target keyspace, where the materialized
		// copies will be queried from. A brand-new target keyspace may not have
		// a schema yet; that only means there is nothing to reference, so it is
		// not fatal.
		referenceTables, err = s.fetchSourceTables(r, selectedCluster, targetKeyspace)
		if err != nil && vterrors.Code(err) != vtrpcpb.Code_NOT_FOUND {
			s.renderError(w, r, http.StatusInternalServerError, "Create Materialize workflow", err)
			return
		}
	}

	s.render(w, r, http.StatusOK, "workflow_materialize_create.html", PageData{
		Title:     "Create Materialize workflow",
		Active:    "workflows",
		NeedsCSRF: true,
		Data: createMaterializeData{
			Form:            form,
			TargetKeyspace:  targetKeyspace,
			ReferenceTables: referenceTables,
			SelectedCluster: selectedCluster,
		},
	})
}

func (s *Server) createMaterialize(w http.ResponseWriter, r *http.Request) {
	const title = "Create Materialize workflow"
	if !s.beginFormAction(w, r, title) {
		return
	}

	clusterID := strings.TrimSpace(r.Form.Get("cluster_id"))
	workflow := strings.TrimSpace(r.Form.Get("workflow"))
	sourceKeyspace := strings.TrimSpace(r.Form.Get("source_keyspace"))
	targetKeyspace := strings.TrimSpace(r.Form.Get("target_keyspace"))

	if clusterID == "" {
		s.renderFormError(w, r, title, "cluster is required")
		return
	}
	if workflow == "" {
		s.renderFormError(w, r, title, "workflow name is required")
		return
	}
	if sourceKeyspace == "" {
		s.renderFormError(w, r, title, "source keyspace is required")
		return
	}
	if targetKeyspace == "" {
		s.renderFormError(w, r, title, "target keyspace is required")
		return
	}
	if sourceKeyspace == targetKeyspace {
		s.renderFormError(w, r, title, "source and target keyspace must differ")
		return
	}

	// The vtctld Materialize contract requires exactly one of TableSettings
	// (JSON) or ReferenceTables. Enforce the exclusive choice here so invalid
	// combinations are rejected before reaching the backend.
	tableSettings := strings.TrimSpace(r.Form.Get("table_settings"))
	referenceTables := r.Form["reference_table"]
	hasTableSettings := tableSettings != ""
	hasReferenceTables := len(referenceTables) > 0
	if hasTableSettings && hasReferenceTables {
		s.renderFormError(w, r, title, "provide either table settings or reference tables, not both")
		return
	}
	if !hasTableSettings && !hasReferenceTables {
		s.renderFormError(w, r, title, "provide table settings or select reference tables")
		return
	}

	tabletTypes, err := parseCreateSourceTabletTypes(r.Form["tablet_type"])
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	// MaterializeSettings carries cells and tablet types as pre-joined
	// strings rather than repeated fields.
	tabletTypeNames := make([]string, 0, len(tabletTypes))
	for _, tt := range tabletTypes {
		tabletTypeNames = append(tabletTypeNames, tt.String())
	}

	_, err = s.api.MaterializeCreate(r.Context(), &vtadminpb.MaterializeCreateRequest{
		ClusterId:     clusterID,
		TableSettings: tableSettings,
		Request: &vtctldatapb.MaterializeCreateRequest{
			Settings: &vtctldatapb.MaterializeSettings{
				Workflow:                  workflow,
				SourceKeyspace:            sourceKeyspace,
				TargetKeyspace:            targetKeyspace,
				ReferenceTables:           referenceTables,
				Cell:                      strings.Join(splitFormList(r.Form.Get("cell")), ","),
				TabletTypes:               strings.Join(tabletTypeNames, ","),
				StopAfterCopy:             r.Form.Get("stop_after_copy") == "on",
				TabletSelectionPreference: parseSelectionPreference(r.Form.Get("tablet_selection_preference") == "on"),
				MaterializationIntent:     vtctldatapb.MaterializationIntent_CUSTOM,
			},
		},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, "/workflow/"+pathEscape(clusterID)+"/"+pathEscape(targetKeyspace)+"/"+pathEscape(workflow), Flash{
		Kind:    "success",
		Message: "created Materialize workflow " + workflow + " (" + sourceKeyspace + " -> " + targetKeyspace + ")",
	})
}

func (s *Server) createMigrationForm(w http.ResponseWriter, r *http.Request) {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return
	}

	requestedCluster := queryValue(r, "cluster_id")
	form, err := s.loadFormOptions(r, requestedCluster, "")
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Create schema migration", err)
		return
	}

	pickCluster := len(form.Clusters) > 1 && requestedCluster != form.SelectedCluster

	s.render(w, r, http.StatusOK, "migration_create.html", PageData{
		Title:     "Create schema migration",
		Active:    "migrations",
		NeedsCSRF: true,
		Data: createMigrationData{
			Form:            form,
			PickCluster:     pickCluster,
			SelectedCluster: form.SelectedCluster,
		},
	})
}

func (s *Server) createMigration(w http.ResponseWriter, r *http.Request) {
	const title = "Create schema migration"
	if !s.beginFormAction(w, r, title) {
		return
	}

	clusterID := strings.TrimSpace(r.Form.Get("cluster_id"))
	keyspace := strings.TrimSpace(r.Form.Get("keyspace"))
	sql := strings.TrimSpace(r.Form.Get("sql"))

	if clusterID == "" {
		s.renderFormError(w, r, title, "cluster is required")
		return
	}
	if keyspace == "" {
		s.renderFormError(w, r, title, "keyspace is required")
		return
	}
	if sql == "" {
		s.renderFormError(w, r, title, "SQL is required")
		return
	}

	batchSize := int64(0)
	if raw := strings.TrimSpace(r.Form.Get("batch_size")); raw != "" {
		var err error
		batchSize, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || batchSize < 0 {
			s.renderFormError(w, r, title, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid batch size: %s (expected non-negative integer)", raw).Error())
			return
		}
	}

	dDLStrategy := strings.TrimSpace(r.Form.Get("ddl_strategy"))
	if dDLStrategy == "" {
		dDLStrategy = "vitess"
	}

	// The vtadmin API layer splits multi-statement SQL into pieces via the
	// SQL parser and overrides Request.Sql, so pass the raw SQL as-is.
	_, err := s.api.ApplySchema(r.Context(), &vtadminpb.ApplySchemaRequest{
		ClusterId: clusterID,
		Sql:       sql,
		CallerId:  strings.TrimSpace(r.Form.Get("caller_id")),
		Request: &vtctldatapb.ApplySchemaRequest{
			Keyspace:         keyspace,
			DdlStrategy:      dDLStrategy,
			BatchSize:        batchSize,
			MigrationContext: strings.TrimSpace(r.Form.Get("migration_context")),
			UuidList:         splitFormList(r.Form.Get("uuid_list")),
		},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, "/migrations?keyspace="+url.QueryEscape(keyspace)+"&cluster_id="+url.QueryEscape(clusterID), Flash{
		Kind:    "success",
		Message: "schema migration request created for keyspace " + keyspace,
	})
}

func parseSelectionPreference(inOrder bool) tabletmanagerdatapb.TabletSelectionPreference {
	if inOrder {
		return tabletmanagerdatapb.TabletSelectionPreference_INORDER
	}
	return tabletmanagerdatapb.TabletSelectionPreference_ANY
}

func (s *Server) createMoveTables(w http.ResponseWriter, r *http.Request) {
	const title = "Create MoveTables workflow"
	if !s.beginFormAction(w, r, title) {
		return
	}

	clusterID := strings.TrimSpace(r.Form.Get("cluster_id"))
	workflow := strings.TrimSpace(r.Form.Get("workflow"))
	sourceKeyspace := strings.TrimSpace(r.Form.Get("source_keyspace"))
	targetKeyspace := strings.TrimSpace(r.Form.Get("target_keyspace"))

	if clusterID == "" {
		s.renderFormError(w, r, title, "cluster is required")
		return
	}
	if workflow == "" {
		s.renderFormError(w, r, title, "workflow name is required")
		return
	}
	if sourceKeyspace == "" {
		s.renderFormError(w, r, title, "source keyspace is required")
		return
	}
	if targetKeyspace == "" {
		s.renderFormError(w, r, title, "target keyspace is required")
		return
	}
	if sourceKeyspace == targetKeyspace {
		s.renderFormError(w, r, title, "source and target keyspace must differ")
		return
	}

	allTables := r.Form.Get("all_tables") == "on"
	includeTables := r.Form["table"]
	if allTables {
		// MoveTables rejects a request with both AllTables and IncludeTables,
		// so drop individual selections when copying all tables.
		includeTables = nil
	}
	if !allTables && len(includeTables) == 0 {
		s.renderFormError(w, r, title, "select at least one table or enable copy of all tables")
		return
	}

	tabletTypes, err := parseCreateSourceTabletTypes(r.Form["tablet_type"])
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	onDdl := strings.TrimSpace(r.Form.Get("on_ddl"))
	if onDdl == "" {
		onDdl = "IGNORE"
	}

	_, err = s.api.MoveTablesCreate(r.Context(), &vtadminpb.MoveTablesCreateRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.MoveTablesCreateRequest{
			Workflow:            workflow,
			SourceKeyspace:      sourceKeyspace,
			TargetKeyspace:      targetKeyspace,
			AllTables:           allTables,
			IncludeTables:       includeTables,
			Cells:               splitFormList(r.Form.Get("cells")),
			TabletTypes:         tabletTypes,
			OnDdl:               onDdl,
			SourceTimeZone:      strings.TrimSpace(r.Form.Get("source_time_zone")),
			ExternalClusterName: strings.TrimSpace(r.Form.Get("external_cluster_name")),
			AutoStart:           r.Form.Get("auto_start") == "on",
			StopAfterCopy:       r.Form.Get("stop_after_copy") == "on",
			DeferSecondaryKeys:  r.Form.Get("defer_secondary_keys") == "on",
		},
	})
	if err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}

	s.redirectWithFlash(w, r, "/workflow/"+pathEscape(clusterID)+"/"+pathEscape(targetKeyspace)+"/"+pathEscape(workflow), Flash{
		Kind:    "success",
		Message: "created MoveTables workflow " + workflow + " (" + sourceKeyspace + " -> " + targetKeyspace + ")",
	})
}

// splitFormList splits a comma-separated form value into trimmed, non-empty parts.
func splitFormList(value string) []string {
	out := make([]string, 0, strings.Count(value, ",")+1)
	for part := range strings.SplitSeq(value, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseTabletTypes(names []string) ([]topodatapb.TabletType, error) {
	types := make([]topodatapb.TabletType, 0, len(names))
	for _, name := range names {
		value, ok := topodatapb.TabletType_value[name]
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown tablet type %s", name)
		}
		types = append(types, topodatapb.TabletType(value))
	}
	return types, nil
}

func parseCreateSourceTabletTypes(names []string) ([]topodatapb.TabletType, error) {
	if len(names) == 0 {
		return []topodatapb.TabletType{
			topodatapb.TabletType_REPLICA,
			topodatapb.TabletType_PRIMARY,
		}, nil
	}
	return parseTabletTypes(names)
}

// renderFormErrorErr is renderFormError for error values.
func (s *Server) renderFormErrorErr(w http.ResponseWriter, r *http.Request, title string, err error) {
	s.renderFormError(w, r, title, err.Error())
}
