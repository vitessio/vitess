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
	"log/slog"
	"net/http"
	"slices"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	schemaDetailData struct {
		ClusterID  string
		Keyspace   string
		Table      string
		Definition *tabletmanagerdatapb.TableDefinition
		Vindexes   []schemaTableVindex
	}

	schemaTableVindex struct {
		Name      string
		Columns   []string
		Type      string
		ParamKeys []string
		Params    map[string]string
		Primary   bool
	}
)

func schemaDetailPath(clusterID, keyspace, table string) string {
	return "/schema/" + pathEscape(clusterID) + "/" + pathEscape(keyspace) + "/" + pathEscape(table)
}

func (s *Server) schemaDetail(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster_id")
	keyspace := r.PathValue("keyspace")
	table := r.PathValue("table")

	schema, err := s.api.GetSchema(r.Context(), &vtadminpb.GetSchemaRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Table:     table,
	})
	if err != nil {
		s.renderError(w, r, tabletErrorStatus(err), "Schema", err)
		return
	}

	definition := findTableDefinition(schema.GetTableDefinitions(), table)
	if definition == nil {
		s.renderError(w, r, http.StatusNotFound, "Schema", vterrors.Errorf(
			vtrpcpb.Code_NOT_FOUND, "no table %s found in keyspace %s (cluster %s)", table, keyspace, clusterID,
		))
		return
	}

	s.render(w, r, http.StatusOK, "schema.html", PageData{
		Title:  table,
		Active: "schemas",
		Data: schemaDetailData{
			ClusterID:  clusterID,
			Keyspace:   keyspace,
			Table:      table,
			Definition: definition,
			Vindexes:   s.resolveTableVindexes(r, clusterID, keyspace, table),
		},
	})
}

// resolveTableVindexes resolves the column vindexes for a table against the
// keyspace VSchema. The VSchema is supplementary to the page (the table
// definition is the primary content), so failures are logged and the page
// renders without the vindexes section rather than erroring out entirely.
func (s *Server) resolveTableVindexes(r *http.Request, clusterID, keyspace, table string) []schemaTableVindex {
	resp, err := s.api.GetVSchema(r.Context(), &vtadminpb.GetVSchemaRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
	})
	if err != nil {
		slog.WarnContext(r.Context(), "failed to fetch VSchema for table vindexes",
			slog.String("cluster_id", clusterID),
			slog.String("keyspace", keyspace),
			slog.String("table", table),
			slog.Any("error", err))
		return nil
	}

	return resolveTableVindexes(resp.GetVSchema(), table)
}

func resolveTableVindexes(vschema *vschemapb.Keyspace, table string) []schemaTableVindex {
	tableInfo := vschema.GetTables()[table]
	if len(tableInfo.GetColumnVindexes()) == 0 {
		return nil
	}

	keyspaceVindexes := vschema.GetVindexes()
	resolved := make([]schemaTableVindex, 0, len(tableInfo.GetColumnVindexes()))
	for i, cv := range tableInfo.GetColumnVindexes() {
		vindex := keyspaceVindexes[cv.GetName()]

		columns := cv.GetColumns()
		if len(columns) == 0 && cv.GetColumn() != "" {
			columns = []string{cv.GetColumn()}
		}

		params := vindex.GetParams()
		paramKeys := make([]string, 0, len(params))
		for key := range params {
			paramKeys = append(paramKeys, key)
		}
		slices.Sort(paramKeys)

		resolved = append(resolved, schemaTableVindex{
			Name:      cv.GetName(),
			Columns:   columns,
			Type:      vindex.GetType(),
			ParamKeys: paramKeys,
			Params:    params,
			Primary:   i == 0,
		})
	}
	return resolved
}

func findTableDefinition(definitions []*tabletmanagerdatapb.TableDefinition, table string) *tabletmanagerdatapb.TableDefinition {
	for _, def := range definitions {
		if def.GetName() == table {
			return def
		}
	}
	return nil
}
