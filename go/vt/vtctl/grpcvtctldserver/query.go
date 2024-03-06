/*
Copyright 2023 The Vitess Authors.

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
package grpcvtctldserver

import (
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/schematools"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

const (
	alterSingleSchemaMigrationSql = `alter vitess_migration %a `
	alterAllSchemaMigrationSql    = `alter vitess_migration %s all`
	selectSchemaMigrationsSql     = `select
	*
	from _vt.schema_migrations where %s %s %s`
	AllMigrationsIndicator = "all"
)

func alterSchemaMigrationQuery(command, uuid string) (string, error) {
	if strings.ToLower(uuid) == AllMigrationsIndicator {
		return fmt.Sprintf(alterAllSchemaMigrationSql, command), nil
	}
	return sqlparser.ParseAndBind(alterSingleSchemaMigrationSql+command, sqltypes.StringBindVariable(uuid))
}

func selectSchemaMigrationsQuery(condition, order, skipLimit string) string {
	return fmt.Sprintf(selectSchemaMigrationsSql, condition, order, skipLimit)
}

// rowToSchemaMigration converts a single row into a SchemaMigration protobuf.
func rowToSchemaMigration(row sqltypes.RowNamedValues) (sm *vtctldatapb.SchemaMigration, err error) {
	sm = new(vtctldatapb.SchemaMigration)
	sm.Uuid = row.AsString("migration_uuid", "")
	sm.Keyspace = row.AsString("keyspace", "")
	sm.Shard = row.AsString("shard", "")
	sm.Schema = row.AsString("mysql_schema", "")
	sm.Table = row.AsString("mysql_table", "")
	sm.MigrationStatement = row.AsString("migration_statement", "")

	sm.Strategy, err = schematools.ParseSchemaMigrationStrategy(row.AsString("strategy", ""))
	if err != nil {
		return nil, err
	}

	sm.Options = row.AsString("options", "")

	sm.AddedAt, err = valueToVTTime(row.AsString("added_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.RequestedAt, err = valueToVTTime(row.AsString("requested_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.ReadyAt, err = valueToVTTime(row.AsString("ready_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.StartedAt, err = valueToVTTime(row.AsString("started_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.LivenessTimestamp, err = valueToVTTime(row.AsString("liveness_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.CompletedAt, err = valueToVTTime(row.AsString("completed_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.CleanedUpAt, err = valueToVTTime(row.AsString("cleanup_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.Status, err = schematools.ParseSchemaMigrationStatus(row.AsString("migration_status", "unknown"))
	if err != nil {
		return nil, err
	}

	sm.LogPath = row.AsString("log_path", "")
	sm.Artifacts = row.AsString("artifacts", "")
	sm.Retries = row.AsUint64("retries", 0)

	if alias := row.AsString("tablet", ""); alias != "" {
		sm.Tablet, err = topoproto.ParseTabletAlias(alias)
		if err != nil {
			return nil, err
		}
	}

	sm.TabletFailure = row.AsBool("tablet_failure", false)
	sm.Progress = float32(row.AsFloat64("progress", 0))
	sm.MigrationContext = row.AsString("migration_context", "")
	sm.DdlAction = row.AsString("ddl_action", "")
	sm.Message = row.AsString("message", "")
	sm.EtaSeconds = row.AsInt64("eta_seconds", -1)
	sm.RowsCopied = row.AsUint64("rows_copied", 0)
	sm.TableRows = row.AsInt64("table_rows", 0)
	sm.AddedUniqueKeys = uint32(row.AsUint64("added_unique_keys", 0))
	sm.RemovedUniqueKeys = uint32(row.AsUint64("removed_unique_keys", 0))
	sm.LogFile = row.AsString("log_file", "")

	sm.ArtifactRetention, err = valueToVTDuration(row.AsString("retain_artifacts_seconds", ""), "s")
	if err != nil {
		return nil, err
	}

	sm.PostponeCompletion = row.AsBool("postpone_completion", false)
	sm.RemovedForeignKeyNames = row.AsString("removed_foreign_key_names", "")
	sm.RemovedUniqueKeyNames = row.AsString("removed_unique_key_names", "")
	sm.DroppedNoDefaultColumnNames = row.AsString("dropped_no_default_column_names", "")
	sm.ExpandedColumnNames = row.AsString("expanded_column_names", "")
	sm.RevertibleNotes = row.AsString("revertible_notes", "")
	sm.AllowConcurrent = row.AsBool("allow_concurrent", false)
	sm.RevertedUuid = row.AsString("reverted_uuid", "")
	sm.IsView = row.AsBool("is_view", false)
	sm.ReadyToComplete = row.AsBool("ready_to_complete", false)
	sm.VitessLivenessIndicator = row.AsInt64("vitess_liveness_indicator", 0)
	sm.UserThrottleRatio = float32(row.AsFloat64("user_throttle_ratio", 0))
	sm.SpecialPlan = row.AsString("special_plan", "")

	sm.LastThrottledAt, err = valueToVTTime(row.AsString("last_throttled_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.ComponentThrottled = row.AsString("component_throttled", "")

	sm.CancelledAt, err = valueToVTTime(row.AsString("cancelled_at", ""))
	if err != nil {
		return nil, err
	}

	sm.PostponeLaunch = row.AsBool("postpone_launch", false)
	sm.Stage = row.AsString("stage", "")
	sm.CutoverAttempts = uint32(row.AsUint64("cutover_attempts", 0))
	sm.IsImmediateOperation = row.AsBool("is_immediate_operation", false)

	sm.ReviewedAt, err = valueToVTTime(row.AsString("reviewed_timestamp", ""))
	if err != nil {
		return nil, err
	}

	sm.ReadyToCompleteAt, err = valueToVTTime(row.AsString("ready_to_complete_timestamp", ""))
	if err != nil {
		return nil, err
	}

	return sm, nil
}

// valueToVTTime converts a SQL timestamp string into a vttime Time type, first
// parsing the raw string value into a Go Time type in the local timezone. This
// is a correct conversion only if the vtctld is set to the same timezone as the
// vttablet that stored the value.
func valueToVTTime(s string) (*vttime.Time, error) {
	if s == "" {
		return nil, nil
	}

	gotime, err := time.ParseInLocation(sqltypes.TimestampFormat, s, time.Local)
	if err != nil {
		return nil, err
	}

	return protoutil.TimeToProto(gotime), nil
}

// valueToVTDuration converts a SQL string into a vttime Duration type. It takes
// a defaultUnit in the event the value is a bare numeral (e.g. 124 vs 124s).
func valueToVTDuration(s string, defaultUnit string) (*vttime.Duration, error) {
	if s == "" {
		return nil, nil
	}

	switch s[len(s)-1] {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		s += defaultUnit
	}

	godur, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}

	return protoutil.DurationToProto(godur), nil
}

// queryResultForTabletResults aggregates given results into a combined result set
func queryResultForTabletResults(results map[string]*sqltypes.Result) *sqltypes.Result {
	var qr = &sqltypes.Result{}
	defaultFields := []*querypb.Field{{
		Name:    "Tablet",
		Type:    sqltypes.VarBinary,
		Charset: collations.CollationBinaryID,
		Flags:   uint32(querypb.MySqlFlag_BINARY_FLAG),
	}}
	var row2 []sqltypes.Value
	for tabletAlias, result := range results {
		if qr.Fields == nil {
			qr.Fields = append(qr.Fields, defaultFields...)
			qr.Fields = append(qr.Fields, result.Fields...)
		}
		for _, row := range result.Rows {
			row2 = nil
			row2 = append(row2, sqltypes.NewVarBinary(tabletAlias))
			row2 = append(row2, row...)
			qr.Rows = append(qr.Rows, row2)
		}
	}
	return qr
}
