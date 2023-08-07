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
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/schematools"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

// 2023-08-05 07:15:16
const mysqlTimestampLayout = "2006-01-02 15:04:05"

// TODO: explicit column list to guarantee parsing stability
func selectSchemaMigrationsQuery(condition, order, skipLimit string) string {
	return fmt.Sprintf(`select
		*
		from _vt.schema_migrations where %s %s %s`, condition, order, skipLimit)
}

func rowToSchemaMigration(row sqltypes.Row) (sm *vtctldatapb.SchemaMigration, err error) {
	row = row[1:] // We're off by 1 because of the extra Tablet field added to the grouped query result

	sm = new(vtctldatapb.SchemaMigration)
	sm.Uuid = row[1].ToString()
	sm.Keyspace = row[2].ToString()
	sm.Shard = row[3].ToString()
	sm.Schema = row[4].ToString()
	sm.Table = row[5].ToString()
	sm.MigrationStatement = row[6].ToString()

	if name := row[7].ToString(); name != "" {
		sm.Strategy, err = schematools.ParseSchemaMigrationStrategy(name)
		if err != nil {
			return nil, err
		}
	}

	sm.Options = row[8].ToString()

	sm.AddedAt, err = valueToVTTime(row[9])
	if err != nil {
		return nil, err
	}

	sm.RequestedAt, err = valueToVTTime(row[10])
	if err != nil {
		return nil, err
	}

	sm.ReadyAt, err = valueToVTTime(row[11])
	if err != nil {
		return nil, err
	}

	sm.StartedAt, err = valueToVTTime(row[12])
	if err != nil {
		return nil, err
	}

	// liveness_indicator row[13]

	sm.CompletedAt, err = valueToVTTime(row[14])
	if err != nil {
		return nil, err
	}

	sm.CleanedUpAt, err = valueToVTTime(row[15])
	if err != nil {
		return nil, err
	}

	if status := row[16].ToString(); status != "" {
		sm.Status, err = schematools.ParseSchemaMigrationStatus(status)
		if err != nil {
			return nil, err
		}
	}

	sm.LogPath = row[17].ToString()
	sm.Artifacts = row[18].ToString()

	sm.Retries, err = row[19].ToUint64()
	if err != nil {
		return nil, err
	}

	if alias := row[20].ToString(); alias != "" {
		sm.Tablet, err = topoproto.ParseTabletAlias(alias)
		if err != nil {
			return nil, err
		}
	}

	sm.TabletFailure, err = row[21].ToBool()
	if err != nil {
		return nil, err
	}

	sm.Progress, err = row[22].ToFloat32()
	if err != nil {
		return nil, err
	}

	sm.MigrationContext = row[23].ToString()
	// ddl_action row[24]
	sm.Message = row[25].ToString()

	sm.EtaSeconds, err = row[26].ToInt64()
	if err != nil {
		return nil, err
	}

	sm.RowsCopied, err = row[27].ToUint64()
	if err != nil {
		return nil, err
	}

	sm.TableRows, err = row[28].ToInt64()
	if err != nil {
		return nil, err
	}

	sm.AddedUniqueKeys, err = row[29].ToUint32()
	if err != nil {
		return nil, err
	}

	sm.RemovedUniqueKeys, err = row[30].ToUint32()
	if err != nil {
		return nil, err
	}

	sm.LogFile = row[31].ToString()

	sm.ArtifactRetention, err = valueToVTDuration(row[32], "s")
	if err != nil {
		return nil, err
	}

	sm.PostponeCompletion, err = row[33].ToBool()
	if err != nil {
		return nil, err
	}

	sm.RemovedUniqueKeyNames = row[34].ToString()
	sm.DroppedNoDefaultColumnNames = row[35].ToString()
	sm.ExpandedColumnNames = row[36].ToString()
	sm.RevertibleNotes = row[37].ToString()

	sm.AllowConcurrent, err = row[38].ToBool()
	if err != nil {
		return nil, err
	}

	sm.RevertedUuid = row[39].ToString()

	sm.IsView, err = row[40].ToBool()
	if err != nil {
		return nil, err
	}

	sm.ReadyToComplete, err = row[41].ToBool()
	if err != nil {
		return nil, err
	}

	sm.VitessLivenessIndicator, err = row[42].ToInt64()
	if err != nil {
		return nil, err
	}

	sm.UserThrottleRatio, err = row[43].ToFloat32()
	if err != nil {
		return nil, err
	}

	sm.SpecialPlan = row[44].ToString()

	sm.LastThrottledAt, err = valueToVTTime(row[45])
	if err != nil {
		return nil, err
	}

	sm.ComponentThrottled = row[46].ToString()

	sm.CancelledAt, err = valueToVTTime(row[47])
	if err != nil {
		return nil, err
	}

	sm.PostponeLaunch, err = row[48].ToBool()
	if err != nil {
		return nil, err
	}

	sm.Stage = row[49].ToString()

	sm.CutoverAttempts, err = row[50].ToUint32()
	if err != nil {
		return nil, err
	}

	sm.IsImmediateOperation, err = row[51].ToBool()
	if err != nil {
		return nil, err
	}

	sm.ReviewedAt, err = valueToVTTime(row[52])
	if err != nil {
		return nil, err
	}

	sm.ReadyToCompleteAt, err = valueToVTTime(row[53])
	if err != nil {
		return nil, err
	}

	return sm, nil
}

func valueToVTTime(value sqltypes.Value) (*vttime.Time, error) {
	s := value.ToString()
	if s == "" {
		return nil, nil
	}

	gotime, err := time.ParseInLocation(mysqlTimestampLayout, s, time.Local)
	if err != nil {
		return nil, err
	}

	return protoutil.TimeToProto(gotime), nil
}

func valueToVTDuration(value sqltypes.Value, defaultUnit string) (*vttime.Duration, error) {
	s := value.ToString()
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
