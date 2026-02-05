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

package schematools

import (
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

type tSchemaMigration struct {
	*vtctldatapb.SchemaMigration
	// Renamed fields
	MigrationUuid            string
	MysqlSchema              string
	MysqlTable               string
	AddedTimestamp           *vttime.Time
	RequestedTimestamp       *vttime.Time
	ReadyTimestamp           *vttime.Time
	StartedTimestamp         *vttime.Time
	CompletedTimestamp       *vttime.Time
	CleanupTimestamp         *vttime.Time
	ArtifactRetentionSeconds int64
	LastThrottledTimestamp   *vttime.Time
	CancelledTimestamp       *vttime.Time
	ReviewedTimestamp        *vttime.Time
	ReadyToCompleteTimestamp *vttime.Time

	// Re-typed fields. These must have distinct names or the first-pass
	// marshalling will not produce fields/rows for these.
	Status_   string `sqltypes:"$$status"`
	Tablet_   string `sqltypes:"$$tablet"`
	Strategy_ string `sqltypes:"$$strategy"`
}

func replaceSchemaMigrationFields(result *sqltypes.Result) *sqltypes.Result {
	// NOTE: this depends entirely on (1) the ordering of the fields in the
	// embedded protobuf message and (2) that MarshalResult walks fields in the
	// order they are defined (via reflect.VisibleFields).
	//
	// That half is stable, as it is part of the VisibleFields API, but if we
	// were to remove or reorder fields in the SchemaMigration proto without
	// updating this function, this could break.
	return sqltypes.ReplaceFields(result, map[string]string{
		"uuid":                 "migration_uuid",
		"schema":               "mysql_schema",
		"table":                "mysql_table",
		"added_at":             "added_timestamp",
		"requested_at":         "requested_timestamp",
		"ready_at":             "ready_timestamp",
		"started_at":           "started_timestamp",
		"completed_at":         "completed_timestamp",
		"cleaned_up_at":        "cleanup_timestamp",
		"artifact_retention":   "artifact_retention_seconds",
		"last_throttled_at":    "last_throttled_timestamp",
		"cancelled_at":         "cancelled_timestamp",
		"reviewed_at":          "reviewed_timestamp",
		"ready_to_complete_at": "ready_to_complete_timestamp",
		"$$status":             "status",
		"$$tablet":             "tablet",
		"$$strategy":           "strategy",
	})
}

type MarshallableSchemaMigration vtctldatapb.SchemaMigration

func (t *MarshallableSchemaMigration) MarshalResult() (*sqltypes.Result, error) {
	artifactRetention, _, err := protoutil.DurationFromProto(t.ArtifactRetention)
	if err != nil {
		return nil, err
	}

	tmp := tSchemaMigration{
		SchemaMigration:          (*vtctldatapb.SchemaMigration)(t),
		MigrationUuid:            t.Uuid,
		MysqlSchema:              t.Schema,
		MysqlTable:               t.Table,
		AddedTimestamp:           t.AddedAt,
		RequestedTimestamp:       t.RequestedAt,
		ReadyTimestamp:           t.ReadyAt,
		StartedTimestamp:         t.StartedAt,
		CompletedTimestamp:       t.CompletedAt,
		CleanupTimestamp:         t.CleanedUpAt,
		ArtifactRetentionSeconds: int64(artifactRetention.Seconds()),
		LastThrottledTimestamp:   t.LastThrottledAt,
		CancelledTimestamp:       t.CancelledAt,
		ReviewedTimestamp:        t.ReviewedAt,
		ReadyToCompleteTimestamp: t.ReadyToCompleteAt,
		Status_:                  SchemaMigrationStatusName(t.Status),
		Tablet_:                  topoproto.TabletAliasString(t.Tablet),
		Strategy_:                SchemaMigrationStrategyName(t.Strategy),
	}

	res, err := sqltypes.MarshalResult(&tmp)
	if err != nil {
		return nil, err
	}

	return replaceSchemaMigrationFields(res), nil
}

type MarshallableSchemaMigrations []*vtctldatapb.SchemaMigration

func (ts MarshallableSchemaMigrations) MarshalResult() (*sqltypes.Result, error) {
	s := make([]*tSchemaMigration, len(ts))
	for i, t := range ts {
		artifactRetention, _, err := protoutil.DurationFromProto(t.ArtifactRetention)
		if err != nil {
			return nil, err
		}

		tmp := &tSchemaMigration{
			SchemaMigration:          (*vtctldatapb.SchemaMigration)(t),
			MigrationUuid:            t.Uuid,
			MysqlSchema:              t.Schema,
			MysqlTable:               t.Table,
			AddedTimestamp:           t.AddedAt,
			RequestedTimestamp:       t.RequestedAt,
			ReadyTimestamp:           t.ReadyAt,
			StartedTimestamp:         t.StartedAt,
			CompletedTimestamp:       t.CompletedAt,
			CleanupTimestamp:         t.CleanedUpAt,
			ArtifactRetentionSeconds: int64(artifactRetention.Seconds()),
			LastThrottledTimestamp:   t.LastThrottledAt,
			CancelledTimestamp:       t.CancelledAt,
			ReviewedTimestamp:        t.ReviewedAt,
			ReadyToCompleteTimestamp: t.ReadyToCompleteAt,
			Status_:                  SchemaMigrationStatusName(t.Status),
			Tablet_:                  topoproto.TabletAliasString(t.Tablet),
			Strategy_:                SchemaMigrationStrategyName(t.Strategy),
		}
		s[i] = tmp
	}

	res, err := sqltypes.MarshalResult(s)
	if err != nil {
		return nil, err
	}

	return replaceSchemaMigrationFields(res), nil
}
