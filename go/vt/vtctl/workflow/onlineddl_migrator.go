/*
Copyright 2022 The Vitess Authors.

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

package workflow

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/onlineddl"
)

/*
todo:
* check that all migrations are completed
* select distinct for online ddl streams: check what to do with different attributes while merging across source streams
*/

type OnlineDDLMigration struct {
	id                 int64
	migrationUUID      string
	keyspace           string
	shard              string
	mysqlSchema        string
	mysqlTable         string
	migrationStatement string
	strategy           string
	options            string
	addedTimestamp     string
	requestedTimestamp string
	readTimestamp      string
	startedTimestamp   string
	livenessTimestamp  string
	completedTimestamp string
	cleanupTimestamp   string
	migrationStatus    string
	logPath            string
	artifacts          string
}

type OnlineDDLMigrator struct {
	migrations []*OnlineDDLMigration
	ts         ITrafficSwitcher
	logger     logutil.Logger
	exec       func(context.Context, *topodata.TabletAlias, bool, string, int) (*query.QueryResult, error)
}

func isPending(migrationStatus string) bool {
	switch schema.OnlineDDLStatus(migrationStatus) {
	case schema.OnlineDDLStatusComplete:
	case schema.OnlineDDLStatusCancelled:
	case schema.OnlineDDLStatusFailed:
		return true
	}
	return false
}

func BuildOnlineDDLMigrator(ctx context.Context, ts ITrafficSwitcher,
	exec func(context.Context, *topodata.TabletAlias, bool, string, int) (*query.QueryResult, error)) (*OnlineDDLMigrator, error) {

	odm := &OnlineDDLMigrator{
		ts:     ts,
		logger: ts.Logger(),
		exec:   exec,
	}
	var mu sync.Mutex
	if odm.ts.MigrationType() != binlogdatapb.MigrationType_SHARDS {
		// Source streams should be stopped only for shard migrations.
		return odm, nil
	}

	var sources [][]*OnlineDDLMigration
	f := func(tabletStreams []*OnlineDDLMigration) {
		log.Infof("tabletStreams %s", len(tabletStreams))
		mu.Lock()
		defer mu.Unlock()
		sources = append(sources, tabletStreams)
	}

	// todo: validate count, uuids of migrations across sources
	done := false
	numStreams := 0
	err := odm.ts.ForAllSources(func(source *MigrationSource) error {
		log.Infof("reading tablet streams for %+v", source)
		tabletStreams, err := odm.readTabletStreams(ctx, source.GetPrimary())
		if err != nil {
			log.Infof("err %s", err)
			return err
		}
		if !done {
			numStreams = len(tabletStreams)
		} else {
			if numStreams != len(tabletStreams) {
				log.Infof("number of online ddl migrations across source shards don't match")
				return fmt.Errorf("number of online ddl migrations across source shards don't match")
			}
		}
		for _, migration := range tabletStreams {
			if isPending(migration.migrationStatus) {
				return fmt.Errorf("migration %s is not yet complete, status is %s", migration.migrationUUID, migration.migrationStatus)
			}
		}
		f(tabletStreams)
		return nil
	})
	if err != nil {
		log.Infof("err %s", err)
		return nil, err
	}
	// todo: more validations to check that the migrations are not mismatched across shards
	log.Infof("sources len is %d", len(sources))
	if len(sources) > 0 {
		odm.migrations = sources[0]
	}
	return odm, nil
}

// to update on target: shard
func getMigrationFromRow(row sqltypes.RowNamedValues) (*OnlineDDLMigration, error) {
	var m OnlineDDLMigration
	m.id, _ = row["id"].ToInt64()
	m.migrationUUID = row["migration_uuid"].ToString()
	m.keyspace = row["keyspace"].ToString()
	m.mysqlSchema = row["mysql_schema"].ToString()
	m.mysqlTable = row["mysql_table"].ToString()
	m.migrationStatement = row["migration_statement"].ToString()
	m.strategy = row["strategy"].ToString()
	m.options = row["options"].ToString()
	m.addedTimestamp = row["added_timestamp"].ToString()
	m.requestedTimestamp = row["requested_timestamp"].ToString()
	m.readTimestamp = row["read_timestamp"].ToString()
	m.startedTimestamp = row["started_timestamp"].ToString()
	m.livenessTimestamp = row["liveness_timestamp"].ToString()
	m.completedTimestamp = row["completed_timestamp"].ToString()
	m.cleanupTimestamp = row["cleanup_timestamp"].ToString()
	m.migrationStatus = row["migration_status"].ToString()
	m.logPath = row["log_path"].ToString()
	m.artifacts = row["artifacts"].ToString()
	m.shard = row["shard"].ToString()
	return &m, nil
}

func (odm *OnlineDDLMigrator) readTabletStreams(ctx context.Context, ti *topo.TabletInfo) ([]*OnlineDDLMigration, error) {
	log.Infof("tablet is %+v", ti)
	log.Flush()
	qry := "select * from _vt.schema_migrations"
	p3qr, err := odm.exec(ctx, ti.Alias, false, qry, 10000)
	if err != nil {
		return nil, err
	}
	qr := sqltypes.Proto3ToResult(p3qr)
	tabletStreams := make([]*OnlineDDLMigration, 0, len(qr.Rows))

	for _, row := range qr.Named().Rows {
		migration, err := getMigrationFromRow(row)
		if err != nil {
			return nil, err
		}
		log.Infof("found migration: %s", migration.migrationUUID)
		tabletStreams = append(tabletStreams, migration)
	}
	log.Infof("readTabletStreams: %d", len(tabletStreams))
	return tabletStreams, nil
}

func (odm *OnlineDDLMigrator) insertMigration(ctx context.Context, tablet *topodata.TabletAlias, migration *OnlineDDLMigration, shard string) error {
	log.Infof("insertMigration: tablet %s, uuid %s, shard %s", tablet.String(), migration.migrationUUID, shard)
	qry := onlineddl.GetOnlineDDLInsertMigrationQuery()
	m := migration
	// tbd add remaining columns to table for POC
	// actually needs to generate based on version of source since target may not contain same columns?
	qry, err := sqlparser.ParseAndBind(qry,
		sqltypes.StringBindVariable(m.migrationUUID),
		sqltypes.StringBindVariable(m.keyspace),
		sqltypes.StringBindVariable(shard),
		sqltypes.StringBindVariable(m.mysqlSchema),
		sqltypes.StringBindVariable(m.mysqlTable),
		sqltypes.StringBindVariable(m.migrationStatement),
		sqltypes.StringBindVariable(m.strategy),
		sqltypes.StringBindVariable(m.options),
		sqltypes.StringBindVariable("tbd"),
		sqltypes.StringBindVariable("tbd"),
		sqltypes.StringBindVariable(m.migrationStatus),
		sqltypes.StringBindVariable("tbd"),
		sqltypes.Int64BindVariable(0),
		sqltypes.BoolBindVariable(false),
		sqltypes.BoolBindVariable(false),
		sqltypes.StringBindVariable(""),
		sqltypes.BoolBindVariable(false),
	)
	if err != nil {
		log.Infof("err %s", err)
		return err
	}
	// todo: cleanup target's schema migration on error or do this in tx?
	log.Infof("running on tablet %s: %s", tablet.String(), qry)
	if _, err := odm.exec(ctx, tablet, true, qry, 10000); err != nil {
		log.Infof("err %s", err)
		return err
	}
	return nil
}
func (odm *OnlineDDLMigrator) createTargetStreams(ctx context.Context) error {
	if err := odm.ts.ForAllTargets(func(target *MigrationTarget) error {
		for _, migration := range odm.migrations {
			log.Infof("calling insertMigration: %s", migration.migrationUUID)
			if err := odm.insertMigration(ctx, target.GetPrimary().Alias, migration, target.GetShard().ShardName()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (odm *OnlineDDLMigrator) MigrateStreams(ctx context.Context) error {
	if odm.migrations == nil {
		return nil
	}
	err := odm.deleteTargetStreams(ctx)
	if err != nil {
		return err
	}
	err = odm.createTargetStreams(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (odm *OnlineDDLMigrator) deleteTargetStreams(ctx context.Context) error {
	if err := odm.ts.ForAllTargets(func(target *MigrationTarget) error {
		qry := "truncate table _vt.schema_migrations"
		_, err := odm.exec(ctx, target.GetPrimary().Alias, false, qry, 10000)
		if err != nil {
			return err
		}
		return err
	}); err != nil {
		return err
	}
	return nil
}
