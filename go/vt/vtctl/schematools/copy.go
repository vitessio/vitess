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

package schematools

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CopySchemas applies a set of `CREATE TABLE` SQL statements on the target
// tablet. It works by applying the SQL statements directly on the primary with
// binary logging enabled, so that schemas propagate via standard MySQL
// replication. Therefore, it should only be used for changes that can be
// applied on a live instance without causing issues; it should not be used for
// anything that will require downtime.
//
// It returns the primary position of target tablet after applying all
// statements, so callers know what position to wait for if verifying replicas
// in the shard.
//
// Each SQL statement is first processed as a text/template, and the
// tablet's database name can be interpolated as {{ .DatabaseName }} there.
func CopySchemas(ctx context.Context, tmc tmclient.TabletManagerClient, target *topo.TabletInfo, changes []string, disableForeignKeyChecks bool) (string, error) {
	buf := bytes.NewBuffer(nil)
	vars := map[string]string{"DatabaseName": target.DbName()}

	filledChanges := make([]string, len(changes))
	for i, change := range changes {
		tmpl, err := template.New("").Parse(change)
		if err != nil {
			return "", err
		}

		if err := tmpl.Execute(buf, vars); err != nil {
			return "", err
		}

		filledChanges[i] = buf.String()
		buf.Reset()
	}

	for i, sql := range filledChanges {
		reloadSchema := i == len(filledChanges)-1
		if err := applySchemaSQL(ctx, tmc, target.Tablet, sql, disableForeignKeyChecks, reloadSchema); err != nil {
			return "", fmt.Errorf("creating a table failed."+
				" Most likely some tables already exist on the destination and differ from the source."+
				" Please remove all to be copied tables from the destination manually and run this command again."+
				" Full error: %v", err)
		}
	}

	pos, err := tmc.PrimaryPosition(ctx, target.Tablet)
	if err != nil {
		return "", fmt.Errorf("CopySchemas: can't get replication position after schema applied: %v", err)
	}

	return pos, nil
}

func applySchemaSQL(ctx context.Context, tmc tmclient.TabletManagerClient, tablet *topodatapb.Tablet, sql string, disableForeignKeyChecks bool, reloadSchema bool) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Need to make sure that we enable binlog, since we're only applying the statement on primaries.
	_, err := tmc.ExecuteFetchAsDba(ctx, tablet, false, []byte(sql), 0, false, disableForeignKeyChecks, reloadSchema)
	return err
}

// CopyShardMetadata copies the contents of the _vt.shard_metadata table from
// the source tablet to the destination tablet.
//
// NOTE: This function assumes that the destination tablet is a primary with
// binary logging enabled, in order to propagate the INSERT statements to any
// replicas in the destination shard.
func CopyShardMetadata(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, source *topodatapb.TabletAlias, dest *topodatapb.TabletAlias) error {
	sourceTablet, err := ts.GetTablet(ctx, source)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %w", topoproto.TabletAliasString(source), err)
	}

	destTablet, err := ts.GetTablet(ctx, dest)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %w", topoproto.TabletAliasString(dest), err)
	}

	sql := "SELECT 1 FROM information_schema.tables WHERE table_schema = '_vt' AND table_name = 'shard_metadata'"
	presenceResult, err := tmc.ExecuteFetchAsDba(ctx, sourceTablet.Tablet, false, []byte(sql), 1, false, false, false)
	if err != nil {
		return fmt.Errorf("ExecuteFetchAsDba(%v, false, %v, 1, false, false, false) failed: %v", topoproto.TabletAliasString(source), sql, err)
	}
	if len(presenceResult.Rows) == 0 {
		log.Infof("_vt.shard_metadata doesn't exist on the source tablet %v, skipping its copy.", topoproto.TabletAliasString(source))
		return nil
	}

	// (TODO|@ajm188,@deepthi): 100 may be too low here for row limit
	sql = "SELECT db_name, name, value FROM _vt.shard_metadata"
	p3qr, err := tmc.ExecuteFetchAsDba(ctx, sourceTablet.Tablet, false, []byte(sql), 100, false, false, false)
	if err != nil {
		return fmt.Errorf("ExecuteFetchAsDba(%v, false, %v, 100, false, false, false) failed: %v", topoproto.TabletAliasString(source), sql, err)
	}

	qr := sqltypes.Proto3ToResult(p3qr)
	queryBuf := bytes.NewBuffer(nil)
	for _, row := range qr.Rows {
		dbName := row[0]
		name := row[1]
		value := row[2]
		queryBuf.WriteString("INSERT INTO _vt.shard_metadata (db_name, name, value) VALUES (")
		dbName.EncodeSQL(queryBuf)
		queryBuf.WriteByte(',')
		name.EncodeSQL(queryBuf)
		queryBuf.WriteByte(',')
		value.EncodeSQL(queryBuf)
		queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
		value.EncodeSQL(queryBuf)

		_, err := tmc.ExecuteFetchAsDba(ctx, destTablet.Tablet, false, queryBuf.Bytes(), 0, false, false, false)
		if err != nil {
			return fmt.Errorf("ExecuteFetchAsDba(%v, false, %v, 0, false, false, false) failed: %v", topoproto.TabletAliasString(dest), queryBuf.String(), err)
		}

		queryBuf.Reset()
	}

	return nil
}
