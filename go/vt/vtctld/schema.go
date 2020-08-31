/*
Copyright 2019 The Vitess Authors.

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

package vtctld

import (
	"flag"
	"fmt"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	migrationCheckTicks *timer.Timer
)

var (
	migrationCheckInterval = flag.Duration("online_ddl_check_interval", time.Minute, "interval polling for new online DDL requests")
)

func initSchemaManager(ts *topo.Server) {
	tmClient := tmclient.NewTabletManagerClient()
	migrationCheckTicks = timer.NewTimer(*migrationCheckInterval)

	runMigrationRequestChecks(ts, tmClient)
}

func runMigrationRequestChecks(ts *topo.Server, tmClient tmclient.TabletManagerClient) {
	ctx, cancel := context.WithCancel(context.Background())
	migrationCheckTicks.Start(func() { onMigrationCheckTick(ctx, ts, tmClient) })

	go func() {
		<-ctx.Done()
		migrationCheckTicks.Stop()
	}()

	// Running cancel on OnTermSync will cancel the context of any
	// running workflow inside vtctld. They may still checkpoint
	// if they want to.
	servenv.OnTermSync(cancel)
}

func reviewMigrationRequest(ctx context.Context, ts *topo.Server, tmClient tmclient.TabletManagerClient, conn topo.Conn, uuid string) error {
	entryPath := fmt.Sprintf("%s/%s", schema.MigrationRequestsPath(), uuid)
	onlineDDL, err := schema.ReadTopo(ctx, conn, entryPath)
	if err != nil {
		return err
	}
	log.Infof("Found schema migration request: %+v", onlineDDL)

	onlineDDL.Status = schema.OnlineDDLStatusQueued

	logstream := logutil.NewMemoryLogger()
	wr := wrangler.New(logstream, ts, tmClient)

	sqlInsertSchemaMigration := `INSERT IGNORE INTO %s.schema_migrations (
		migration_uuid,
		keyspace,
		shard,
		mysql_schema,
		mysql_table,
		migration_statement,
		strategy,
		options,
		requested_timestamp,
		migration_status
	) VALUES (
		%a, %a, %a, %a, %a, %a, %a, %a, FROM_UNIXTIME(%a), %a
	)`
	parsed := sqlparser.BuildParsedQuery(sqlInsertSchemaMigration, "_vt",
		":migration_uuid",
		":keyspace",
		":shard",
		":mysql_schema",
		":mysql_table",
		":migration_statement",
		":strategy",
		":options",
		":requested_timestamp",
		":migration_status",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid":      sqltypes.StringBindVariable(onlineDDL.UUID),
		"keyspace":            sqltypes.StringBindVariable(onlineDDL.Keyspace),
		"shard":               sqltypes.StringBindVariable(""),
		"mysql_schema":        sqltypes.StringBindVariable(""),
		"mysql_table":         sqltypes.StringBindVariable(onlineDDL.Table),
		"migration_statement": sqltypes.StringBindVariable(onlineDDL.SQL),
		"strategy":            sqltypes.StringBindVariable(string(onlineDDL.Strategy)),
		"options":             sqltypes.StringBindVariable(onlineDDL.Options),
		"requested_timestamp": sqltypes.Int64BindVariable(onlineDDL.RequestTimeSeconds()),
		"migration_status":    sqltypes.StringBindVariable(string(onlineDDL.Status)),
	}

	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}

	_, err = wr.VExecResult(ctx, onlineDDL.UUID, onlineDDL.Keyspace, bound, false)
	if err != nil {
		return err
	}

	if err := onlineDDL.WriteTopo(ctx, conn, schema.MigrationQueuedPath()); err != nil {
		return fmt.Errorf("unable to write reviewed migration: %+v, error: %s", onlineDDL, err)
	}
	if err := conn.Delete(ctx, entryPath, nil); err != nil {
		return fmt.Errorf("unable to delete %+v, error: %s", entryPath, err)
	}
	return nil
}

func reviewMigrationRequests(ctx context.Context, ts *topo.Server, tmClient tmclient.TabletManagerClient, conn topo.Conn) error {
	entries, err := conn.ListDir(ctx, schema.MigrationRequestsPath(), true)
	if err != nil {
		log.Errorf("vtctld.reviewMigrationRequests listDir error: %s", err.Error())
		return err
	}

	for _, entry := range entries {
		if err := reviewMigrationRequest(ctx, ts, tmClient, conn, entry.Name); err != nil {
			log.Errorf("vtctld.reviewMigrationRequest %s error: %s", entry.Name, err.Error())
			continue
		}
	}
	return nil
}

func onMigrationCheckTick(ctx context.Context, ts *topo.Server, tmClient tmclient.TabletManagerClient) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		log.Errorf("Executor.checkMigrations ConnForCell error: %s", err.Error())
		return
	}

	lockDescriptor, err := conn.Lock(ctx, schema.MigrationRequestsPath(), "cvtctld.checkMigrationRequests")
	if err != nil {
		log.Errorf("Executor.checkMigrations ConnForCell error: %s", err.Error())
		return
	}
	defer lockDescriptor.Unlock(ctx)

	reviewMigrationRequests(ctx, ts, tmClient, conn)
}
