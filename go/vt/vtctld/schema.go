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
	"fmt"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
)

var (
	migrationCheckTicks *timer.Timer
)

var (
	migrationCheckInterval = time.Second * 10
)

func initSchemaManager(ts *topo.Server) {
	migrationCheckTicks = timer.NewTimer(migrationCheckInterval)

	runMigrationRequestChecks(ts)
}

func runMigrationRequestChecks(ts *topo.Server) {
	ctx, cancel := context.WithCancel(context.Background())
	migrationCheckTicks.Start(func() { onMigrationCheckTick(ctx, ts) })

	go func() {
		<-ctx.Done()
		migrationCheckTicks.Stop()
	}()

	// Running cancel on OnTermSync will cancel the context of any
	// running workflow inside vtctld. They may still checkpoint
	// if they want to.
	servenv.OnTermSync(cancel)
}

func reviewMigrationRequest(ctx context.Context, ts *topo.Server, conn topo.Conn, uuid string) error {
	entryPath := fmt.Sprintf("%s/%s", schema.MigrationRequestsPath(), uuid)
	onlineDDL, err := schema.ReadTopo(ctx, conn, entryPath)
	if err != nil {
		return err
	}
	log.Infof("Found schema migration request: %+v", onlineDDL)

	onlineDDL.Status = schema.OnlineDDLStatusQueued

	shardNames, err := ts.GetShardNames(ctx, onlineDDL.Keyspace)
	if err != nil {
		return fmt.Errorf("unable to get shard names for keyspace: %s, error: %v", onlineDDL.Keyspace, err)
	}
	for _, shardName := range shardNames {
		if err := onlineDDL.WriteTopo(ctx, conn, onlineDDL.JobsKeyspaceShardPath(shardName)); err != nil {
			return fmt.Errorf("unable to write shard job %+v error: %s", onlineDDL, err.Error())
		}
	}

	if err := onlineDDL.WriteTopo(ctx, conn, schema.MigrationQueuedPath()); err != nil {
		return fmt.Errorf("unable to write reviewed migration: %+v, error: %s", onlineDDL, err)
	}
	if err := conn.Delete(ctx, entryPath, nil); err != nil {
		return fmt.Errorf("unable to delete %+v, error: %s", entryPath, err)
	}
	return nil
}

func reviewMigrationRequests(ctx context.Context, ts *topo.Server, conn topo.Conn) error {
	entries, err := conn.ListDir(ctx, schema.MigrationRequestsPath(), true)
	if err != nil {
		log.Errorf("vtctld.reviewMigrationRequests listDir error: %s", err.Error())
		return err
	}

	for _, entry := range entries {
		if err := reviewMigrationRequest(ctx, ts, conn, entry.Name); err != nil {
			log.Errorf("vtctld.reviewMigrationRequest %s error: %s", entry.Name, err.Error())
			continue
		}
	}
	return nil
}

func onMigrationCheckTick(ctx context.Context, ts *topo.Server) {
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

	reviewMigrationRequests(ctx, ts, conn)
}
