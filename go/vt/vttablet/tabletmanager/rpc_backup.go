/*
Copyright 2017 Google Inc.

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

package tabletmanager

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Backup takes a db backup and sends it to the BackupStorage
func (agent *ActionAgent) Backup(ctx context.Context, concurrency int, logger logutil.Logger) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	// update our type to BACKUP
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot take backup, if you really need to do this, restart vttablet in replica mode")
	}
	originalType := tablet.Type
	if _, err := topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, topodatapb.TabletType_BACKUP); err != nil {
		return err
	}

	// let's update our internal state (stop query service and other things)
	if err := agent.refreshTablet(ctx, "before backup"); err != nil {
		return err
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format("2006-01-02.150405"), topoproto.TabletAliasString(tablet.Alias))
	returnErr := mysqlctl.Backup(ctx, agent.MysqlDaemon, l, dir, name, concurrency, agent.hookExtraEnv())

	// change our type back to the original value
	_, err = topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, originalType)
	if err != nil {
		// failure in changing the topology type is probably worse,
		// so returning that (we logged the snapshot error anyway)
		if returnErr != nil {
			l.Errorf("mysql backup command returned error: %v", returnErr)
		}
		returnErr = err
	}

	// let's update our internal state (start query service and other things)
	if err := agent.refreshTablet(ctx, "after backup"); err != nil {
		return err
	}

	// and re-run health check to be sure to capture any replication delay
	agent.runHealthCheckLocked()

	return returnErr
}

// RestoreFromBackup deletes all local data and restores anew from the latest backup.
func (agent *ActionAgent) RestoreFromBackup(ctx context.Context, logger logutil.Logger) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot restore from backup, if you really need to do this, restart vttablet in replica mode")
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run restore
	err = agent.restoreDataLocked(ctx, l, true /* deleteBeforeRestore */)

	// re-run health check to be sure to capture any replication delay
	agent.runHealthCheckLocked()

	return err
}
