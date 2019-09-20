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

package tabletmanager

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	backupModeOnline  = "online"
	backupModeOffline = "offline"
)

// Backup takes a db backup and sends it to the BackupStorage
func (agent *ActionAgent) Backup(ctx context.Context, concurrency int, logger logutil.Logger, allowMaster bool) error {
	if agent.Cnf == nil {
		return fmt.Errorf("cannot perform backup without my.cnf, please restart vttablet with a my.cnf file specified")
	}

	// Check tablet type current process has.
	// During a network partition it is possible that from the topology perspective this is no longer the master,
	// but the process didn't find out about this.
	// It is not safe to take backups from tablet in this state
	currentTablet := agent.Tablet()
	if !allowMaster && currentTablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot take backup. if you really need to do this, rerun the backup command with -allow_master")
	}
	engine, err := mysqlctl.GetBackupEngine()
	if err != nil {
		return vterrors.Wrap(err, "failed to find backup engine")
	}
	// get Tablet info from topo so that it is up to date
	tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		return err
	}
	if !allowMaster && tablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot take backup. if you really need to do this, rerun the backup command with -allow_master")
	}

	// prevent concurrent backups, and record stats
	backupMode := backupModeOnline
	if engine.ShouldDrainForBackup() {
		backupMode = backupModeOffline
	}
	if err := agent.beginBackup(backupMode); err != nil {
		return err
	}
	defer agent.endBackup(backupMode)

	var originalType topodatapb.TabletType
	if engine.ShouldDrainForBackup() {
		if err := agent.lock(ctx); err != nil {
			return err
		}
		defer agent.unlock()
		tablet, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
		if err != nil {
			return err
		}
		originalType = tablet.Type
		// update our type to BACKUP
		if _, err := topotools.ChangeType(ctx, agent.TopoServer, tablet.Alias, topodatapb.TabletType_BACKUP); err != nil {
			return err
		}

		// let's update our internal state (stop query service and other things)
		if err := agent.refreshTablet(ctx, "before backup"); err != nil {
			return err
		}
	}
	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	dir := fmt.Sprintf("%v/%v", tablet.Keyspace, tablet.Shard)
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format("2006-01-02.150405"), topoproto.TabletAliasString(tablet.Alias))
	backupParams := mysqlctl.BackupParams{
		Cnf:          agent.Cnf,
		Mysqld:       agent.MysqlDaemon,
		Logger:       l,
		Concurrency:  concurrency,
		HookExtraEnv: agent.hookExtraEnv(),
		TopoServer:   agent.TopoServer,
		Keyspace:     tablet.Keyspace,
		Shard:        tablet.Shard,
	}

	returnErr := mysqlctl.Backup(ctx, dir, name, backupParams)

	if engine.ShouldDrainForBackup() {
		bgCtx := context.Background()
		// Starting from here we won't be able to recover if we get stopped by a cancelled
		// context. It is also possible that the context already timed out during the
		// above call to Backup. Thus we use the background context to get through to the finish.

		// change our type back to the original value
		_, err = topotools.ChangeType(bgCtx, agent.TopoServer, tablet.Alias, originalType)
		if err != nil {
			// failure in changing the topology type is probably worse,
			// so returning that (we logged the snapshot error anyway)
			if returnErr != nil {
				l.Errorf("mysql backup command returned error: %v", returnErr)
			}
			returnErr = err
		}

		// let's update our internal state (start query service and other things)
		if err := agent.refreshTablet(bgCtx, "after backup"); err != nil {
			return err
		}
		// and re-run health check to be sure to capture any replication delay
		// not needed for online backups because it will continue to run per schedule
		agent.runHealthCheckLocked()
	}

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
	err = agent.restoreDataLocked(ctx, l, 0 /* waitForBackupInterval */, true /* deleteBeforeRestore */)

	// re-run health check to be sure to capture any replication delay
	agent.runHealthCheckLocked()

	return err
}

func (agent *ActionAgent) beginBackup(backupMode string) error {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	if agent._isBackupRunning {
		return fmt.Errorf("a backup is already running on tablet: %v", agent.TabletAlias)
	}
	// when mode is online we don't take the action lock, so we continue to serve,
	// but let's set _isBackupRunning to true
	// so that we only allow one online backup at a time
	// offline backups also run only one at a time because we take the action lock
	// so this is not really needed in that case, however we are using it to record the state
	agent._isBackupRunning = true
	if agent.exportStats {
		agent.statsBackupIsRunning.Set([]string{backupMode}, 1)
	}
	return nil
}

func (agent *ActionAgent) endBackup(backupMode string) {
	// now we set _isBackupRunning back to false
	// have to take the mutex lock before writing to _ fields
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	agent._isBackupRunning = false
	if agent.exportStats {
		agent.statsBackupIsRunning.Set([]string{backupMode}, 0)
	}
}
