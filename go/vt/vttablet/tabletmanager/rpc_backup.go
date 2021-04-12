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

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	backupModeOnline  = "online"
	backupModeOffline = "offline"
)

// Backup takes a db backup and sends it to the BackupStorage
func (tm *TabletManager) Backup(ctx context.Context, concurrency int, logger logutil.Logger, allowMaster bool) error {
	if tm.Cnf == nil {
		return fmt.Errorf("cannot perform backup without my.cnf, please restart vttablet with a my.cnf file specified")
	}

	// Check tablet type current process has.
	// During a network partition it is possible that from the topology perspective this is no longer the master,
	// but the process didn't find out about this.
	// It is not safe to take backups from tablet in this state
	currentTablet := tm.Tablet()
	if !allowMaster && currentTablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot take backup. if you really need to do this, rerun the backup command with -allow_master")
	}
	engine, err := mysqlctl.GetBackupEngine()
	if err != nil {
		return vterrors.Wrap(err, "failed to find backup engine")
	}
	// get Tablet info from topo so that it is up to date
	tablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
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
	if err := tm.beginBackup(backupMode); err != nil {
		return err
	}
	defer tm.endBackup(backupMode)

	var originalType topodatapb.TabletType
	if engine.ShouldDrainForBackup() {
		if err := tm.lock(ctx); err != nil {
			return err
		}
		defer tm.unlock()

		tablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
		if err != nil {
			return err
		}
		originalType = tablet.Type
		// update our type to BACKUP
		if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_BACKUP, DBActionNone); err != nil {
			return err
		}
		// Tell Orchestrator we're stopped on purpose for some Vitess task.
		// Do this in the background, as it's best-effort.
		go func() {
			if tm.orc == nil {
				return
			}
			if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to run an offline backup"); err != nil {
				logger.Warningf("Orchestrator BeginMaintenance failed: %v", err)
			}
		}()
	}
	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	backupParams := mysqlctl.BackupParams{
		Cnf:          tm.Cnf,
		Mysqld:       tm.MysqlDaemon,
		Logger:       l,
		Concurrency:  concurrency,
		HookExtraEnv: tm.hookExtraEnv(),
		TopoServer:   tm.TopoServer,
		Keyspace:     tablet.Keyspace,
		Shard:        tablet.Shard,
		TabletAlias:  topoproto.TabletAliasString(tablet.Alias),
		BackupTime:   time.Now(),
	}

	returnErr := mysqlctl.Backup(ctx, backupParams)

	if engine.ShouldDrainForBackup() {
		bgCtx := context.Background()
		// Starting from here we won't be able to recover if we get stopped by a cancelled
		// context. It is also possible that the context already timed out during the
		// above call to Backup. Thus we use the background context to get through to the finish.

		// Change our type back to the original value.
		// Original type could be master so pass in a real value for masterTermStartTime
		if err := tm.changeTypeLocked(bgCtx, originalType, DBActionNone); err != nil {
			// failure in changing the topology type is probably worse,
			// so returning that (we logged the snapshot error anyway)
			if returnErr != nil {
				l.Errorf("mysql backup command returned error: %v", returnErr)
			}
			returnErr = err
		} else {
			// Tell Orchestrator we're no longer stopped on purpose.
			// Do this in the background, as it's best-effort.
			go func() {
				if tm.orc == nil {
					return
				}
				if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
					logger.Warningf("Orchestrator EndMaintenance failed: %v", err)
				}
			}()
		}
	}

	return returnErr
}

// RestoreFromBackup deletes all local data and restores anew from the latest backup.
func (tm *TabletManager) RestoreFromBackup(ctx context.Context, logger logutil.Logger) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	tablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("type MASTER cannot restore from backup, if you really need to do this, restart vttablet in replica mode")
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run restore
	err = tm.restoreDataLocked(ctx, l, 0 /* waitForBackupInterval */, true /* deleteBeforeRestore */)

	// re-run health check to be sure to capture any replication delay
	tm.QueryServiceControl.BroadcastHealth()

	return err
}

func (tm *TabletManager) beginBackup(backupMode string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	if tm._isBackupRunning {
		return fmt.Errorf("a backup is already running on tablet: %v", tm.tabletAlias)
	}
	// when mode is online we don't take the action lock, so we continue to serve,
	// but let's set _isBackupRunning to true
	// so that we only allow one online backup at a time
	// offline backups also run only one at a time because we take the action lock
	// so this is not really needed in that case, however we are using it to record the state
	tm._isBackupRunning = true
	statsBackupIsRunning.Set([]string{backupMode}, 1)
	return nil
}

func (tm *TabletManager) endBackup(backupMode string) {
	// now we set _isBackupRunning back to false
	// have to take the mutex lock before writing to _ fields
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm._isBackupRunning = false
	statsBackupIsRunning.Set([]string{backupMode}, 0)
}
