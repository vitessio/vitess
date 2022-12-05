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
	"context"
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	backupModeOnline  = "online"
	backupModeOffline = "offline"
)

// Backup takes a db backup and sends it to the BackupStorage
func (tm *TabletManager) Backup(ctx context.Context, logger logutil.Logger, req *tabletmanagerdatapb.BackupRequest) error {
	if tm.Cnf == nil {
		return fmt.Errorf("cannot perform backup without my.cnf, please restart vttablet with a my.cnf file specified")
	}

	// Check tablet type current process has.
	// During a network partition it is possible that from the topology perspective this is no longer the primary,
	// but the process didn't find out about this.
	// It is not safe to take backups from tablet in this state
	currentTablet := tm.Tablet()
	if !req.AllowPrimary && currentTablet.Type == topodatapb.TabletType_PRIMARY {
		return fmt.Errorf("type PRIMARY cannot take backup. if you really need to do this, rerun the backup command with --allow_primary")
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
	if !req.AllowPrimary && tablet.Type == topodatapb.TabletType_PRIMARY {
		return fmt.Errorf("type PRIMARY cannot take backup. if you really need to do this, rerun the backup command with --allow_primary")
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
		if err := tm.changeTypeLocked(ctx, topodatapb.TabletType_BACKUP, DBActionNone, SemiSyncActionUnset); err != nil {
			return err
		}
	}
	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run the backup
	backupParams := mysqlctl.BackupParams{
		Cnf:                tm.Cnf,
		Mysqld:             tm.MysqlDaemon,
		Logger:             l,
		Concurrency:        int(req.Concurrency),
		IncrementalFromPos: req.IncrementalFromPos,
		HookExtraEnv:       tm.hookExtraEnv(),
		TopoServer:         tm.TopoServer,
		Keyspace:           tablet.Keyspace,
		Shard:              tablet.Shard,
		TabletAlias:        topoproto.TabletAliasString(tablet.Alias),
		BackupTime:         time.Now(),
	}

	returnErr := mysqlctl.Backup(ctx, backupParams)

	if engine.ShouldDrainForBackup() {
		bgCtx := context.Background()
		// Starting from here we won't be able to recover if we get stopped by a cancelled
		// context. It is also possible that the context already timed out during the
		// above call to Backup. Thus we use the background context to get through to the finish.

		// Change our type back to the original value.
		// Original type could be primary so pass in a real value for PrimaryTermStartTime
		if err := tm.changeTypeLocked(bgCtx, originalType, DBActionNone, SemiSyncActionNone); err != nil {
			// failure in changing the topology type is probably worse,
			// so returning that (we logged the snapshot error anyway)
			if returnErr != nil {
				l.Errorf("mysql backup command returned error: %v", returnErr)
			}
			returnErr = err
		}
	}

	return returnErr
}

// RestoreFromBackup deletes all local data and then restores the data from the latest backup [at
// or before the backupTime value if specified]
func (tm *TabletManager) RestoreFromBackup(ctx context.Context, logger logutil.Logger, request *tabletmanagerdatapb.RestoreFromBackupRequest) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()

	tablet, err := tm.TopoServer.GetTablet(ctx, tm.tabletAlias)
	if err != nil {
		return err
	}
	if tablet.Type == topodatapb.TabletType_PRIMARY {
		return fmt.Errorf("type PRIMARY cannot restore from backup, if you really need to do this, restart vttablet in replica mode")
	}

	// create the loggers: tee to console and source
	l := logutil.NewTeeLogger(logutil.NewConsoleLogger(), logger)

	// now we can run restore
	err = tm.restoreDataLocked(ctx, l, 0 /* waitForBackupInterval */, true /* deleteBeforeRestore */, request)

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
