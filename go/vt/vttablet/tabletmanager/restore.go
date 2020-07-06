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
	"flag"
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file handles the initial backup restore upon startup.
// It is only enabled if restore_from_backup is set.

var (
	restoreFromBackup     = flag.Bool("restore_from_backup", false, "(init restore parameter) will check BackupStorage for a recent backup at startup and start there")
	restoreConcurrency    = flag.Int("restore_concurrency", 4, "(init restore parameter) how many concurrent files to restore at once")
	waitForBackupInterval = flag.Duration("wait_for_backup_interval", 0, "(init restore parameter) if this is greater than 0, instead of starting up empty when no backups are found, keep checking at this interval for a backup to appear")
)

// RestoreData is the main entry point for backup restore.
// It will either work, fail gracefully, or return
// an error in case of a non-recoverable error.
// It takes the action lock so no RPC interferes.
func (tm *TabletManager) RestoreData(ctx context.Context, logger logutil.Logger, waitForBackupInterval time.Duration, deleteBeforeRestore bool) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()
	if tm.Cnf == nil {
		return fmt.Errorf("cannot perform restore without my.cnf, please restart vttablet with a my.cnf file specified")
	}
	return tm.restoreDataLocked(ctx, logger, waitForBackupInterval, deleteBeforeRestore)
}

func (tm *TabletManager) restoreDataLocked(ctx context.Context, logger logutil.Logger, waitForBackupInterval time.Duration, deleteBeforeRestore bool) error {
	var originalType topodatapb.TabletType
	tablet := tm.Tablet()
	originalType, tablet.Type = tablet.Type, topodatapb.TabletType_RESTORE
	tm.updateState(ctx, tablet, "restore from backup")

	// Try to restore. Depending on the reason for failure, we may be ok.
	// If we're not ok, return an error and the tm will log.Fatalf,
	// causing the process to be restarted and the restore retried.
	// Record local metadata values based on the original type.
	localMetadata := tm.getLocalMetadataValues(originalType)

	keyspace := tablet.Keyspace
	keyspaceInfo, err := tm.TopoServer.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	// For a SNAPSHOT keyspace, we have to look for backups of BaseKeyspace
	// so we will pass the BaseKeyspace in RestoreParams instead of tablet.Keyspace
	if keyspaceInfo.KeyspaceType == topodatapb.KeyspaceType_SNAPSHOT {
		if keyspaceInfo.BaseKeyspace == "" {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, fmt.Sprintf("snapshot keyspace %v has no base_keyspace set", tablet.Keyspace))
		}
		keyspace = keyspaceInfo.BaseKeyspace
		log.Infof("Using base_keyspace %v to restore keyspace %v", keyspace, tablet.Keyspace)
	}

	params := mysqlctl.RestoreParams{
		Cnf:                 tm.Cnf,
		Mysqld:              tm.MysqlDaemon,
		Logger:              logger,
		Concurrency:         *restoreConcurrency,
		HookExtraEnv:        tm.hookExtraEnv(),
		LocalMetadata:       localMetadata,
		DeleteBeforeRestore: deleteBeforeRestore,
		DbName:              topoproto.TabletDbName(tablet),
		Keyspace:            keyspace,
		Shard:               tablet.Shard,
		StartTime:           logutil.ProtoToTime(keyspaceInfo.SnapshotTime),
	}

	// Loop until a backup exists, unless we were told to give up immediately.
	var backupManifest *mysqlctl.BackupManifest
	for {
		backupManifest, err = mysqlctl.Restore(ctx, params)
		if waitForBackupInterval == 0 {
			break
		}
		// We only retry a specific set of errors. The rest we return immediately.
		if err != mysqlctl.ErrNoBackup && err != mysqlctl.ErrNoCompleteBackup {
			break
		}

		log.Infof("No backup found. Waiting %v (from -wait_for_backup_interval flag) to check again.", waitForBackupInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitForBackupInterval):
		}
	}

	var pos mysql.Position
	if backupManifest != nil {
		pos = backupManifest.Position
	}
	switch err {
	case nil:
		// Starting from here we won't be able to recover if we get stopped by a cancelled
		// context. Thus we use the background context to get through to the finish.
		if keyspaceInfo.KeyspaceType == topodatapb.KeyspaceType_NORMAL {
			// Reconnect to master only for "NORMAL" keyspaces
			if err := tm.startReplication(context.Background(), pos, originalType); err != nil {
				return err
			}
		}
	case mysqlctl.ErrNoBackup:
		// No-op, starting with empty database.
	case mysqlctl.ErrExistingDB:
		// No-op, assuming we've just restarted.  Note the
		// replication reporter may restart replication at the
		// next health check if it thinks it should. We do not
		// alter replication here.
	default:
		// If anything failed, we should reset the original tablet type
		tablet.Type = originalType
		tm.updateState(ctx, tablet, "failed for restore from backup")
		return vterrors.Wrap(err, "Can't restore backup")
	}

	// If we had type BACKUP or RESTORE it's better to set our type to the init_tablet_type to make result of the restore
	// similar to completely clean start from scratch.
	if (originalType == topodatapb.TabletType_BACKUP || originalType == topodatapb.TabletType_RESTORE) && *initTabletType != "" {
		initType, err := topoproto.ParseTabletType(*initTabletType)
		if err == nil {
			originalType = initType
		}
	}

	// Change type back to original type if we're ok to serve.
	tablet.Type = originalType
	tm.updateState(ctx, tablet, "after restore from backup")

	return nil
}

func (tm *TabletManager) startReplication(ctx context.Context, pos mysql.Position, tabletType topodatapb.TabletType) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
	}
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, "failed to reset replication")
	}

	// Set the position at which to resume from the master.
	if err := tm.MysqlDaemon.SetReplicationPosition(ctx, pos); err != nil {
		return vterrors.Wrap(err, "failed to set replication position")
	}

	// Read the shard to find the current master, and its location.
	tablet := tm.Tablet()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return vterrors.Wrap(err, "can't read shard")
	}
	if si.MasterAlias == nil {
		// We've restored, but there's no master. This is fine, since we've
		// already set the position at which to resume when we're later reparented.
		// If we had instead considered this fatal, all tablets would crash-loop
		// until a master appears, which would make it impossible to elect a master.
		log.Warningf("Can't start replication after restore: shard %v/%v has no master.", tablet.Keyspace, tablet.Shard)
		return nil
	}
	if topoproto.TabletAliasEqual(si.MasterAlias, tablet.Alias) {
		// We used to be the master before we got restarted in an empty data dir,
		// and no other master has been elected in the meantime.
		// This shouldn't happen, so we'll let the operator decide which tablet
		// should actually be promoted to master.
		log.Warningf("Can't start replication after restore: master record still points to this tablet.")
		return nil
	}
	ti, err := tm.TopoServer.GetTablet(ctx, si.MasterAlias)
	if err != nil {
		return vterrors.Wrapf(err, "Cannot read master tablet %v", si.MasterAlias)
	}

	// If using semi-sync, we need to enable it before connecting to master.
	if err := tm.fixSemiSync(tabletType); err != nil {
		return err
	}

	// Set master and start replication.
	if err := tm.MysqlDaemon.SetMaster(ctx, ti.Tablet.MysqlHostname, int(ti.Tablet.MysqlPort), false /* slaveStopBefore */, true /* slaveStartAfter */); err != nil {
		return vterrors.Wrap(err, "MysqlDaemon.SetMaster failed")
	}

	// wait for reliable seconds behind master
	// we have pos where we want to resume from
	// if MasterPosition is the same, that means no writes
	// have happened to master, so we are up-to-date
	// otherwise, wait for replica's Position to change from
	// the initial pos before proceeding
	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()
	remoteCtx, remoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer remoteCancel()
	posStr, err := tmc.MasterPosition(remoteCtx, ti.Tablet)
	if err != nil {
		// It is possible that though MasterAlias is set, the master tablet is unreachable
		// Log a warning and let tablet restore in that case
		// If we had instead considered this fatal, all tablets would crash-loop
		// until a master appears, which would make it impossible to elect a master.
		log.Warningf("Can't get master replication position after restore: %v", err)
		return nil
	}
	masterPos, err := mysql.DecodePosition(posStr)
	if err != nil {
		return vterrors.Wrapf(err, "can't decode master replication position: %q", posStr)
	}

	if !pos.Equal(masterPos) {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			status, err := tm.MysqlDaemon.ReplicationStatus()
			if err != nil {
				return vterrors.Wrap(err, "can't get replication status")
			}
			newPos := status.Position
			if !newPos.Equal(pos) {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func (tm *TabletManager) getLocalMetadataValues(tabletType topodatapb.TabletType) map[string]string {
	tablet := tm.Tablet()
	values := map[string]string{
		"Alias":         topoproto.TabletAliasString(tablet.Alias),
		"ClusterAlias":  fmt.Sprintf("%s.%s", tablet.Keyspace, tablet.Shard),
		"DataCenter":    tablet.Alias.Cell,
		"PromotionRule": "must_not",
	}
	if isMasterEligible(tabletType) {
		values["PromotionRule"] = "neutral"
	}
	return values
}
