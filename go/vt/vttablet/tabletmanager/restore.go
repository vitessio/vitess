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
	"errors"
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"

	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file handles the initial backup restore upon startup.
// It is only enabled if restore-from-backup is set.

var (
	restoreFromBackup               bool
	restoreFromBackupAllowedEngines []string
	restoreFromBackupTsStr          string
	restoreConcurrency              = 4
	waitForBackupInterval           time.Duration

	statsRestoreBackupTime     *stats.String
	statsRestoreBackupPosition *stats.String
)

func registerRestoreFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &restoreFromBackup, "restore-from-backup", restoreFromBackup, "(init restore parameter) will check BackupStorage for a recent backup at startup and start there")
	fs.StringSliceVar(&restoreFromBackupAllowedEngines, "restore-from-backup-allowed-engines", restoreFromBackupAllowedEngines, "(init restore parameter) if set, only backups taken with the specified engines are eligible to be restored")
	utils.SetFlagStringVar(fs, &restoreFromBackupTsStr, "restore-from-backup-ts", restoreFromBackupTsStr, "(init restore parameter) if set, restore the latest backup taken at or before this timestamp. Example: '2021-04-29.133050'")
	utils.SetFlagIntVar(fs, &restoreConcurrency, "restore-concurrency", restoreConcurrency, "(init restore parameter) how many concurrent files to restore at once")
	utils.SetFlagDurationVar(fs, &waitForBackupInterval, "wait-for-backup-interval", waitForBackupInterval, "(init restore parameter) if this is greater than 0, instead of starting up empty when no backups are found, keep checking at this interval for a backup to appear")
}

var (
	// Flags for incremental restore (PITR) - new iteration
	restoreToTimestampStr string
	restoreToPos          string
)

func registerIncrementalRestoreFlags(fs *pflag.FlagSet) {
	fs.StringVar(&restoreToTimestampStr, "restore-to-timestamp", restoreToTimestampStr, "(init incremental restore parameter) if set, run a point in time recovery that restores up to the given timestamp, if possible. Given timestamp in RFC3339 format. Example: '2006-01-02T15:04:05Z07:00'")
	fs.StringVar(&restoreToPos, "restore-to-pos", restoreToPos, "(init incremental restore parameter) if set, run a point in time recovery that ends with the given position. This will attempt to use one full backup followed by zero or more incremental backups")
}

func init() {
	servenv.OnParseFor("vtcombo", registerRestoreFlags)
	servenv.OnParseFor("vttablet", registerRestoreFlags)

	servenv.OnParseFor("vtcombo", registerIncrementalRestoreFlags)
	servenv.OnParseFor("vttablet", registerIncrementalRestoreFlags)

	statsRestoreBackupTime = stats.NewString("RestoredBackupTime")
	statsRestoreBackupPosition = stats.NewString("RestorePosition")
}

// RestoreData is the main entry point for backup restore.
// It will either work, fail gracefully, or return
// an error in case of a non-recoverable error.
// It takes the action lock so no RPC interferes.
func (tm *TabletManager) RestoreData(
	ctx context.Context,
	logger logutil.Logger,
	waitForBackupInterval time.Duration,
	deleteBeforeRestore bool,
	backupTime time.Time,
	restoreToTimetamp time.Time,
	restoreToPos string,
	allowedBackupEngines []string,
	mysqlShutdownTimeout time.Duration) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()
	if tm.Cnf == nil {
		return errors.New("cannot perform restore without my.cnf, please restart vttablet with a my.cnf file specified")
	}

	var (
		err       error
		startTime time.Time
	)

	defer func() {
		stopTime := time.Now()

		h := hook.NewSimpleHook("vttablet_restore_done")
		h.ExtraEnv = tm.hookExtraEnv()
		h.ExtraEnv["TM_RESTORE_DATA_START_TS"] = startTime.UTC().Format(time.RFC3339)
		h.ExtraEnv["TM_RESTORE_DATA_STOP_TS"] = stopTime.UTC().Format(time.RFC3339)
		h.ExtraEnv["TM_RESTORE_DATA_DURATION"] = stopTime.Sub(startTime).String()

		if err != nil {
			h.ExtraEnv["TM_RESTORE_DATA_ERROR"] = err.Error()
		}

		// vttablet_restore_done is best-effort (for now?).
		go func() {
			// Package vthook already logs the stdout/stderr of hooks when they
			// are run, so we don't duplicate that here.
			hr := h.Execute()
			switch hr.ExitStatus {
			case hook.HOOK_SUCCESS:
			case hook.HOOK_DOES_NOT_EXIST:
				log.Info("No vttablet_restore_done hook.")
			default:
				log.Warning("vttablet_restore_done hook failed")
			}
		}()
	}()

	startTime = time.Now()

	req := &tabletmanagerdatapb.RestoreFromBackupRequest{
		BackupTime:           protoutil.TimeToProto(backupTime),
		RestoreToPos:         restoreToPos,
		RestoreToTimestamp:   protoutil.TimeToProto(restoreToTimetamp),
		AllowedBackupEngines: allowedBackupEngines,
	}
	err = tm.restoreDataLocked(ctx, logger, waitForBackupInterval, deleteBeforeRestore, req, mysqlShutdownTimeout)
	if err != nil {
		return err
	}
	return nil
}

func (tm *TabletManager) restoreDataLocked(ctx context.Context, logger logutil.Logger, waitForBackupInterval time.Duration, deleteBeforeRestore bool, request *tabletmanagerdatapb.RestoreFromBackupRequest, mysqlShutdownTimeout time.Duration) error {
	tablet := tm.Tablet()
	originalType := tablet.Type
	// Try to restore. Depending on the reason for failure, we may be ok.
	// If we're not ok, return an error and the tm will log.Fatalf,
	// causing the process to be restarted and the restore retried.

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
		log.Infof("Using base_keyspace %v to restore keyspace %v using a backup time of %v", keyspace, tablet.Keyspace, protoutil.TimeFromProto(request.BackupTime).UTC())
	}

	startTime := protoutil.TimeFromProto(request.BackupTime).UTC()
	if startTime.IsZero() {
		startTime = protoutil.TimeFromProto(keyspaceInfo.SnapshotTime).UTC()
	}

	params := mysqlctl.RestoreParams{
		Cnf:                  tm.Cnf,
		Mysqld:               tm.MysqlDaemon,
		Logger:               logger,
		Concurrency:          restoreConcurrency,
		HookExtraEnv:         tm.hookExtraEnv(),
		DeleteBeforeRestore:  deleteBeforeRestore,
		DbName:               topoproto.TabletDbName(tablet),
		Keyspace:             keyspace,
		Shard:                tablet.Shard,
		StartTime:            startTime,
		DryRun:               request.DryRun,
		Stats:                backupstats.RestoreStats(),
		MysqlShutdownTimeout: mysqlShutdownTimeout,
		AllowedBackupEngines: request.AllowedBackupEngines,
	}
	restoreToTimestamp := protoutil.TimeFromProto(request.RestoreToTimestamp).UTC()
	if request.RestoreToPos != "" && !restoreToTimestamp.IsZero() {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--restore-to-pos and --restore-to-timestamp are mutually exclusive")
	}
	if request.RestoreToPos != "" {
		pos, _, err := replication.DecodePositionMySQL56(request.RestoreToPos)
		if err != nil {
			return vterrors.Wrapf(err, "restore failed: unable to decode --restore-to-pos: %s", request.RestoreToPos)
		}
		params.RestoreToPos = pos
	}
	if !restoreToTimestamp.IsZero() {
		// Restore to given timestamp
		params.RestoreToTimestamp = restoreToTimestamp
	}
	params.Logger.Infof("Restore: original tablet type=%v", originalType)

	// Check whether we're going to restore before changing to RESTORE type,
	// so we keep our PrimaryTermStartTime (if any) if we aren't actually restoring.
	ok, err := mysqlctl.ShouldRestore(ctx, params)
	if err != nil {
		return err
	}
	if !ok {
		params.Logger.Infof("Attempting to restore, but mysqld already contains data. Assuming vttablet was just restarted.")
		return nil
	}
	// We should not become primary after restore, because that would incorrectly
	// start a new primary term, and it's likely our data dir will be out of date.
	if originalType == topodatapb.TabletType_PRIMARY {
		originalType = tm.baseTabletType
	}
	if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_RESTORE, DBActionNone); err != nil {
		return err
	}
	// Loop until a backup exists, unless we were told to give up immediately.
	var backupManifest *mysqlctl.BackupManifest
	for {
		backupManifest, err = mysqlctl.Restore(ctx, params)
		if backupManifest != nil {
			statsRestoreBackupPosition.Set(replication.EncodePosition(backupManifest.Position))
			statsRestoreBackupTime.Set(backupManifest.BackupTime)
		}
		params.Logger.Infof("Restore: got a restore manifest: %v, err=%v, waitForBackupInterval=%v", backupManifest, err, waitForBackupInterval)
		if waitForBackupInterval == 0 {
			break
		}
		// We only retry a specific set of errors. The rest we return immediately.
		if err != mysqlctl.ErrNoBackup && err != mysqlctl.ErrNoCompleteBackup {
			break
		}

		log.Infof("No backup found. Waiting %v (from -wait-for-backup-interval flag) to check again.", waitForBackupInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitForBackupInterval):
		}
	}

	var pos replication.Position
	if backupManifest != nil {
		pos = backupManifest.Position
		params.Logger.Infof("Restore: pos=%v", replication.EncodePosition(pos))
	}
	switch {
	case err == nil && backupManifest != nil:
		// Starting from here we won't be able to recover if we get stopped by a cancelled
		// context. Thus we use the background context to get through to the finish.
		if params.IsIncrementalRecovery() && !params.DryRun {
			// The whole point of point-in-time recovery is that we want to restore up to a given position,
			// and to NOT proceed from that position. We want to disable replication and NOT let the replica catch
			// up with the primary.
			params.Logger.Infof("Restore: disabling replication")
			if err := tm.disableReplication(context.Background()); err != nil {
				return err
			}
		} else if keyspaceInfo.KeyspaceType == topodatapb.KeyspaceType_NORMAL {
			// Reconnect to primary only for "NORMAL" keyspaces
			params.Logger.Infof("Restore: starting replication at position %v", pos)
			if err := tm.startReplication(ctx, pos, originalType); err != nil {
				return err
			}
		}
	case err == mysqlctl.ErrNoBackup:
		// Starting with empty database.
		// We just need to initialize replication
		_, err := tm.initializeReplication(ctx, originalType)
		if err != nil {
			return err
		}
	case err == nil && params.DryRun:
		// Do nothing here, let the rest of code run
		params.Logger.Infof("Dry run. No changes made")
	default:
		bgCtx := context.Background()
		// If anything failed, we should reset the original tablet type
		if err := tm.tmState.ChangeTabletType(bgCtx, originalType, DBActionNone); err != nil {
			log.Errorf("Could not change back to original tablet type %v: %v", originalType, err)
		}
		return vterrors.Wrap(err, "Can't restore backup")
	}

	// If we had type BACKUP or RESTORE it's better to set our type to the init-tablet-type to make result of the restore
	// similar to completely clean start from scratch.
	if (originalType == topodatapb.TabletType_BACKUP || originalType == topodatapb.TabletType_RESTORE) && initTabletType != "" {
		initType, err := topoproto.ParseTabletType(initTabletType)
		if err == nil {
			originalType = initType
		}
	}
	if params.IsIncrementalRecovery() && !params.DryRun {
		// override
		params.Logger.Infof("Restore: will set tablet type to DRAINED as this is a point in time recovery")
		originalType = topodatapb.TabletType_DRAINED
	}
	params.Logger.Infof("Restore: changing tablet type to %v for %s", originalType, tm.tabletAlias.String())
	// Change type back to original type if we're ok to serve.
	bgCtx := context.Background()
	return tm.tmState.ChangeTabletType(bgCtx, originalType, DBActionNone)
}

// disableReplication stops and resets replication on the mysql server. It moreover sets impossible replication
// source params, so that the replica can't possibly reconnect. It would take a `CHANGE [MASTER|REPLICATION SOURCE] TO ...` to
// make the mysql server replicate again (available via tm.MysqlDaemon.SetReplicationPosition)
func (tm *TabletManager) disableReplication(ctx context.Context) error {
	if err := tm.MysqlDaemon.StopReplication(ctx, nil); err != nil {
		return vterrors.Wrap(err, "failed to stop replication")
	}
	if err := tm.MysqlDaemon.ResetReplicationParameters(ctx); err != nil {
		return vterrors.Wrap(err, "failed to reset replication")
	}

	if err := tm.MysqlDaemon.SetReplicationSource(ctx, "//", 0, 0, false, true); err != nil {
		return vterrors.Wrap(err, "failed to disable replication")
	}

	return nil
}

func (tm *TabletManager) startReplication(ctx context.Context, pos replication.Position, tabletType topodatapb.TabletType) error {
	// The first three steps of stopping replication, and setting the replication position,
	// we want to do even if the context expires, so we use a background context for these tasks.
	if err := tm.MysqlDaemon.StopReplication(context.Background(), nil); err != nil {
		return vterrors.Wrap(err, "failed to stop replication")
	}
	if err := tm.MysqlDaemon.ResetReplicationParameters(context.Background()); err != nil {
		return vterrors.Wrap(err, "failed to reset replication")
	}

	// Set the position at which to resume from the primary.
	if err := tm.MysqlDaemon.SetReplicationPosition(context.Background(), pos); err != nil {
		return vterrors.Wrap(err, "failed to set replication position")
	}

	primaryPosStr, err := tm.initializeReplication(ctx, tabletType)
	// If we ran into an error while initializing replication, then there is no point in waiting for catch-up.
	// Also, if there is no primary tablet in the shard, we don't need to proceed further.
	if err != nil || primaryPosStr == "" {
		return err
	}

	primaryPos, err := replication.DecodePosition(primaryPosStr)
	if err != nil {
		return vterrors.Wrapf(err, "can't decode primary replication position: %q", primaryPos)
	}

	if !pos.Equal(primaryPos) {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			status, err := tm.MysqlDaemon.ReplicationStatus(ctx)
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
