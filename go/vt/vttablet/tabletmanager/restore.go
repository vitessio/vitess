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
	"slices"
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

	statsRestoreBackupTime *stats.String
	statsRestorePosition   *stats.String
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
	statsRestorePosition = stats.NewString("RestorePosition")
}

// RestoreBackup is the main entry point for backup restore. It will either
// work, fail gracefully, or return an error in case of a non-recoverable error.
// It takes the action lock so no RPC interferes.
func (tm *TabletManager) RestoreBackup(
	ctx context.Context,
	logger logutil.Logger,
	allowedBackupEngines []string,
	backupTime time.Time,
	deleteBeforeRestore bool,
	dryRun bool,
	mysqlShutdownTimeout time.Duration,
	restoreToPosStr string,
	restoreToTimestamp time.Time,
	waitForBackupInterval time.Duration,
) error {
	// Perform the restore inside of the common outer restore routine.
	return tm.restoreCommonOuter(ctx, logger, deleteBeforeRestore, func() error {
		return tm.restoreBackupOuterLocked(ctx, logger, allowedBackupEngines, backupTime, deleteBeforeRestore,
			dryRun, mysqlShutdownTimeout, restoreToPosStr, restoreToTimestamp, waitForBackupInterval)
	})
}

// restoreCommonOuter acquires the TabletManager's lock, checks that a restore
// is allowed, and then performs the supplied restoreFn. After, the
// vttablet_restore_done hooks is executed.
func (tm *TabletManager) restoreCommonOuter(ctx context.Context, logger logutil.Logger, deleteBeforeRestore bool, restoreFn func() error) error {
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

	// Check whether we're going to restore before changing to RESTORE type,
	// so we keep our PrimaryTermStartTime (if any) if we aren't actually restoring.
	ok, err := mysqlctl.ShouldRestore(ctx, logger, tm.Cnf, tm.MysqlDaemon, topoproto.TabletDbName(tm.Tablet()), deleteBeforeRestore)
	if err != nil {
		return err
	}
	if !ok {
		logger.Infof("Attempting to restore, but mysqld already contains data. Assuming vttablet was just restarted.")
		return nil
	}

	return restoreFn()
}

// restoreBackupOuterLocked parses and validates the supplied parameters, and then
// calls restoreBackupInner inside of restoreInner.
func (tm *TabletManager) restoreBackupOuterLocked(
	ctx context.Context,
	logger logutil.Logger,
	allowedBackupEngines []string,
	backupTime time.Time,
	deleteBeforeRestore bool,
	dryRun bool,
	mysqlShutdownTimeout time.Duration,
	restoreToPosStr string,
	restoreToTimestamp time.Time,
	waitForBackupInterval time.Duration,
) error {
	// Get keyspace.
	tablet := tm.Tablet()
	// Try to restore. Depending on the reason for failure, we may be ok.
	// If we're not ok, return an error and the tm will log.Fatalf,
	// causing the process to be restarted and the restore retried.

	keyspace := tablet.Keyspace
	keyspaceInfo, err := tm.TopoServer.GetKeyspace(ctx, keyspace)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get keyspace %q from topology server", keyspace)
	}

	// For a SNAPSHOT keyspace, we have to look for backups of BaseKeyspace
	// so we will pass the BaseKeyspace in RestoreParams instead of tablet.Keyspace
	if keyspaceInfo.KeyspaceType == topodatapb.KeyspaceType_SNAPSHOT {
		if keyspaceInfo.BaseKeyspace == "" {
			return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, fmt.Sprintf("snapshot keyspace %v has no base_keyspace set", tablet.Keyspace))
		}
		keyspace = keyspaceInfo.BaseKeyspace
		log.Infof("Using base_keyspace %v to restore keyspace %v using a backup time of %v", keyspace, tablet.Keyspace, backupTime)
	}

	// Choose start time.
	startTime := backupTime
	if startTime.IsZero() {
		startTime = protoutil.TimeFromProto(keyspaceInfo.SnapshotTime).UTC()
	}

	// Validate --restore-to-pos and --restore-to-timestamp.
	if restoreToPosStr != "" && !restoreToTimestamp.IsZero() {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--restore-to-pos and --restore-to-timestamp are mutually exclusive")
	}

	// Parse --restore-to-pos.
	var restoreToPos replication.Position
	if restoreToPosStr != "" {
		restoreToPos, _, err = replication.DecodePositionMySQL56(restoreToPosStr)
		if err != nil {
			return vterrors.Wrapf(err, "restore failed: unable to decode --restore-to-pos: %s", restoreToPosStr)
		}
	}

	// Perform the actual restore inside of the common inner restore routine.
	return tm.restoreCommonInnerLocked(ctx, logger, func() (replicationAction, replication.Position, *topodatapb.TabletType, error) {
		return tm.restoreBackupInnerLocked(ctx, logger, allowedBackupEngines, deleteBeforeRestore, dryRun,
			keyspace, keyspaceInfo.KeyspaceType, mysqlShutdownTimeout, restoreToPos, restoreToTimestamp,
			startTime, waitForBackupInterval)
	})
}

// replicationAction is used by replicationInnerFn to signal to replicationInner
// a replication action to perform after a successful restore.
type replicationAction int

const (
	replicationActionNone replicationAction = iota
	replicationActionDisable
	replicationActionInitialize
	replicationActionStart
)

// restoreInnerLockedFn is called by restoreCommonInnerLocked between
// transitions to and from the RESTORE tablet type, and before performing any
// replication actions.
type restoreInnerLockedFn func() (
	// replicationAction signals the replication action to take when the restore succeeds.
	replicationAction replicationAction,
	// replicationPos specifies the replication start position when the
	// replicationAction is replicationActionStart.
	replicationPos replication.Position,
	// overrideTabletType signals the tablet type to transition to when the
	// restore succeeds. When not provided, the previous or init tablet type is
	// used.
	overrideTabletType *topodatapb.TabletType,
	// err indicates whether the restore succeeded.
	err error,
)

// restoreCommonInnerLocked calls the supplied restoreFn after transitioning to
// the RESTORE tablet type.
//
// If the restore fails, the tablet type is reverted to the previous tablet type.
//
// If the call to restoreFn returns a replicationAction, that action is
// performed. If the action fails, the tablet type is reverted to the previous
// tablet type.
//
// Finally, if the restore succeeds, the tablet type is reverted either to the
// previous tablet type, or, if the call to restoreFn returns an
// overrideTabletType, the tablet type is changed to that override.
//
// It is only safe to call this from restoreCommonOuter, or another function
// that has obtained the TabletManager's lock.
func (tm *TabletManager) restoreCommonInnerLocked(
	ctx context.Context,
	logger logutil.Logger,
	restoreFn restoreInnerLockedFn,
) error {
	// Transition to RESTORE tablet type.
	prevTabletType := tm.Tablet().Type
	if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_RESTORE, DBActionNone); err != nil {
		return vterrors.Wrap(err, `failed to transition to "restore" tablet type`)
	}

	// Perform the restore.
	replicationAction, replicationPos, overrideTabletType, err := restoreFn()

	// On error, revert to the previous tablet type.
	if err != nil {
		if err := tm.tmState.ChangeTabletType(context.Background(), prevTabletType, DBActionNone); err != nil {
			logger.Errorf("failed to revert to previous tablet type: %v", err)
		}
		return err
	}

	// Disable, initialize, or start replication. Note that, in all cases, if we
	// fail to perform the replication action, we do not transition the tablet type.
	switch replicationAction {
	case replicationActionDisable:
		logger.Infof("Restore: disabling replication")
		if err := tm.disableReplication(context.Background()); err != nil {
			return vterrors.Wrap(err, "failed to disable replication")
		}
	case replicationActionInitialize:
		_, err := tm.initializeReplication(ctx, prevTabletType)
		if err != nil {
			return err
		}
	case replicationActionStart:
		logger.Infof("Restore: starting replication at position %v", replicationPos)
		if err := tm.startReplication(ctx, replicationPos, prevTabletType); err != nil {
			return vterrors.Wrap(err, "failed to start replication")
		}
	default:
	}

	// Change to the next tablet type.
	nextTabletType := prevTabletType
	switch {
	case overrideTabletType != nil:
		// If we are provided with an override, use that.
		nextTabletType = *overrideTabletType
	case prevTabletType == topodatapb.TabletType_BACKUP || prevTabletType == topodatapb.TabletType_RESTORE && initTabletType != "":
		// If the original tablet type was BACKUP or RESTORE, use the init-tablet-type.
		initType, err := topoproto.ParseTabletType(initTabletType)
		if err == nil {
			nextTabletType = initType
		}
	}
	if err := tm.tmState.ChangeTabletType(context.Background(), nextTabletType, DBActionNone); err != nil {
		logger.Errorf("failed to transition to next tablet type %q: %v", topoproto.TabletTypeLString(nextTabletType), err)
	}
	return nil
}

// restoreBackupInnerLocked restores a backup. It is only appropriate to call
// within the context of restoreCommonInnerLocked, or another function that
// transitions to and from the RESTORE tablet type, and performs the replication
// action requested by this functions return values.
func (tm *TabletManager) restoreBackupInnerLocked(
	ctx context.Context,
	logger logutil.Logger,
	allowedBackupEngines []string,
	deleteBeforeRestore bool,
	dryRun bool,
	keyspace string,
	keyspaceType topodatapb.KeyspaceType,
	mysqlShutdownTimeout time.Duration,
	restoreToPos replication.Position,
	restoreToTimestamp time.Time,
	startTime time.Time,
	waitForBackupInterval time.Duration,
) (
	replicationAction replicationAction,
	replicationPos replication.Position,
	overrideTabletType *topodatapb.TabletType,
	err error,
) {
	tablet := tm.Tablet()

	// Perform the restore. Loop until a backup exists, unless we were told to give up immediately.
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
		DryRun:               dryRun,
		Stats:                backupstats.RestoreStats(),
		MysqlShutdownTimeout: mysqlShutdownTimeout,
		AllowedBackupEngines: allowedBackupEngines,
		RestoreToPos:         restoreToPos,
		RestoreToTimestamp:   restoreToTimestamp,
	}
	var backupManifest *mysqlctl.BackupManifest
	for {
		backupManifest, err = mysqlctl.Restore(ctx, params)
		if backupManifest != nil {
			statsRestorePosition.Set(replication.EncodePosition(backupManifest.Position))
			statsRestoreBackupTime.Set(backupManifest.BackupTime)
		}
		logger.Infof("Restore: got a restore manifest: %v, err=%v, waitForBackupInterval=%v", backupManifest, err, waitForBackupInterval)
		if waitForBackupInterval == 0 {
			break
		}
		// Break if no errors.
		if err == nil {
			break
		}
		// Retry a specific set of errors. The rest we handle immediately.
		if !slices.Contains([]error{mysqlctl.ErrNoBackup, mysqlctl.ErrNoCompleteBackup}, err) {
			break
		}
		log.Infof("No backup found. Waiting %v (from -wait-for-backup-interval flag) to check again.", waitForBackupInterval)
		select {
		case <-ctx.Done():
			return replicationAction, replicationPos, overrideTabletType, ctx.Err()
		case <-time.After(waitForBackupInterval):
		}
	}

	// Handle error.
	if err == nil && backupManifest == nil {
		err = vterrors.New(vtrpcpb.Code_INTERNAL, "no backup manifest")
	}
	if err != nil && err != mysqlctl.ErrNoBackup {
		return replicationAction, replicationPos, overrideTabletType, vterrors.Wrap(err, "can't restore backup")
	}

	// Choose replication action.
	switch {
	case params.IsIncrementalRecovery():
		// The whole point of point-in-time recovery is that we want to restore
		// up to a given position, and to NOT proceed from that position. We
		// want to disable replication and NOT let the replica catch up with the
		// primary.
		replicationAction = replicationActionDisable
	case keyspaceType == topodatapb.KeyspaceType_NORMAL:
		// Reconnect to primary only for "NORMAL" keyspaces.
		replicationAction = replicationActionStart
		replicationPos = backupManifest.Position
	case err == mysqlctl.ErrNoBackup:
		// Starting with empty database.
		// We just need to initialize replication
		replicationAction = replicationActionInitialize
	case params.DryRun:
		// Do nothing here.
		params.Logger.Infof("Dry run. No changes made")
		replicationAction = replicationActionNone
	default:
	}

	// Override tablet type.
	if params.IsIncrementalRecovery() && !params.DryRun {
		params.Logger.Infof("Restore: will set tablet type to DRAINED as this is a point in time recovery")
		drained := topodatapb.TabletType_DRAINED
		overrideTabletType = &drained
	}

	return replicationAction, replicationPos, overrideTabletType, nil
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
