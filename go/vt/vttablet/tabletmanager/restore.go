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
	"io"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"

	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file handles the initial backup restore upon startup.
// It is only enabled if restore_from_backup is set.

var (
	restoreFromBackup      bool
	restoreFromBackupTsStr string
	restoreConcurrency     = 4
	waitForBackupInterval  time.Duration

	statsRestoreBackupTime     *stats.String
	statsRestoreBackupPosition *stats.String
)

func registerRestoreFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&restoreFromBackup, "restore_from_backup", restoreFromBackup, "(init restore parameter) will check BackupStorage for a recent backup at startup and start there")
	fs.StringVar(&restoreFromBackupTsStr, "restore_from_backup_ts", restoreFromBackupTsStr, "(init restore parameter) if set, restore the latest backup taken at or before this timestamp. Example: '2021-04-29.133050'")
	fs.IntVar(&restoreConcurrency, "restore_concurrency", restoreConcurrency, "(init restore parameter) how many concurrent files to restore at once")
	fs.DurationVar(&waitForBackupInterval, "wait_for_backup_interval", waitForBackupInterval, "(init restore parameter) if this is greater than 0, instead of starting up empty when no backups are found, keep checking at this interval for a backup to appear")
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

var (
	// Flags for PITR - old iteration
	binlogHost           string
	binlogPort           int
	binlogUser           string
	binlogPwd            string
	timeoutForGTIDLookup = 60 * time.Second
	binlogSslCa          string
	binlogSslCert        string
	binlogSslKey         string
	binlogSslServerName  string
)

func registerPointInTimeRestoreFlags(fs *pflag.FlagSet) {
	fs.StringVar(&binlogHost, "binlog_host", binlogHost, "PITR restore parameter: hostname/IP of binlog server.")
	fs.IntVar(&binlogPort, "binlog_port", binlogPort, "PITR restore parameter: port of binlog server.")
	fs.StringVar(&binlogUser, "binlog_user", binlogUser, "PITR restore parameter: username of binlog server.")
	fs.StringVar(&binlogPwd, "binlog_password", binlogPwd, "PITR restore parameter: password of binlog server.")
	fs.DurationVar(&timeoutForGTIDLookup, "pitr_gtid_lookup_timeout", timeoutForGTIDLookup, "PITR restore parameter: timeout for fetching gtid from timestamp.")
	fs.StringVar(&binlogSslCa, "binlog_ssl_ca", binlogSslCa, "PITR restore parameter: Filename containing TLS CA certificate to verify binlog server TLS certificate against.")
	fs.StringVar(&binlogSslCert, "binlog_ssl_cert", binlogSslCert, "PITR restore parameter: Filename containing mTLS client certificate to present to binlog server as authentication.")
	fs.StringVar(&binlogSslKey, "binlog_ssl_key", binlogSslKey, "PITR restore parameter: Filename containing mTLS client private key for use in binlog server authentication.")
	fs.StringVar(&binlogSslServerName, "binlog_ssl_server_name", binlogSslServerName, "PITR restore parameter: TLS server name (common name) to verify against for the binlog server we are connecting to (If not set: use the hostname or IP supplied in --binlog_host).")
}

func init() {
	servenv.OnParseFor("vtcombo", registerRestoreFlags)
	servenv.OnParseFor("vttablet", registerRestoreFlags)

	servenv.OnParseFor("vtcombo", registerIncrementalRestoreFlags)
	servenv.OnParseFor("vttablet", registerIncrementalRestoreFlags)

	servenv.OnParseFor("vtcombo", registerPointInTimeRestoreFlags)
	servenv.OnParseFor("vttablet", registerPointInTimeRestoreFlags)

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
	mysqlShutdownTimeout time.Duration) error {
	if err := tm.lock(ctx); err != nil {
		return err
	}
	defer tm.unlock()
	if tm.Cnf == nil {
		return fmt.Errorf("cannot perform restore without my.cnf, please restart vttablet with a my.cnf file specified")
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
		BackupTime:         protoutil.TimeToProto(backupTime),
		RestoreToPos:       restoreToPos,
		RestoreToTimestamp: protoutil.TimeToProto(restoreToTimetamp),
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
	}
	restoreToTimestamp := protoutil.TimeFromProto(request.RestoreToTimestamp).UTC()
	if request.RestoreToPos != "" && !restoreToTimestamp.IsZero() {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--restore-to-pos and --restore-to-timestamp are mutually exclusive")
	}
	if request.RestoreToPos != "" {
		pos, err := replication.DecodePosition(request.RestoreToPos)
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

		log.Infof("No backup found. Waiting %v (from -wait_for_backup_interval flag) to check again.", waitForBackupInterval)
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
	// If SnapshotTime is set , then apply the incremental change
	if keyspaceInfo.SnapshotTime != nil {
		params.Logger.Infof("Restore: Restoring to time %v from binlog", keyspaceInfo.SnapshotTime)
		err = tm.restoreToTimeFromBinlog(ctx, pos, keyspaceInfo.SnapshotTime)
		if err != nil {
			log.Errorf("unable to restore to the specified time %s, error : %v", keyspaceInfo.SnapshotTime.String(), err)
			return nil
		}
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
			if err := tm.startReplication(context.Background(), pos, originalType); err != nil {
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

	// If we had type BACKUP or RESTORE it's better to set our type to the init_tablet_type to make result of the restore
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

// restoreToTimeFromBinlog restores to the snapshot time of the keyspace
// currently this works with mysql based database only (as it uses mysql specific queries for restoring)
func (tm *TabletManager) restoreToTimeFromBinlog(ctx context.Context, pos replication.Position, restoreTime *vttime.Time) error {
	// validate the minimal settings necessary for connecting to binlog server
	if binlogHost == "" || binlogPort <= 0 || binlogUser == "" {
		log.Warning("invalid binlog server setting, restoring to last available backup.")
		return nil
	}

	timeoutCtx, cancelFnc := context.WithTimeout(ctx, timeoutForGTIDLookup)
	defer cancelFnc()

	afterGTIDPos, beforeGTIDPos, err := tm.getGTIDFromTimestamp(timeoutCtx, pos, restoreTime.Seconds)
	if err != nil {
		return err
	}

	if afterGTIDPos == "" && beforeGTIDPos == "" {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, fmt.Sprintf("unable to fetch the GTID for the specified time - %s", restoreTime.String()))
	} else if afterGTIDPos == "" && beforeGTIDPos != "" {
		log.Info("no afterGTIDPos found, which implies we reached the end of all GTID events")
	}

	log.Infof("going to restore upto the GTID - %s", afterGTIDPos)
	// when we don't have before GTID, we will take it as current backup pos's last GTID
	// this is case where someone tries to restore just to the 1st event after backup
	if beforeGTIDPos == "" {
		beforeGTIDPos = pos.GTIDSet.Last()
	}
	err = tm.catchupToGTID(timeoutCtx, afterGTIDPos, beforeGTIDPos)
	if err != nil {
		return vterrors.Wrapf(err, "unable to replicate upto desired GTID : %s", afterGTIDPos)
	}

	return nil
}

// getGTIDFromTimestamp computes 2 GTIDs based on restoreTime
// afterPos is the GTID of the first event at or after restoreTime.
// beforePos is the GTID of the last event before restoreTime. This is the GTID upto which replication will be applied
// afterPos can be used directly in the query `START SLAVE UNTIL SQL_BEFORE_GTIDS = ”`
// beforePos will be used to check if replication was able to catch up from the binlog server
func (tm *TabletManager) getGTIDFromTimestamp(ctx context.Context, pos replication.Position, restoreTime int64) (afterPos string, beforePos string, err error) {
	connParams := &mysql.ConnParams{
		Host:       binlogHost,
		Port:       binlogPort,
		Uname:      binlogUser,
		SslCa:      binlogSslCa,
		SslCert:    binlogSslCert,
		SslKey:     binlogSslKey,
		ServerName: binlogSslServerName,
	}
	if binlogPwd != "" {
		connParams.Pass = binlogPwd
	}
	if binlogSslCa != "" || binlogSslCert != "" {
		connParams.EnableSSL()
	}
	dbCfgs := &dbconfigs.DBConfigs{
		Host: connParams.Host,
		Port: connParams.Port,
	}
	dbCfgs.SetDbParams(*connParams, *connParams, *connParams)
	vsClient := vreplication.NewReplicaConnector(tm.Env, connParams)

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	// get current lastPos of binlog server, so that if we hit that in vstream, we'll return from there
	binlogConn, err := mysql.Connect(ctx, connParams)
	if err != nil {
		return "", "", err
	}
	defer binlogConn.Close()
	lastPos, err := binlogConn.PrimaryPosition()
	if err != nil {
		return "", "", err
	}

	gtidsChan := make(chan []string, 1)

	go func() {
		err := vsClient.VStream(ctx, replication.EncodePosition(pos), filter, func(events []*binlogdatapb.VEvent) error {
			for _, event := range events {
				if event.Gtid != "" {
					// check if we reached the lastPos then return
					eventPos, err := replication.DecodePosition(event.Gtid)
					if err != nil {
						return err
					}

					if event.Timestamp >= restoreTime {
						afterPos = event.Gtid
						gtidsChan <- []string{event.Gtid, beforePos}
						return io.EOF
					}

					if eventPos.AtLeast(lastPos) {
						gtidsChan <- []string{"", beforePos}
						return io.EOF
					}
					beforePos = event.Gtid
				}
			}
			return nil
		})
		if err != nil && err != io.EOF {
			log.Warningf("Error using VStream to find timestamp for GTID position: %v error: %v", pos, err)
			gtidsChan <- []string{"", ""}
		}
	}()
	defer vsClient.Close()
	select {
	case val := <-gtidsChan:
		return val[0], val[1], nil
	case <-ctx.Done():
		log.Warningf("Can't find the GTID from restore time stamp, exiting.")
		return "", beforePos, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "unable to find GTID from the snapshot time as context timed out")
	}
}

// catchupToGTID replicates upto specified GTID from binlog server
//
// copies the data from binlog server by pointing to as replica
// waits till all events to GTID replicated
// once done, it will reset the replication
func (tm *TabletManager) catchupToGTID(ctx context.Context, afterGTIDPos string, beforeGTIDPos string) error {
	var afterGTIDStr string
	if afterGTIDPos != "" {
		afterGTIDParsed, err := replication.DecodePosition(afterGTIDPos)
		if err != nil {
			return err
		}
		afterGTIDStr = afterGTIDParsed.GTIDSet.Last()
	}

	beforeGTIDPosParsed, err := replication.DecodePosition(beforeGTIDPos)
	if err != nil {
		return err
	}

	// it uses mysql specific queries here
	cmds := []string{
		"STOP SLAVE FOR CHANNEL '' ",
		"STOP SLAVE IO_THREAD FOR CHANNEL ''",
	}

	if binlogSslCa != "" || binlogSslCert != "" {
		// We need to use TLS
		cmd := fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1, MASTER_SSL=1", binlogHost, binlogPort, binlogUser, binlogPwd)
		if binlogSslCa != "" {
			cmd += fmt.Sprintf(", MASTER_SSL_CA='%s'", binlogSslCa)
		}
		if binlogSslCert != "" {
			cmd += fmt.Sprintf(", MASTER_SSL_CERT='%s'", binlogSslCert)
		}
		if binlogSslKey != "" {
			cmd += fmt.Sprintf(", MASTER_SSL_KEY='%s'", binlogSslKey)
		}
		cmds = append(cmds, cmd+";")
	} else {
		// No TLS
		cmds = append(cmds, fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1;", binlogHost, binlogPort, binlogUser, binlogPwd))
	}

	if afterGTIDPos == "" { // when the there is no afterPos, that means need to replicate completely
		cmds = append(cmds, "START SLAVE")
	} else {
		cmds = append(cmds, fmt.Sprintf("START SLAVE UNTIL SQL_BEFORE_GTIDS = '%s'", afterGTIDStr))
	}

	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, fmt.Sprintf("failed to restart the replication until %s GTID", afterGTIDStr))
	}
	log.Infof("Waiting for position to reach", beforeGTIDPosParsed.GTIDSet.Last())
	// Could not use `agent.MysqlDaemon.WaitSourcePos` as replication is stopped with `START SLAVE UNTIL SQL_BEFORE_GTIDS`
	// this is as per https://dev.mysql.com/doc/refman/5.6/en/start-slave.html
	// We need to wait until replication catches upto the specified afterGTIDPos
	chGTIDCaughtup := make(chan bool)
	go func() {
		timeToWait := time.Now().Add(timeoutForGTIDLookup)
		for time.Now().Before(timeToWait) {
			pos, err := tm.MysqlDaemon.PrimaryPosition()
			if err != nil {
				chGTIDCaughtup <- false
			}

			if pos.AtLeast(beforeGTIDPosParsed) {
				chGTIDCaughtup <- true
			}
			select {
			case <-ctx.Done():
				chGTIDCaughtup <- false
			default:
				time.Sleep(300 * time.Millisecond)
			}
		}
	}()
	select {
	case resp := <-chGTIDCaughtup:
		if resp {
			cmds := []string{
				"STOP SLAVE",
				"RESET SLAVE ALL",
			}
			if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
				return vterrors.Wrap(err, "failed to stop replication")
			}
			return nil
		}
		return vterrors.Wrap(err, "error while fetching the current GTID position")
	case <-ctx.Done():
		log.Warningf("Could not copy up to GTID.")
		return vterrors.Wrapf(err, "context timeout while restoring up to specified GTID - %s", beforeGTIDPos)
	}
}

// disableReplication stops and resets replication on the mysql server. It moreover sets impossible replication
// source params, so that the replica can't possibly reconnect. It would take a `CHANGE [MASTER|REPLICATION SOURCE] TO ...` to
// make the mysql server replicate again (available via tm.MysqlDaemon.SetReplicationPosition)
func (tm *TabletManager) disableReplication(ctx context.Context) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget primary host:port.
	}
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, "failed to reset replication")
	}
	if err := tm.MysqlDaemon.SetReplicationSource(ctx, "//", 0, false /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
		return vterrors.Wrap(err, "failed to disable replication")
	}

	return nil
}

func (tm *TabletManager) startReplication(ctx context.Context, pos replication.Position, tabletType topodatapb.TabletType) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget primary host:port.
	}
	if err := tm.MysqlDaemon.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, "failed to reset replication")
	}

	// Set the position at which to resume from the primary.
	if err := tm.MysqlDaemon.SetReplicationPosition(ctx, pos); err != nil {
		return vterrors.Wrap(err, "failed to set replication position")
	}

	primary, err := tm.initializeReplication(ctx, tabletType)
	// If we ran into an error while initializing replication, then there is no point in waiting for catch-up.
	// Also, if there is no primary tablet in the shard, we don't need to proceed further.
	if err != nil || primary == nil {
		return err
	}

	// wait for reliable replication_lag_seconds
	// we have pos where we want to resume from
	// if PrimaryPosition is the same, that means no writes
	// have happened to primary, so we are up-to-date
	// otherwise, wait for replica's Position to change from
	// the initial pos before proceeding
	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()
	remoteCtx, remoteCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer remoteCancel()
	posStr, err := tmc.PrimaryPosition(remoteCtx, primary.Tablet)
	if err != nil {
		// It is possible that though PrimaryAlias is set, the primary tablet is unreachable
		// Log a warning and let tablet restore in that case
		// If we had instead considered this fatal, all tablets would crash-loop
		// until a primary appears, which would make it impossible to elect a primary.
		log.Warningf("Can't get primary replication position after restore: %v", err)
		return nil
	}
	primaryPos, err := replication.DecodePosition(posStr)
	if err != nil {
		return vterrors.Wrapf(err, "can't decode primary replication position: %q", posStr)
	}

	if !pos.Equal(primaryPos) {
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
