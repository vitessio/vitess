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
	"io"
	"time"

	"vitess.io/vitess/go/vt/proto/vttime"

	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	"vitess.io/vitess/go/vt/dbconfigs"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"context"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file handles the initial backup restore upon startup.
// It is only enabled if restore_from_backup is set.

var (
	restoreFromBackup     = flag.Bool("restore_from_backup", false, "(init restore parameter) will check BackupStorage for a recent backup at startup and start there")
	restoreConcurrency    = flag.Int("restore_concurrency", 4, "(init restore parameter) how many concurrent files to restore at once")
	waitForBackupInterval = flag.Duration("wait_for_backup_interval", 0, "(init restore parameter) if this is greater than 0, instead of starting up empty when no backups are found, keep checking at this interval for a backup to appear")

	// Flags for PITR
	binlogHost           = flag.String("binlog_host", "", "PITR restore parameter: hostname/IP of binlog server.")
	binlogPort           = flag.Int("binlog_port", 0, "PITR restore parameter: port of binlog server.")
	binlogUser           = flag.String("binlog_user", "", "PITR restore parameter: username of binlog server.")
	binlogPwd            = flag.String("binlog_password", "", "PITR restore parameter: password of binlog server.")
	timeoutForGTIDLookup = flag.Duration("pitr_gtid_lookup_timeout", 60*time.Second, "PITR restore parameter: timeout for fetching gtid from timestamp.")
	binlogSslCa          = flag.String("binlog_ssl_ca", "", "PITR restore parameter: Filename containing TLS CA certificate to verify binlog server TLS certificate against.")
	binlogSslCert        = flag.String("binlog_ssl_cert", "", "PITR restore parameter: Filename containing mTLS client certificate to present to binlog server as authentication.")
	binlogSslKey         = flag.String("binlog_ssl_key", "", "PITR restore parameter: Filename containing mTLS client private key for use in binlog server authentication.")
	binlogSslServerName  = flag.String("binlog_ssl_server_name", "", "PITR restore parameter: TLS server name (common name) to verify against for the binlog server we are connecting to (If not set: use the hostname or IP supplied in -binlog_host).")
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
	// Tell Orchestrator we're stopped on purpose for some Vitess task.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.BeginMaintenance(tm.Tablet(), "vttablet has been told to Restore"); err != nil {
			log.Warningf("Orchestrator BeginMaintenance failed: %v", err)
		}
	}()
	err := tm.restoreDataLocked(ctx, logger, waitForBackupInterval, deleteBeforeRestore)
	if err != nil {
		return err
	}
	// Tell Orchestrator we're no longer stopped on purpose.
	// Do this in the background, as it's best-effort.
	go func() {
		if tm.orc == nil {
			return
		}
		if err := tm.orc.EndMaintenance(tm.Tablet()); err != nil {
			log.Warningf("Orchestrator EndMaintenance failed: %v", err)
		}
	}()
	return nil
}

func (tm *TabletManager) restoreDataLocked(ctx context.Context, logger logutil.Logger, waitForBackupInterval time.Duration, deleteBeforeRestore bool) error {

	tablet := tm.Tablet()
	originalType := tablet.Type
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

	// Check whether we're going to restore before changing to RESTORE type,
	// so we keep our MasterTermStartTime (if any) if we aren't actually restoring.
	ok, err := mysqlctl.ShouldRestore(ctx, params)
	if err != nil {
		return err
	}
	if !ok {
		params.Logger.Infof("Attempting to restore, but mysqld already contains data. Assuming vttablet was just restarted.")
		return mysqlctl.PopulateMetadataTables(params.Mysqld, params.LocalMetadata, params.DbName)
	}
	// We should not become master after restore, because that would incorrectly
	// start a new master term, and it's likely our data dir will be out of date.
	if originalType == topodatapb.TabletType_MASTER {
		originalType = tm.baseTabletType
	}
	if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_RESTORE, DBActionNone); err != nil {
		return err
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
	// If SnapshotTime is set , then apply the incremental change
	if keyspaceInfo.SnapshotTime != nil {
		err = tm.restoreToTimeFromBinlog(ctx, pos, keyspaceInfo.SnapshotTime)
		if err != nil {
			log.Errorf("unable to restore to the specified time %s, error : %v", keyspaceInfo.SnapshotTime.String(), err)
			return nil
		}
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
	default:
		// If anything failed, we should reset the original tablet type
		if err := tm.tmState.ChangeTabletType(ctx, originalType, DBActionNone); err != nil {
			log.Errorf("Could not change back to original tablet type %v: %v", originalType, err)
		}
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
	return tm.tmState.ChangeTabletType(ctx, originalType, DBActionNone)
}

// restoreToTimeFromBinlog restores to the snapshot time of the keyspace
// currently this works with mysql based database only (as it uses mysql specific queries for restoring)
func (tm *TabletManager) restoreToTimeFromBinlog(ctx context.Context, pos mysql.Position, restoreTime *vttime.Time) error {
	// validate the minimal settings necessary for connecting to binlog server
	if *binlogHost == "" || *binlogPort <= 0 || *binlogUser == "" {
		log.Warning("invalid binlog server setting, restoring to last available backup.")
		return nil
	}

	timeoutCtx, cancelFnc := context.WithTimeout(ctx, *timeoutForGTIDLookup)
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
// afterPos can be used directly in the query `START SLAVE UNTIL SQL_BEFORE_GTIDS = ''`
// beforePos will be used to check if replication was able to catch up from the binlog server
func (tm *TabletManager) getGTIDFromTimestamp(ctx context.Context, pos mysql.Position, restoreTime int64) (afterPos string, beforePos string, err error) {
	connParams := &mysql.ConnParams{
		Host:       *binlogHost,
		Port:       *binlogPort,
		Uname:      *binlogUser,
		SslCa:      *binlogSslCa,
		SslCert:    *binlogSslCert,
		SslKey:     *binlogSslKey,
		ServerName: *binlogSslServerName,
	}
	if *binlogPwd != "" {
		connParams.Pass = *binlogPwd
	}
	if *binlogSslCa != "" || *binlogSslCert != "" {
		connParams.EnableSSL()
	}
	dbCfgs := &dbconfigs.DBConfigs{
		Host: connParams.Host,
		Port: connParams.Port,
	}
	dbCfgs.SetDbParams(*connParams, *connParams)
	vsClient := vreplication.NewReplicaConnector(connParams)

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
	lastPos, err := binlogConn.MasterPosition()
	if err != nil {
		return "", "", err
	}

	gtidsChan := make(chan []string, 1)

	go func() {
		err := vsClient.VStream(ctx, mysql.EncodePosition(pos), filter, func(events []*binlogdatapb.VEvent) error {
			for _, event := range events {
				if event.Gtid != "" {
					// check if we reached the lastPos then return
					eventPos, err := mysql.DecodePosition(event.Gtid)
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
	defer vsClient.Close(ctx)
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
		afterGTIDParsed, err := mysql.DecodePosition(afterGTIDPos)
		if err != nil {
			return err
		}
		afterGTIDStr = afterGTIDParsed.GTIDSet.Last()
	}

	beforeGTIDPosParsed, err := mysql.DecodePosition(beforeGTIDPos)
	if err != nil {
		return err
	}

	// it uses mysql specific queries here
	cmds := []string{
		"STOP SLAVE FOR CHANNEL '' ",
		"STOP SLAVE IO_THREAD FOR CHANNEL ''",
	}

	if *binlogSslCa != "" || *binlogSslCert != "" {
		// We need to use TLS
		changeMasterCmd := fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1, MASTER_SSL=1", *binlogHost, *binlogPort, *binlogUser, *binlogPwd)
		if *binlogSslCa != "" {
			changeMasterCmd += fmt.Sprintf(", MASTER_SSL_CA='%s'", *binlogSslCa)
		}
		if *binlogSslCert != "" {
			changeMasterCmd += fmt.Sprintf(", MASTER_SSL_CERT='%s'", *binlogSslCert)
		}
		if *binlogSslKey != "" {
			changeMasterCmd += fmt.Sprintf(", MASTER_SSL_KEY='%s'", *binlogSslKey)
		}
		cmds = append(cmds, changeMasterCmd+";")
	} else {
		// No TLS
		cmds = append(cmds, fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1;", *binlogHost, *binlogPort, *binlogUser, *binlogPwd))
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
	// Could not use `agent.MysqlDaemon.WaitMasterPos` as replication is stopped with `START SLAVE UNTIL SQL_BEFORE_GTIDS`
	// this is as per https://dev.mysql.com/doc/refman/5.6/en/start-slave.html
	// We need to wait until replication catches upto the specified afterGTIDPos
	chGTIDCaughtup := make(chan bool)
	go func() {
		timeToWait := time.Now().Add(*timeoutForGTIDLookup)
		for time.Now().Before(timeToWait) {
			pos, err := tm.MysqlDaemon.MasterPosition()
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
	if err := tm.MysqlDaemon.SetMaster(ctx, ti.Tablet.MysqlHostname, int(ti.Tablet.MysqlPort), false /* stopReplicationBefore */, !*mysqlctl.DisableActiveReparents /* startReplicationAfter */); err != nil {
		return vterrors.Wrap(err, "MysqlDaemon.SetMaster failed")
	}

	// If active reparents are disabled, we don't restart replication. So it makes no sense to wait for an update on the replica.
	// Return immediately.
	if *mysqlctl.DisableActiveReparents {
		return nil
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
