/*
Copyright 2019 The Vitess Authors

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

/*
vtbackup is a batch command to perform a single pass of backup maintenance for a shard.

When run periodically for each shard, vtbackup can ensure these configurable policies:
* There is always a recent backup for the shard.
* Old backups for the shard are removed.

Whatever system launches vtbackup is responsible for the following:
* Running vtbackup with similar flags that would be used for a vttablet and
  mysqlctld in the target shard to be backed up.
* Provisioning as much disk space for vtbackup as would be given to vttablet.
  The data directory MUST be empty at startup. Do NOT reuse a persistent disk.
* Running vtbackup periodically for each shard, for each backup storage location.
* Ensuring that at most one instance runs at a time for a given pair of shard
  and backup storage location.
* Retrying vtbackup if it fails.
* Alerting human operators if the failure is persistent.

The process vtbackup follows to take a new backup is as follows:
1. Restore from the most recent backup.
2. Start a mysqld instance (but no vttablet) from the restored data.
3. Instruct mysqld to connect to the current shard master and replicate any
   transactions that are new since the last backup.
4. Ask the master for its current replication position and set that as the goal
   for catching up on replication before taking the backup, so the goalposts
   don't move.
5. Wait until replication is caught up to the goal position or beyond.
6. Stop mysqld and take a new backup.

Aside from additional replication load while vtbackup's mysqld catches up on
new transactions, the shard should be otherwise unaffected. Existing tablets
will continue to serve, and no new tablets will appear in topology, meaning no
query traffic will ever be routed to vtbackup's mysqld. This silent operation
mode helps make backups minimally disruptive to serving capacity and orthogonal
to the handling of the query path.

The command-line parameters to vtbackup specify a policy for when a new backup
is needed, and when old backups should be removed. If the existing backups
already satisfy the policy, then vtbackup will do nothing and return success
immediately.
*/
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"time"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	// vtbackup-specific flags
	timeout            = flag.Duration("timeout", 2*time.Hour, "Overall timeout for this whole vtbackup run, including restoring the previous backup, waiting for replication, and uploading files")
	replicationTimeout = flag.Duration("replication_timeout", 1*time.Hour, "The timeout for the step of waiting for replication to catch up. If progress is made before this timeout is reached, the backup will be taken anyway to save partial progress, but vtbackup will return a non-zero exit code to indicate it should be retried since not all expected data was backed up")
	initialBackup      = flag.Bool("initial_backup", false, "Instead of restoring from backup, initialize an empty database with the provided init_db_sql_file and upload a backup of that for the shard. This can be used to seed a brand new shard with an initial, empty backup. This can only be done before the shard exists in topology (i.e. before any tablets are deployed), and before any backups exist for the shard")

	// vttablet-like flags
	initDbNameOverride = flag.String("init_db_name_override", "", "(init parameter) override the name of the db used by vttablet")
	initKeyspace       = flag.String("init_keyspace", "", "(init parameter) keyspace to use for this tablet")
	initShard          = flag.String("init_shard", "", "(init parameter) shard to use for this tablet")
	concurrency        = flag.Int("concurrency", 4, "(init restore parameter) how many concurrent files to restore at once")

	// mysqlctld-like flags
	mysqlPort     = flag.Int("mysql_port", 3306, "mysql port")
	mysqlSocket   = flag.String("mysql_socket", "", "path to the mysql socket")
	mysqlTimeout  = flag.Duration("mysql_timeout", 5*time.Minute, "how long to wait for mysqld startup")
	initDBSQLFile = flag.String("init_db_sql_file", "", "path to .sql file to run after mysql_install_db")
)

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.ParseFlags("vtbackup")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// This is an imaginary tablet alias. The value doesn't matter for anything,
	// except that we generate a random UID to ensure the target backup
	// directory is unique if multiple vtbackup instances are launched for the
	// same shard, at exactly the same second, pointed at the same backup
	// storage location.
	bigN, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
	if err != nil {
		log.Errorf("can't generate random tablet UID: %v", err)
		exit.Return(1)
	}
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "vtbackup",
		Uid:  uint32(bigN.Uint64()),
	}

	// Clean up our temporary data dir if we exit for any reason, to make sure
	// every invocation of vtbackup starts with a clean slate, and it does not
	// accumulate garbage (and run out of disk space) if it's restarted.
	tabletDir := mysqlctl.TabletDir(tabletAlias.Uid)
	defer func() {
		log.Infof("Removing temporary tablet directory: %v", tabletDir)
		if err := os.RemoveAll(tabletDir); err != nil {
			log.Errorf("Failed to remove temporary tablet directory: %v", err)
		}
	}()

	// Start up mysqld as if we are mysqlctld provisioning a fresh tablet.
	mysqld, mycnf, err := mysqlctl.CreateMysqldAndMycnf(tabletAlias.Uid, *mysqlSocket, int32(*mysqlPort))
	if err != nil {
		log.Errorf("failed to initialize mysql config: %v", err)
		exit.Return(1)
	}
	initCtx, initCancel := context.WithTimeout(ctx, *mysqlTimeout)
	defer initCancel()
	if err := mysqld.Init(initCtx, mycnf, *initDBSQLFile); err != nil {
		log.Errorf("failed to initialize mysql data dir and start mysqld: %v", err)
		exit.Return(1)
	}
	// Shut down mysqld when we're done.
	defer func() {
		// Be careful not to use the original context, because we don't want to
		// skip shutdown just because we timed out waiting for other things.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		mysqld.Shutdown(ctx, mycnf, false)
	}()

	extraEnv := map[string]string{
		"TABLET_ALIAS": topoproto.TabletAliasString(tabletAlias),
	}
	dir := fmt.Sprintf("%v/%v", *initKeyspace, *initShard)
	topoServer := topo.Open()
	defer topoServer.Close()

	// In initial_backup mode, just take a backup of this empty database.
	if *initialBackup {
		// Check that the shard doesn't exist.
		_, err := topoServer.GetShard(ctx, *initKeyspace, *initShard)
		if !topo.IsErrType(err, topo.NoNode) {
			log.Errorf("Refusing to upload initial backup of empty database: the shard %v/%v already exists in topology.", *initKeyspace, *initShard)
			exit.Return(1)
		}
		// Check that no existing backups exist in this backup storage location.
		bs, err := backupstorage.GetBackupStorage()
		if err != nil {
			log.Errorf("Can't get backup storage: %v", err)
			exit.Return(1)
		}
		backups, err := bs.ListBackups(ctx, dir)
		if err != nil {
			log.Errorf("Can't list backups: %v", err)
			exit.Return(1)
		}
		if len(backups) > 0 {
			log.Errorf("Refusing to upload initial backup of empty database: the shard %v/%v already has at least one backup.", *initKeyspace, *initShard)
			exit.Return(1)
		}

		// If the checks pass, go ahead and take a backup of this empty DB.
		// First, initialize it the way InitShardMaster would, so this backup
		// produces a result that can be used to skip InitShardMaster entirely.
		// This involves resetting replication (to erase any history) and then
		// executing a statement to force the creation of a replication position.
		mysqld.ResetReplication(ctx)
		cmds := mysqlctl.CreateReparentJournal()
		if err := mysqld.ExecuteSuperQueryList(ctx, cmds); err != nil {
			log.Errorf("Can't initialize database with reparent journal: %v", err)
			exit.Return(1)
		}
		// Now we're ready to take the backup.
		name := backupName(time.Now(), tabletAlias)
		if err := mysqlctl.Backup(ctx, mycnf, mysqld, logutil.NewConsoleLogger(), dir, name, *concurrency, extraEnv); err != nil {
			log.Errorf("Error taking backup: %v", err)
			exit.Return(1)
		}
		log.Info("Backup successful")
		return
	}

	// Restore from backup.
	dbName := *initDbNameOverride
	if dbName == "" {
		dbName = fmt.Sprintf("vt_%s", *initKeyspace)
	}

	log.Infof("Restoring latest backup from directory %v", dir)
	restorePos, err := mysqlctl.Restore(ctx, mycnf, mysqld, dir, *concurrency, extraEnv, map[string]string{}, logutil.NewConsoleLogger(), true, dbName)
	switch err {
	case nil:
		log.Infof("Successfully restored from backup at replication position %v", restorePos)
	case mysqlctl.ErrNoBackup:
		log.Error("No backup found. Not starting up empty since -initial_backup flag was not enabled.")
		exit.Return(1)
	case mysqlctl.ErrExistingDB:
		log.Error("Can't run vtbackup because data directory is not empty.")
		exit.Return(1)
	default:
		log.Errorf("Error restoring from backup: %v", err)
		exit.Return(1)
	}

	// We have restored a backup. Now start replication.
	if err := resetReplication(ctx, restorePos, mysqld); err != nil {
		log.Errorf("Error resetting replication %v", err)
		exit.Return(1)
	}
	if err := startReplication(ctx, mysqld, topoServer); err != nil {
		log.Errorf("Error starting replication %v", err)
		exit.Return(1)
	}

	// Get the current master replication position, and wait until we catch up
	// to that point. We do this instead of looking at Seconds_Behind_Master
	// (replication lag reported by SHOW SLAVE STATUS) because that value can
	// sometimes lie and tell you there's 0 lag when actually replication is
	// stopped. Also, if replication is making progress but is too slow to ever
	// catch up to live changes, we'd rather take a backup of something rather
	// than timing out.
	tmc := tmclient.NewTabletManagerClient()
	masterPos, err := getMasterPosition(ctx, tmc, topoServer)
	if err != nil {
		log.Errorf("Can't get the master replication position: %v", err)
		exit.Return(1)
	}

	// Remember the time when we fetched the master position, not when we caught
	// up to it, so the timestamp on our backup is honest (assuming we make it
	// to the goal position).
	backupTime := time.Now()

	if restorePos.Equal(masterPos) {
		// Nothing has happened on the master since the last backup, so there's
		// no point taking a new backup since it would be identical.
		log.Infof("No backup is necessary. The latest backup is up-to-date with the master.")
		return
	}

	// Wait for replication to catch up.
	waitStartTime := time.Now()
	for {
		time.Sleep(time.Second)

		// Check if the replication context is still good.
		if time.Since(waitStartTime) > *replicationTimeout {
			// If we time out on this step, we still might take the backup anyway.
			log.Errorf("Timed out waiting for replication to catch up to %v.", masterPos)
			break
		}

		status, statusErr := mysqld.SlaveStatus()
		if statusErr != nil {
			log.Warningf("Error getting replication status: %v", statusErr)
			continue
		}
		if status.Position.AtLeast(masterPos) {
			// We're caught up on replication to at least the point the master
			// was at when this vtbackup run started.
			log.Infof("Replication caught up to %v after %v", status.Position, time.Since(waitStartTime))
			break
		}
		if !status.SlaveRunning() {
			log.Warning("Replication has stopped before backup could be taken. Trying to restart replication.")
			if err := startReplication(ctx, mysqld, topoServer); err != nil {
				log.Warningf("Failed to restart replication: %v", err)
			}
		}
	}

	// Did we make any progress?
	status, err := mysqld.SlaveStatus()
	if err != nil {
		log.Errorf("Error getting replication status: %v", err)
		exit.Return(1)
	}
	log.Infof("Replication caught up to at least %v", status.Position)
	if status.Position.Equal(restorePos) {
		log.Errorf("Not taking backup: replication did not make any progress from restore point: %v", restorePos)
		exit.Return(1)
	}

	// Now we can take a new backup.
	name := backupName(backupTime, tabletAlias)
	if err := mysqlctl.Backup(ctx, mycnf, mysqld, logutil.NewConsoleLogger(), dir, name, *concurrency, extraEnv); err != nil {
		log.Errorf("Error taking backup: %v", err)
		exit.Return(1)
	}

	// Return a non-zero exit code if we didn't meet the replication position
	// goal, even though we took a backup that pushes the high-water mark up.
	if !status.Position.AtLeast(masterPos) {
		log.Warningf("Replication caught up to %v but didn't make it to the goal of %v. A backup was taken anyway to save partial progress, but the operation should still be retried since not all expected data is backed up.", status.Position, masterPos)
		exit.Return(1)
	}
	log.Info("Backup successful")
}

func resetReplication(ctx context.Context, pos mysql.Position, mysqld mysqlctl.MysqlDaemon) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
	}
	if err := mysqld.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, "failed to reset slave")
	}

	// Set the position at which to resume from the master.
	if err := mysqld.SetSlavePosition(ctx, pos); err != nil {
		return vterrors.Wrap(err, "failed to set slave position")
	}
	return nil
}

func startReplication(ctx context.Context, mysqld mysqlctl.MysqlDaemon, topoServer *topo.Server) error {
	si, err := topoServer.GetShard(ctx, *initKeyspace, *initShard)
	if err != nil {
		return vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.MasterAlias) {
		// Normal tablets will sit around waiting to be reparented in this case.
		// Since vtbackup is a batch job, we just have to fail.
		return fmt.Errorf("can't start replication after restore: shard %v/%v has no master", *initKeyspace, *initShard)
	}
	// TODO(enisoc): Support replicating from another replica, preferably in the
	//   same cell, preferably rdonly, to reduce load on the master.
	ti, err := topoServer.GetTablet(ctx, si.MasterAlias)
	if err != nil {
		return vterrors.Wrapf(err, "Cannot read master tablet %v", si.MasterAlias)
	}

	// Set master and start slave.
	if err := mysqld.SetMaster(ctx, topoproto.MysqlHostname(ti.Tablet), int(topoproto.MysqlPort(ti.Tablet)), false /* slaveStopBefore */, true /* slaveStartAfter */); err != nil {
		return vterrors.Wrap(err, "MysqlDaemon.SetMaster failed")
	}
	return nil
}

func getMasterPosition(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server) (mysql.Position, error) {
	si, err := ts.GetShard(ctx, *initKeyspace, *initShard)
	if err != nil {
		return mysql.Position{}, vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.MasterAlias) {
		// Normal tablets will sit around waiting to be reparented in this case.
		// Since vtbackup is a batch job, we just have to fail.
		return mysql.Position{}, fmt.Errorf("shard %v/%v has no master", *initKeyspace, *initShard)
	}
	ti, err := ts.GetTablet(ctx, si.MasterAlias)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("can't get master tablet record %v: %v", topoproto.TabletAliasString(si.MasterAlias), err)
	}
	posStr, err := tmc.MasterPosition(ctx, ti.Tablet)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("can't get master replication position: %v", err)
	}
	pos, err := mysql.DecodePosition(posStr)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("can't decode master replication position %q: %v", posStr, err)
	}
	return pos, nil
}

func backupName(backupTime time.Time, tabletAlias *topodatapb.TabletAlias) string {
	return fmt.Sprintf("%v.%v", backupTime.UTC().Format("2006-01-02.150405"), topoproto.TabletAliasString(tabletAlias))
}
