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
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
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

const (
	// operationTimeout is the timeout for individual operations like fetching
	// the master position. This does not impose an overall timeout on
	// long-running processes like taking the backup. It only applies to
	// steps along the way that should complete quickly. This ensures we don't
	// place a hard cap on the overall time for a backup, while also not waiting
	// forever for things that should be quick.
	operationTimeout = 1 * time.Minute
)

var (
	// vtbackup-specific flags
	// We used to have overall timeouts, but these did more harm than good. If a backup
	// has been going for a while, giving up and starting over from scratch is
	// pretty much never going to help. We should just keep trying and have a
	// system that alerts a human if it's taking longer than expected.
	_ = flag.Duration("timeout", 2*time.Hour, "DEPRECATED AND UNUSED")
	_ = flag.Duration("replication_timeout", 1*time.Hour, "DEPRECATED AND UNUSED")

	minBackupInterval = flag.Duration("min_backup_interval", 0, "Only take a new backup if it's been at least this long since the most recent backup.")
	minRetentionTime  = flag.Duration("min_retention_time", 0, "Keep each old backup for at least this long before removing it. Set to 0 to disable pruning of old backups.")
	minRetentionCount = flag.Int("min_retention_count", 1, "Always keep at least this many of the most recent backups in this backup storage location, even if some are older than the min_retention_time. This must be at least 1 since a backup must always exist to allow new backups to be made")

	initialBackup    = flag.Bool("initial_backup", false, "Instead of restoring from backup, initialize an empty database with the provided init_db_sql_file and upload a backup of that for the shard, if the shard has no backups yet. This can be used to seed a brand new shard with an initial, empty backup. If any backups already exist for the shard, this will be considered a successful no-op. This can only be done before the shard exists in topology (i.e. before any tablets are deployed).")
	allowFirstBackup = flag.Bool("allow_first_backup", false, "Allow this job to take the first backup of an existing shard.")

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
	detachedMode  = flag.Bool("detach", false, "detached mode - run backups detached from the terminal")
)

func main() {
	defer exit.Recover()
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.ParseFlags("vtbackup")

	if *detachedMode {
		// this method will call os.Exit and kill this process
		terminateStayResident()
	}

	defer logutil.Flush()

	if *minRetentionCount < 1 {
		log.Errorf("min_retention_count must be at least 1 to allow restores to succeed")
		exit.Return(1)
	}

	// Catch SIGTERM and SIGINT so we get a chance to clean up.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Infof("Cancelling due to signal: %v", sig)
		cancel()
	}()

	// Open connection backup storage.
	backupStorage, err := backupstorage.GetBackupStorage()
	if err != nil {
		log.Errorf("Can't get backup storage: %v", err)
		exit.Return(1)
	}
	defer backupStorage.Close()
	// Open connection to topology server.
	topoServer := topo.Open()
	defer topoServer.Close()

	// Try to take a backup, if it's been long enough since the last one.
	// Skip pruning if backup wasn't fully successful. We don't want to be
	// deleting things if the backup process is not healthy.
	backupDir := mysqlctl.GetBackupDir(*initKeyspace, *initShard)
	doBackup, err := shouldBackup(ctx, topoServer, backupStorage, backupDir)
	if err != nil {
		log.Errorf("Can't take backup: %v", err)
		exit.Return(1)
	}
	if doBackup {
		if err := takeBackup(ctx, topoServer, backupStorage); err != nil {
			log.Errorf("Failed to take backup: %v", err)
			exit.Return(1)
		}
	}

	// Prune old backups.
	if err := pruneBackups(ctx, backupStorage, backupDir); err != nil {
		log.Errorf("Couldn't prune old backups: %v", err)
		exit.Return(1)
	}
}

func terminateStayResident() {
	args := os.Args[1:]
	i := 0
	for ; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-detach") {
			args[i] = "-detach=false"
			break
		}
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Start()
	fmt.Println("[PID]", cmd.Process.Pid)
	os.Exit(0)
}

func takeBackup(ctx context.Context, topoServer *topo.Server, backupStorage backupstorage.BackupStorage) error {
	// This is an imaginary tablet alias. The value doesn't matter for anything,
	// except that we generate a random UID to ensure the target backup
	// directory is unique if multiple vtbackup instances are launched for the
	// same shard, at exactly the same second, pointed at the same backup
	// storage location.
	bigN, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
	if err != nil {
		return fmt.Errorf("can't generate random tablet UID: %v", err)
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
			log.Warningf("Failed to remove temporary tablet directory: %v", err)
		}
	}()

	// Start up mysqld as if we are mysqlctld provisioning a fresh tablet.
	mysqld, mycnf, err := mysqlctl.CreateMysqldAndMycnf(tabletAlias.Uid, *mysqlSocket, int32(*mysqlPort))
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	initCtx, initCancel := context.WithTimeout(ctx, *mysqlTimeout)
	defer initCancel()
	if err := mysqld.Init(initCtx, mycnf, *initDBSQLFile); err != nil {
		return fmt.Errorf("failed to initialize mysql data dir and start mysqld: %v", err)
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
	dbName := *initDbNameOverride
	if dbName == "" {
		dbName = fmt.Sprintf("vt_%s", *initKeyspace)
	}

	backupParams := mysqlctl.BackupParams{
		Cnf:          mycnf,
		Mysqld:       mysqld,
		Logger:       logutil.NewConsoleLogger(),
		Concurrency:  *concurrency,
		HookExtraEnv: extraEnv,
		TopoServer:   topoServer,
		Keyspace:     *initKeyspace,
		Shard:        *initShard,
		TabletAlias:  topoproto.TabletAliasString(tabletAlias),
	}
	// In initial_backup mode, just take a backup of this empty database.
	if *initialBackup {
		// Take a backup of this empty DB without restoring anything.
		// First, initialize it the way InitShardMaster would, so this backup
		// produces a result that can be used to skip InitShardMaster entirely.
		// This involves resetting replication (to erase any history) and then
		// creating the main database and some Vitess system tables.
		if err := mysqld.ResetReplication(ctx); err != nil {
			return fmt.Errorf("can't reset replication: %v", err)
		}
		cmds := mysqlctl.CreateReparentJournal()
		cmds = append(cmds, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(dbName)))
		if err := mysqld.ExecuteSuperQueryList(ctx, cmds); err != nil {
			return fmt.Errorf("can't initialize database: %v", err)
		}
		backupParams.BackupTime = time.Now()
		// Now we're ready to take the backup.
		if err := mysqlctl.Backup(ctx, backupParams); err != nil {
			return fmt.Errorf("backup failed: %v", err)
		}
		log.Info("Initial backup successful.")
		return nil
	}

	backupDir := mysqlctl.GetBackupDir(*initKeyspace, *initShard)
	log.Infof("Restoring latest backup from directory %v", backupDir)
	params := mysqlctl.RestoreParams{
		Cnf:                 mycnf,
		Mysqld:              mysqld,
		Logger:              logutil.NewConsoleLogger(),
		Concurrency:         *concurrency,
		HookExtraEnv:        extraEnv,
		LocalMetadata:       map[string]string{},
		DeleteBeforeRestore: true,
		DbName:              dbName,
		Keyspace:            *initKeyspace,
		Shard:               *initShard,
	}
	backupManifest, err := mysqlctl.Restore(ctx, params)
	var restorePos mysql.Position
	switch err {
	case nil:
		log.Infof("Successfully restored from backup at replication position %v", restorePos)
		// if err is nil, we expect backupManifest to be non-nil
		restorePos = backupManifest.Position
	case mysqlctl.ErrNoBackup:
		// There is no backup found, but we may be taking the initial backup of a shard
		if !*allowFirstBackup {
			return fmt.Errorf("no backup found; not starting up empty since -initial_backup flag was not enabled")
		}
		restorePos = mysql.Position{}
	case mysqlctl.ErrExistingDB:
		return fmt.Errorf("can't run vtbackup because data directory is not empty")
	default:
		return fmt.Errorf("can't restore from backup: %v", err)
	}

	// We have restored a backup. Now start replication.
	if err := resetReplication(ctx, restorePos, mysqld); err != nil {
		return fmt.Errorf("error resetting replication: %v", err)
	}
	if err := startReplication(ctx, mysqld, topoServer); err != nil {
		return fmt.Errorf("error starting replication: %v", err)
	}

	// Get the current master replication position, and wait until we catch up
	// to that point. We do this instead of looking at Seconds_Behind_Master
	// (replication lag reported by SHOW SLAVE STATUS) because that value can
	// sometimes lie and tell you there's 0 lag when actually replication is
	// stopped. Also, if replication is making progress but is too slow to ever
	// catch up to live changes, we'd rather take a backup of something rather
	// than timing out.
	tmc := tmclient.NewTabletManagerClient()
	// Keep retrying if we can't contact the master. The master might be
	// changing, moving, or down temporarily.
	var masterPos mysql.Position
	err = retryOnError(ctx, func() error {
		// Add a per-operation timeout so we re-read topo if the master is unreachable.
		opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		defer cancel()
		pos, err := getMasterPosition(opCtx, tmc, topoServer)
		if err != nil {
			return fmt.Errorf("can't get the master replication position: %v", err)
		}
		masterPos = pos
		return nil
	})
	if err != nil {
		return err
	}

	// Remember the time when we fetched the master position, not when we caught
	// up to it, so the timestamp on our backup is honest (assuming we make it
	// to the goal position).
	backupParams.BackupTime = time.Now()

	// Wait for replication to catch up.
	waitStartTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
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
		return fmt.Errorf("can't get replication status: %v", err)
	}
	log.Infof("Replication caught up to at least %v", status.Position)
	if !status.Position.AtLeast(masterPos) && status.Position.Equal(restorePos) {
		return fmt.Errorf("not taking backup: replication did not make any progress from restore point: %v", restorePos)
	}

	// Now we can take a new backup.
	if err := mysqlctl.Backup(ctx, backupParams); err != nil {
		return fmt.Errorf("error taking backup: %v", err)
	}

	// Return a non-zero exit code if we didn't meet the replication position
	// goal, even though we took a backup that pushes the high-water mark up.
	if !status.Position.AtLeast(masterPos) {
		return fmt.Errorf("replication caught up to %v but didn't make it to the goal of %v; a backup was taken anyway to save partial progress, but the operation should still be retried since not all expected data is backed up", status.Position, masterPos)
	}
	log.Info("Backup successful.")
	return nil
}

func resetReplication(ctx context.Context, pos mysql.Position, mysqld mysqlctl.MysqlDaemon) error {
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
	}
	if err := mysqld.ExecuteSuperQueryList(ctx, cmds); err != nil {
		return vterrors.Wrap(err, "failed to reset slave")
	}

	// Check if we have a position to resume from, if not reset to the beginning of time
	if !pos.IsZero() {
		// Set the position at which to resume from the master.
		if err := mysqld.SetSlavePosition(ctx, pos); err != nil {
			return vterrors.Wrap(err, "failed to set slave position")
		}
	} else {
		if err := mysqld.ResetReplication(ctx); err != nil {
			return vterrors.Wrap(err, "failed to reset replication")
		}
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

	// Stop slave (in case we're restarting), set master, and start slave.
	if err := mysqld.SetMaster(ctx, topoproto.MysqlHostname(ti.Tablet), int(topoproto.MysqlPort(ti.Tablet)), true /* slaveStopBefore */, true /* slaveStartAfter */); err != nil {
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

// retryOnError keeps calling the given function until it succeeds, or the given
// Context is done. It waits an exponentially increasing amount of time between
// retries to avoid hot-looping. The only time this returns an error is if the
// Context is cancelled.
func retryOnError(ctx context.Context, fn func() error) error {
	waitTime := 1 * time.Second

	for {
		err := fn()
		if err == nil {
			return nil
		}
		log.Errorf("Waiting %v to retry after error: %v", waitTime, err)

		select {
		case <-ctx.Done():
			log.Errorf("Not retrying after error: %v", ctx.Err())
			return ctx.Err()
		case <-time.After(waitTime):
			waitTime *= 2
		}
	}
}

func pruneBackups(ctx context.Context, backupStorage backupstorage.BackupStorage, backupDir string) error {
	if *minRetentionTime == 0 {
		log.Info("Pruning of old backups is disabled.")
		return nil
	}
	backups, err := backupStorage.ListBackups(ctx, backupDir)
	if err != nil {
		return fmt.Errorf("can't list backups: %v", err)
	}
	numBackups := len(backups)
	if numBackups <= *minRetentionCount {
		log.Infof("Found %v backups. Not pruning any since this is within the min_retention_count of %v.", numBackups, *minRetentionCount)
		return nil
	}
	// We have more than the minimum retention count, so we could afford to
	// prune some. See if any are beyond the minimum retention time.
	// ListBackups returns them sorted by oldest first.
	for _, backup := range backups {
		backupTime, err := parseBackupTime(backup.Name())
		if err != nil {
			return err
		}
		if time.Since(backupTime) < *minRetentionTime {
			// The oldest remaining backup is not old enough to prune.
			log.Infof("Oldest backup taken at %v has not reached min_retention_time of %v. Nothing left to prune.", backupTime, *minRetentionTime)
			break
		}
		// Remove the backup.
		log.Infof("Removing old backup %v from %v, since it's older than min_retention_time of %v", backup.Name(), backupDir, *minRetentionTime)
		if err := backupStorage.RemoveBackup(ctx, backupDir, backup.Name()); err != nil {
			return fmt.Errorf("couldn't remove backup %v from %v: %v", backup.Name(), backupDir, err)
		}
		// We successfully removed one backup. Can we afford to prune any more?
		numBackups--
		if numBackups == *minRetentionCount {
			log.Infof("Successfully pruned backup count to min_retention_count of %v.", *minRetentionCount)
			break
		}
	}
	return nil
}

func parseBackupTime(name string) (time.Time, error) {
	// Backup names are formatted as "date.time.tablet-alias".
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("backup name not in expected format (date.time.tablet-alias): %v", name)
	}
	backupTime, err := time.Parse(mysqlctl.BackupTimestampFormat, fmt.Sprintf("%s.%s", parts[0], parts[1]))
	if err != nil {
		return time.Time{}, fmt.Errorf("can't parse timestamp from backup %q: %v", name, err)
	}
	return backupTime, nil
}

func shouldBackup(ctx context.Context, topoServer *topo.Server, backupStorage backupstorage.BackupStorage, backupDir string) (bool, error) {
	// Look for the most recent, complete backup.
	backups, err := backupStorage.ListBackups(ctx, backupDir)
	if err != nil {
		return false, fmt.Errorf("can't list backups: %v", err)
	}
	lastBackup := lastCompleteBackup(ctx, backups)

	// Check preconditions for initial_backup mode.
	if *initialBackup {
		// Check if any backups for the shard already exist in this backup storage location.
		if lastBackup != nil {
			log.Infof("At least one complete backup already exists, so there's no need to seed an empty backup. Doing nothing.")
			return false, nil
		}

		// Check whether the shard exists.
		_, shardErr := topoServer.GetShard(ctx, *initKeyspace, *initShard)
		switch {
		case shardErr == nil:
			// If the shard exists, we should make sure none of the tablets are
			// already in a serving state, because then they might have data
			// that conflicts with the initial backup we're about to take.
			tablets, err := topoServer.GetTabletMapForShard(ctx, *initKeyspace, *initShard)
			if err != nil {
				// We don't know for sure whether any tablets are serving,
				// so it's not safe to continue.
				return false, fmt.Errorf("failed to check whether shard %v/%v has serving tablets before doing initial backup: %v", *initKeyspace, *initShard, err)
			}
			for tabletAlias, tablet := range tablets {
				// Check if any tablet has its type set to one of the serving types.
				// If so, it's too late to do an initial backup.
				if tablet.IsInServingGraph() {
					return false, fmt.Errorf("refusing to upload initial backup of empty database: the shard %v/%v already has at least one tablet that may be serving (%v); you must take a backup from a live tablet instead", *initKeyspace, *initShard, tabletAlias)
				}
			}
			log.Infof("Shard %v/%v exists but has no serving tablets.", *initKeyspace, *initShard)
		case topo.IsErrType(shardErr, topo.NoNode):
			// The shard doesn't exist, so we know no tablets are running.
			log.Infof("Shard %v/%v doesn't exist; assuming it has no serving tablets.", *initKeyspace, *initShard)
		default:
			// If we encounter any other error, we don't know for sure whether
			// the shard exists, so it's not safe to continue.
			return false, fmt.Errorf("failed to check whether shard %v/%v exists before doing initial backup: %v", *initKeyspace, *initShard, err)
		}

		log.Infof("Shard %v/%v has no existing backups. Creating initial backup.", *initKeyspace, *initShard)
		return true, nil
	}

	// We need at least one backup so we can restore first, unless the user explicitly says we don't
	if len(backups) == 0 && !*allowFirstBackup {
		return false, fmt.Errorf("no existing backups to restore from; backup is not possible since -initial_backup flag was not enabled")
	}
	if lastBackup == nil {
		if *allowFirstBackup {
			// There's no complete backup, but we were told to take one from scratch anyway.
			return true, nil
		}
		return false, fmt.Errorf("no complete backups to restore from; backup is not possible since -initial_backup flag was not enabled")
	}

	// Has it been long enough since the last complete backup to need a new one?
	if *minBackupInterval == 0 {
		// No minimum interval is set, so always backup.
		return true, nil
	}
	lastBackupTime, err := parseBackupTime(lastBackup.Name())
	if err != nil {
		return false, fmt.Errorf("can't check last backup time: %v", err)
	}
	if elapsedTime := time.Since(lastBackupTime); elapsedTime < *minBackupInterval {
		// It hasn't been long enough yet.
		log.Infof("Skipping backup since only %v has elapsed since the last backup at %v, which is less than the min_backup_interval of %v.", elapsedTime, lastBackupTime, *minBackupInterval)
		return false, nil
	}
	// It has been long enough.
	log.Infof("The last backup was taken at %v, which is older than the min_backup_interval of %v.", lastBackupTime, *minBackupInterval)
	return true, nil
}

func lastCompleteBackup(ctx context.Context, backups []backupstorage.BackupHandle) backupstorage.BackupHandle {
	if len(backups) == 0 {
		return nil
	}

	// Backups are sorted in ascending order by start time. Start at the end.
	for i := len(backups) - 1; i >= 0; i-- {
		// Check if this backup is complete by looking for the MANIFEST file,
		// which is written at the end after all files are uploaded.
		backup := backups[i]
		if err := checkBackupComplete(ctx, backup); err != nil {
			log.Warningf("Ignoring backup %v because it's incomplete: %v", backup.Name(), err)
			continue
		}
		return backup
	}

	return nil
}

func checkBackupComplete(ctx context.Context, backup backupstorage.BackupHandle) error {
	manifest, err := mysqlctl.GetBackupManifest(ctx, backup)
	if err != nil {
		return fmt.Errorf("can't get backup MANIFEST: %v", err)
	}

	log.Infof("Found complete backup %v taken at position %v", backup.Name(), manifest.Position.String())
	return nil
}
