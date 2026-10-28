/*
Copyright 2023 The Vitess Authors

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

package cli

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

const (
	// operationTimeout is the timeout for individual operations like fetching
	// the primary position. This does not impose an overall timeout on
	// long-running processes like taking the backup. It only applies to
	// steps along the way that should complete quickly. This ensures we don't
	// place a hard cap on the overall time for a backup, while also not waiting
	// forever for things that should be quick.
	operationTimeout = 1 * time.Minute

	phaseNameCatchupReplication = "CatchupReplication"
	phaseNameInitialBackup      = "InitialBackup"
	phaseNameRestoreLastBackup  = "RestoreLastBackup"
	phaseNameTakeNewBackup      = "TakeNewBackup"
	// phaseNameVerifyErrantGTIDs is active while the errant GTID checks of the
	// restored and to-be-backed-up data run. It isn't in phaseNames, so it only
	// appears with --enable-errant-gtid-checks.
	phaseNameVerifyErrantGTIDs           = "VerifyErrantGTIDs"
	phaseStatusCatchupReplicationStalled = "Stalled"
	phaseStatusCatchupReplicationStopped = "Stopped"

	timeoutWaitingForReplicationStatus = 60 * time.Second
)

var (
	minBackupInterval   time.Duration
	minRetentionTime    time.Duration
	minRetentionCount   = 1
	initialBackup       bool
	allowFirstBackup    bool
	restartBeforeBackup bool
	upgradeSafe         bool
	includeBinlogs      bool
	initSQLQueries      []string
	initSQLTabletTypes  []topodatapb.TabletType
	initSQLTimeout      time.Duration
	initSQLFailOnError  bool
	errantGTIDChecks    bool

	// vttablet-like flags
	initDbNameOverride string
	initKeyspace       string
	initShard          string
	concurrency        = 4
	incrementalFromPos string
	restoreWithClone   bool

	// mysqlctld-like flags
	mysqlPort            = 3306
	mysqlSocket          string
	mysqlTimeout         = 5 * time.Minute
	mysqlShutdownTimeout = mysqlctl.DefaultShutdownTimeout
	initDBSQLFile        string
	detachedMode         bool
	keepAliveTimeout     time.Duration
	disableRedoLog       bool

	collationEnv *collations.Environment

	// Deprecated, use "Phase" instead.
	deprecatedDurationByPhase = stats.NewGaugesWithSingleLabel(
		"DurationByPhaseSeconds",
		"[DEPRECATED] How long it took vtbackup to perform each phase (in seconds).",
		"phase",
	)

	// This gauge is updated 3*N times during the course of a vtbackup run,
	// where N is the number of different phases vtbackup transitions through.
	// Once to initialize to 0, another time to set the phase to active (1),
	// and another to deactivate the phase (back to 0).
	//
	// At most a single phase is active at a given time.
	//
	// The sync gauge immediately reports changes to push-backed backends.
	// The benefit of the sync gauge is that it makes verifying stats in
	// integration tests a lot more tractable.
	phase = stats.NewSyncGaugesWithSingleLabel(
		"Phase",
		"Active phase.",
		"phase",
	)
	phaseNames = []string{
		phaseNameCatchupReplication,
		phaseNameInitialBackup,
		phaseNameRestoreLastBackup,
		phaseNameTakeNewBackup,
	}
	phaseStatus = stats.NewGaugesWithMultiLabels(
		"PhaseStatus",
		"Internal state of vtbackup phase.",
		[]string{"phase", "status"},
	)
	phaseStatuses = map[string][]string{
		phaseNameCatchupReplication: {
			phaseStatusCatchupReplicationStalled,
			phaseStatusCatchupReplicationStopped,
		},
	}

	Main = &cobra.Command{
		Use:   "vtbackup",
		Short: "vtbackup is a batch command to perform a single pass of backup maintenance for a shard.",
		Long: `vtbackup is a batch command to perform a single pass of backup maintenance for a shard.

When run periodically for each shard, vtbackup can ensure these configurable policies:
 * There is always a recent backup for the shard.

 * Old backups for the shard are removed.

Whatever system launches vtbackup is responsible for the following:
 - Running vtbackup with similar flags that would be used for a vttablet and
   mysqlctld in the target shard to be backed up.

 - Provisioning as much disk space for vtbackup as would be given to vttablet.
   The data directory MUST be empty at startup. Do NOT reuse a persistent disk.

 - Running vtbackup periodically for each shard, for each backup storage location.

 - Ensuring that at most one instance runs at a time for a given pair of shard
   and backup storage location.

 - Retrying vtbackup if it fails.

 - Alerting human operators if the failure is persistent.

The process vtbackup follows to take a new backup has the following steps:
 1. Restore from the most recent backup.
 2. Start a mysqld instance (but no vttablet) from the restored data.
 3. Instruct mysqld to connect to the current shard primary and replicate any
    transactions that are new since the last backup.
 4. Ask the primary for its current replication position and set that as the goal
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
immediately.`,
		Version: servenv.AppVersion.String(),
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

func init() {
	servenv.RegisterDefaultFlags()
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.MoveFlagsToCobraCommand(Main)

	utils.SetFlagDurationVar(Main.Flags(), &minBackupInterval, "min-backup-interval", minBackupInterval, "Only take a new backup if it's been at least this long since the most recent backup.")
	utils.SetFlagDurationVar(Main.Flags(), &minRetentionTime, "min-retention-time", minRetentionTime, "Keep each old backup for at least this long before removing it. Set to 0 to disable pruning of old backups.")
	utils.SetFlagIntVar(Main.Flags(), &minRetentionCount, "min-retention-count", minRetentionCount, "Always keep at least this many of the most recent backups in this backup storage location, even if some are older than --min-retention-time. This must be at least 1 since a backup must always exist to allow new backups to be made")
	utils.SetFlagBoolVar(Main.Flags(), &initialBackup, "initial-backup", initialBackup, "Instead of restoring from backup, initialize an empty database with the provided init-db-sql-file and upload a backup of that for the shard, if the shard has no backups yet. This can be used to seed a brand new shard with an initial, empty backup. If any backups already exist for the shard, this will be considered a successful no-op. This can only be done before the shard exists in topology (i.e. before any tablets are deployed).")
	utils.SetFlagBoolVar(Main.Flags(), &allowFirstBackup, "allow-first-backup", allowFirstBackup, "Allow this job to take the first backup of an existing shard.")
	utils.SetFlagBoolVar(Main.Flags(), &restartBeforeBackup, "restart-before-backup", restartBeforeBackup, "Perform a mysqld clean/full restart after applying binlogs, but before taking the backup. Only makes sense to work around xtrabackup bugs.")
	Main.Flags().BoolVar(&upgradeSafe, "upgrade-safe", upgradeSafe, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")
	Main.Flags().BoolVar(&includeBinlogs, "include-binlogs", includeBinlogs, "Include the source's binary logs in a full builtin backup. Restores use them automatically.")
	Main.Flags().StringSliceVar(&initSQLQueries, "init-backup-sql-queries", nil, "Queries to execute after catch-up replication, before initializing the backup")
	Main.Flags().Var((*topoproto.TabletTypeListFlag)(&initSQLTabletTypes), "init-backup-tablet-types", "Tablet types used for the backup where the init SQL queries (--init-backup-sql-queries) will be executed after catch-up replication, before initializing the backup")
	Main.Flags().DurationVar(&initSQLTimeout, "init-backup-sql-timeout", initSQLTimeout, "At what point should we time out the init SQL query (--init-backup-sql-queries) work and either fail the backup job (--init-backup-sql-fail-on-error) or continue on with the backup")
	Main.Flags().BoolVar(&initSQLFailOnError, "init-backup-sql-fail-on-error", false, "Whether or not to fail the backup if the init SQL queries (--init-backup-sql-queries) fail, which includes if they fail to complete before the specified timeout (--init-backup-sql-timeout)")
	Main.Flags().BoolVar(&errantGTIDChecks, "enable-errant-gtid-checks", errantGTIDChecks, "Fail the run when the restored backup or the backup this run takes has errant GTIDs.")

	// vttablet-like flags
	utils.SetFlagStringVar(Main.Flags(), &initDbNameOverride, "init-db-name-override", initDbNameOverride, "(init parameter) override the name of the db used by vttablet")
	utils.SetFlagStringVar(Main.Flags(), &initKeyspace, "init-keyspace", initKeyspace, "(init parameter) keyspace to use for this tablet")
	utils.SetFlagStringVar(Main.Flags(), &initShard, "init-shard", initShard, "(init parameter) shard to use for this tablet")
	Main.Flags().IntVar(&concurrency, "concurrency", concurrency, "(init restore parameter) how many concurrent files to restore at once")
	utils.SetFlagStringVar(Main.Flags(), &incrementalFromPos, "incremental-from-pos", incrementalFromPos, "Position, or name of backup from which to create an incremental backup. Default: empty. If given, then this backup becomes an incremental backup from given position or given backup. If value is 'auto', this backup will be taken from the last successful backup position.")
	Main.Flags().BoolVar(&restoreWithClone, "restore-with-clone", restoreWithClone, "(init parameter) will perform the restore phase with MySQL CLONE, requires either --clone-from-primary or --clone-from-tablet")

	// mysqlctld-like flags
	utils.SetFlagIntVar(Main.Flags(), &mysqlPort, "mysql-port", mysqlPort, "MySQL port")
	utils.SetFlagStringVar(Main.Flags(), &mysqlSocket, "mysql-socket", mysqlSocket, "Path to the mysqld socket file")
	utils.SetFlagDurationVar(Main.Flags(), &mysqlTimeout, "mysql-timeout", mysqlTimeout, "how long to wait for mysqld startup")
	Main.Flags().DurationVar(&mysqlShutdownTimeout, "mysql-shutdown-timeout", mysqlShutdownTimeout, "how long to wait for mysqld shutdown")
	utils.SetFlagStringVar(Main.Flags(), &initDBSQLFile, "init-db-sql-file", initDBSQLFile, "path to .sql file to run after mysql_install_db")
	Main.Flags().BoolVar(&detachedMode, "detach", detachedMode, "detached mode - run backups detached from the terminal")
	Main.Flags().DurationVar(&keepAliveTimeout, "keep-alive-timeout", keepAliveTimeout, "Wait until timeout elapses after a successful backup before shutting down.")
	Main.Flags().BoolVar(&disableRedoLog, "disable-redo-log", disableRedoLog, "Disable InnoDB redo log during replication-from-primary phase of backup.")

	acl.RegisterFlags(Main.Flags())

	collationEnv = collations.NewEnvironment(servenv.MySQLServerVersion())
}

func run(cc *cobra.Command, args []string) error {
	servenv.Init()

	ctx, cancel := context.WithCancel(cc.Context())
	servenv.OnClose(func() {
		cancel()
	})

	defer func() {
		servenv.ExitChan <- syscall.SIGTERM
		<-ctx.Done()
	}()

	go servenv.RunDefault()
	// Some stats plugins use OnRun to initialize. Wait for them to finish
	// initializing before continuing, so we don't lose any stats.
	if err := stats.AwaitBackend(ctx); err != nil {
		return fmt.Errorf("failed to await stats backend: %w", err)
	}

	if detachedMode {
		// this method will call os.Exit and kill this process
		cmd.DetachFromTerminalAndExit()
	}

	defer logutil.Flush()

	if minRetentionCount < 1 {
		log.Error("min_retention_count must be at least 1 to allow restores to succeed")
		exit.Return(1)
	}

	// Open connection backup storage.
	backupStorage, err := backupstorage.GetBackupStorage()
	if err != nil {
		return fmt.Errorf("Can't get backup storage: %w", err)
	}
	defer backupStorage.Close()
	// Open connection to topology server.
	topoServer := topo.Open()
	defer topoServer.Close()

	// Initialize stats.
	initPhases()
	for phaseName, statuses := range phaseStatuses {
		for _, status := range statuses {
			phaseStatus.Set([]string{phaseName, status}, 0)
		}
	}

	// Try to take a backup, if it's been long enough since the last one.
	// Skip pruning if backup wasn't fully successful. We don't want to be
	// deleting things if the backup process is not healthy.
	backupDir := mysqlctl.GetBackupDir(initKeyspace, initShard)
	doBackup, err := shouldBackup(ctx, topoServer, backupStorage, backupDir)
	if err != nil {
		return fmt.Errorf("Can't take backup: %w", err)
	}
	if doBackup {
		if err := takeBackup(ctx, topoServer, backupStorage); err != nil {
			return fmt.Errorf("Failed to take backup: %w", err)
		}
	}

	// Prune old backups.
	if err := pruneBackups(ctx, backupStorage, backupDir); err != nil {
		return fmt.Errorf("Couldn't prune old backups: %w", err)
	}

	if keepAliveTimeout > 0 {
		log.Info(fmt.Sprintf("Backup was successful, waiting %s before exiting (or until context expires).", keepAliveTimeout))
		select {
		case <-time.After(keepAliveTimeout):
		case <-ctx.Done():
		}
	}
	log.Info("Exiting.")

	return nil
}

// takeBackup restores or clones the shard data into a temporary mysqld,
// catches replication up to the primary, and writes a new backup.
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
		log.Info(fmt.Sprintf("Removing temporary tablet directory: %v", tabletDir))
		if err := os.RemoveAll(tabletDir); err != nil {
			log.Warn(fmt.Sprintf("Failed to remove temporary tablet directory: %v", err))
		}
	}()

	// Start up mysqld as if we are mysqlctld provisioning a fresh tablet.
	mysqld, mycnf, err := mysqlctl.CreateMysqldAndMycnf(tabletAlias.Uid, mysqlSocket, mysqlPort, collationEnv)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}

	// shutdownCtx is a separate context for local mysqld work in this function.
	// We do not use the main job context for this because mysqld can stop as
	// part of normal CLONE work. Local mysqld calls should stop when that
	// happens, but allow the rest of the backup job to continue.
	shutdownCtx, cancelShutdownCtx := context.WithCancel(ctx)

	// cleanupCtx is only for cleanup work at the end.
	cleanupCtx, cancelCleanupCtx := context.WithCancel(ctx)

	defer func() {
		cancelShutdownCtx()
		cancelCleanupCtx()
	}()

	mysqlTermHandler := newMySQLTermHandler(func() {
		log.Warn("Cancelling vtbackup as MySQL has terminated")
		cancelShutdownCtx()
		cancelCleanupCtx()
	})
	mysqld.OnTerm(mysqlTermHandler.onTerm)

	initCtx, initCancel := context.WithTimeout(shutdownCtx, mysqlTimeout)
	defer initCancel()
	initMysqldAt := time.Now()
	if err := mysqld.Init(initCtx, mycnf, initDBSQLFile); err != nil {
		return fmt.Errorf("failed to initialize mysql data dir and start mysqld: %v", err)
	}
	deprecatedDurationByPhase.Set("InitMySQLd", int64(time.Since(initMysqldAt).Seconds()))
	// Shut down mysqld when we're done.
	defer func() {
		// Use cleanupCtx here so we still try to clean up even if startup timed out.
		mysqlShutdownCtx, mysqlShutdownCancel := context.WithTimeout(cleanupCtx, mysqlShutdownTimeout+mysqlctl.MysqldShutdownGracePeriod)
		defer mysqlShutdownCancel()
		if err := mysqld.Shutdown(mysqlShutdownCtx, mycnf, false, mysqlShutdownTimeout); err != nil {
			log.Error(fmt.Sprintf("failed to shutdown mysqld: %v", err))
		}
	}()

	extraEnv := map[string]string{
		"TABLET_ALIAS": topoproto.TabletAliasString(tabletAlias),
	}
	dbName := initDbNameOverride
	if dbName == "" {
		dbName = "vt_" + initKeyspace
	}

	backupParams := mysqlctl.BackupParams{
		Cnf:                  mycnf,
		Mysqld:               mysqld,
		Logger:               logutil.NewConsoleLogger(),
		Concurrency:          concurrency,
		IncrementalFromPos:   incrementalFromPos,
		HookExtraEnv:         extraEnv,
		TopoServer:           topoServer,
		Keyspace:             initKeyspace,
		Shard:                initShard,
		TabletAlias:          topoproto.TabletAliasString(tabletAlias),
		TabletType:           topodatapb.TabletType_BACKUP,
		Stats:                backupstats.BackupStats(),
		UpgradeSafe:          upgradeSafe,
		IncludeBinlogs:       includeBinlogs,
		MysqlShutdownTimeout: mysqlShutdownTimeout,
		InitSQL: &tabletmanagerdatapb.BackupRequest_InitSQL{
			Queries:     initSQLQueries,
			TabletTypes: initSQLTabletTypes,
			Timeout:     protoutil.DurationToProto(initSQLTimeout),
			FailOnError: initSQLFailOnError,
		},
	}
	if err := validateBinlogFlags(backupParams, restoreWithClone); err != nil {
		return err
	}

	// In initial_backup mode, just take a backup of this empty database.
	if initialBackup {
		// Take a backup of this empty DB without restoring anything.
		// First, initialize it the way InitShardPrimary would, so this backup
		// produces a result that can be used to skip InitShardPrimary entirely.
		// This involves resetting replication (to erase any history) and then
		// creating the main database and some Vitess system tables.
		if err := mysqld.ResetReplication(shutdownCtx); err != nil {
			return fmt.Errorf("can't reset replication: %v", err)
		}
		// We need to switch off super_read_only before we create the database.
		resetFunc, err := mysqld.SetSuperReadOnly(shutdownCtx, false)
		if err != nil {
			return fmt.Errorf("failed to disable super_read_only during backup: %v", err)
		}
		if resetFunc != nil {
			defer func() {
				err := resetFunc()
				if err != nil {
					log.Error("Failed to set super_read_only back to its original value during backup")
				}
			}()
		}
		cmd := mysqlctl.GenerateInitialBinlogEntry()
		if err := mysqld.ExecuteSuperQueryList(shutdownCtx, []string{cmd}); err != nil {
			return err
		}

		backupParams.BackupTime = time.Now()
		// Now we're ready to take the backup.
		phase.Set(phaseNameInitialBackup, int64(1))
		defer phase.Set(phaseNameInitialBackup, int64(0))
		if err := mysqlctl.Backup(shutdownCtx, backupParams); err != nil {
			return fmt.Errorf("backup failed: %v", err)
		}
		deprecatedDurationByPhase.Set("InitialBackup", int64(time.Since(backupParams.BackupTime).Seconds()))
		log.Info("Initial backup successful.")
		phase.Set(phaseNameInitialBackup, int64(0))
		return nil
	}

	// Set up the context for the restore phase. If we are restoring from
	// backup, use shutdownCtx so local mysqld work stops if mysqld exits. If
	// we are cloning, use the main job context because CLONE makes mysqld
	// restart on purpose and we do not want the restore to stop for that.
	restoreCtx := shutdownCtx
	if restoreWithClone {
		restoreCtx = ctx
	}

	restorePos, restoredFrom, keepBinlogs, err := restoreForBackup(restoreCtx, mysqlTermHandler, topoServer, mysqld, mycnf, dbName, extraEnv)
	if err != nil {
		return err
	}

	if err := runBackup(shutdownCtx, topoServer, mysqld, mycnf, restorePos, restoredFrom, keepBinlogs, &backupParams); err != nil {
		return err
	}

	log.Info("Backup successful.")
	return nil
}

// validateBinlogFlags fails before the restore, rather than after catching up, when
// the binlogs cannot be backed up. CLONE does not copy binlogs, so the history would
// silently restart.
func validateBinlogFlags(params mysqlctl.BackupParams, restoreWithClone bool) error {
	if !params.IncludeBinlogs {
		return nil
	}
	if restoreWithClone {
		return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "--include-binlogs cannot be used with --restore-with-clone: CLONE does not copy binary logs")
	}
	return mysqlctl.ValidateIncludeBinlogs(params)
}

// restoreForBackup runs the restore step and returns the position the rest of
// the backup job should use, what that position came from for messages, and
// whether the restore installed the backup's binlogs.
func restoreForBackup(ctx context.Context, mysqlTermHandler *mySQLTermHandler, topoServer *topo.Server, mysqld mysqlctl.MysqlDaemon, mycnf *mysqlctl.Mycnf, dbName string, extraEnv map[string]string) (replication.Position, string, bool, error) {
	if restoreWithClone {
		var restorePos replication.Position

		// CLONE intentionally restarts mysqld. Suppress the resulting OnTerm
		// cancellation and pass mycnf so CloneFromDonor can restart mysqld
		// itself if no process supervisor does.
		err := mysqlTermHandler.ignoreTermsFor(func() error {
			var err error

			restorePos, err = mysqlctl.CloneFromDonor(ctx, topoServer, mysqld, mycnf, initKeyspace, initShard)

			return err
		})
		if err != nil {
			return replication.Position{}, "", false, vterrors.Wrap(err, "restore with clone failed")
		}

		return restorePos, "data cloned from the donor", false, nil
	}

	phase.Set(phaseNameRestoreLastBackup, int64(1))
	defer phase.Set(phaseNameRestoreLastBackup, int64(0))

	backupDir := mysqlctl.GetBackupDir(initKeyspace, initShard)
	log.Info(fmt.Sprintf("Restoring latest backup from directory %v", backupDir))

	restoreAt := time.Now()

	params := mysqlctl.RestoreParams{
		Cnf:                  mycnf,
		Mysqld:               mysqld,
		Logger:               logutil.NewConsoleLogger(),
		Concurrency:          concurrency,
		HookExtraEnv:         extraEnv,
		DeleteBeforeRestore:  true,
		DbName:               dbName,
		Keyspace:             initKeyspace,
		Shard:                initShard,
		Stats:                backupstats.RestoreStats(),
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}

	backupManifest, err := mysqlctl.Restore(ctx, params)
	switch err {
	case nil:
		// If err is nil, we expect backupManifest to be non-nil.
		log.Info(fmt.Sprintf("Successfully restored from backup at replication position %v", backupManifest.Position))
		deprecatedDurationByPhase.Set("RestoreLastBackup", int64(time.Since(restoreAt).Seconds()))
		restoredFrom := "the restored backup"
		if backupManifest.BackupName != "" {
			// Quoted, since the name comes from the MANIFEST and ends up in
			// errors and logs.
			restoredFrom = fmt.Sprintf("restored backup %q", backupManifest.BackupName)
		}
		return backupManifest.Position, restoredFrom, backupManifest.IncludesBinlogs, nil
	case mysqlctl.ErrNoBackup:
		// There is no backup found, but we may be taking the initial backup of a shard.
		if !allowFirstBackup {
			return replication.Position{}, "", false, vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "no backup found; not starting up empty since --allow-first-backup flag was not enabled")
		}

		deprecatedDurationByPhase.Set("RestoreLastBackup", int64(time.Since(restoreAt).Seconds()))
		return replication.Position{}, "", false, nil
	default:
		return replication.Position{}, "", false, vterrors.Wrap(err, "can't restore from backup")
	}
}

// runBackup starts replication from the restore position, catches up to the
// current primary position, and takes a new backup.
func runBackup(ctx context.Context, topoServer *topo.Server, mysqld *mysqlctl.Mysqld, mycnf *mysqlctl.Mycnf, restorePos replication.Position, restoredFrom string, keepBinlogs bool, backupParams *mysqlctl.BackupParams) error {
	// As of MySQL 8.0.21, you can disable redo logging using the ALTER INSTANCE
	// DISABLE INNODB REDO_LOG statement. This functionality is intended for
	// loading data into a new MySQL instance. Disabling redo logging speeds up
	// data loading by avoiding redo log writes and doublewrite buffering.
	disabledRedoLog := false
	if disableRedoLog {
		if err := mysqld.DisableRedoLog(ctx); err != nil {
			log.Warn(fmt.Sprintf("Error disabling redo logging: %v", err))
		} else {
			disabledRedoLog = true
		}
	}

	// We have restored a backup. Now start replication.
	if err := resetReplication(ctx, restorePos, keepBinlogs, mysqld); err != nil {
		return fmt.Errorf("error resetting replication: %v", err)
	}

	tmc := tmclient.NewTabletManagerClient()
	stopAtCommitted := stopsAtCommittedPosition(ctx, mysqld)
	primaryPos, err := startCatchUp(ctx, tmc, topoServer, mysqld, stopAtCommitted)
	if err != nil {
		return err
	}

	log.Info("takeBackup: primary position is: " + primaryPos.String())

	// Fail before catching up if the restored base has GTIDs that tablets would
	// reject. A shard with no primary has already failed in startCatchUp.
	if errantGTIDChecks {
		if err := inVerifyErrantGTIDsPhase(func() error {
			_, err := verifyPosition(ctx, tmc, topoServer, restorePos, positionCheck{what: restoredFrom, restorePos: restorePos})
			return err
		}); err != nil {
			return err
		}
	}

	status, primaryPos, err := catchUpReplicationForBackup(ctx, tmc, topoServer, mysqld, restorePos, primaryPos, stopAtCommitted)
	if err != nil {
		return err
	}

	// Re-enable redo logging before running init SQL, so that init SQL
	// queries run with full crash safety.
	if disabledRedoLog {
		if err := mysqld.EnableRedoLog(ctx); err != nil {
			return fmt.Errorf("failed to re-enable redo log: %v", err)
		}
	}

	// Perform any requested backup initialization queries after catch-up replication.
	if err := mysqlctl.ExecuteBackupInitSQL(ctx, backupParams); err != nil {
		return vterrors.Wrap(err, "failed to execute backup init SQL queries")
	}

	// Set BackupTime after catch-up and init SQL, so the timestamp on the
	// backup postdates any changes made by init SQL queries.
	backupParams.BackupTime = time.Now()

	if restartBeforeBackup {
		restartAt := time.Now()
		log.Info("Proceeding with clean MySQL shutdown and startup to flush all buffers.")

		// Prep for full/clean shutdown (not typically the default)
		if err := mysqld.ExecuteSuperQuery(ctx, "SET GLOBAL innodb_fast_shutdown=0"); err != nil {
			return fmt.Errorf("Could not prep for full shutdown: %v", err)
		}

		// Shutdown, waiting for it to finish
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, mysqlShutdownTimeout+mysqlctl.MysqldShutdownGracePeriod)
		defer shutdownCancel()
		if err := mysqld.Shutdown(shutdownCtx, mycnf, true, mysqlShutdownTimeout); err != nil {
			return fmt.Errorf("Something went wrong during full MySQL shutdown: %v", err)
		}

		// Start MySQL, waiting for it to come up
		if err := mysqld.Start(ctx, mycnf); err != nil {
			return fmt.Errorf("Could not start MySQL after full shutdown: %v", err)
		}

		deprecatedDurationByPhase.Set("RestartBeforeBackup", int64(time.Since(restartAt).Seconds()))
	}

	// Check the position to back up before the upload starts, and set the check
	// that runs just before the backup's MANIFEST is written.
	if errantGTIDChecks {
		if err := inVerifyErrantGTIDsPhase(func() error {
			return verifyNewBackup(ctx, tmc, topoServer, mysqld, restorePos, backupParams)
		}); err != nil {
			return err
		}
	}

	// Now we can take a new backup.
	backupAt := time.Now()
	phase.Set(phaseNameTakeNewBackup, int64(1))
	defer phase.Set(phaseNameTakeNewBackup, int64(0))

	if err := mysqlctl.Backup(ctx, *backupParams); err != nil {
		return fmt.Errorf("error taking backup: %v", err)
	}

	deprecatedDurationByPhase.Set("TakeNewBackup", int64(time.Since(backupAt).Seconds()))
	phase.Set(phaseNameTakeNewBackup, int64(0))

	// Return a non-zero exit code if we didn't meet the replication position
	// goal, even though we took a backup that pushes the high-water mark up.
	if !status.Position.AtLeast(primaryPos) {
		return fmt.Errorf("replication caught up to %v but didn't make it to the goal of %v; a backup was taken anyway to save partial progress, but the operation should still be retried since not all expected data is backed up", status.Position, primaryPos)
	}

	return nil
}

// catchUpReplicationForBackup catches a restored mysqld up to the current
// primary position and returns the final replication status after it stops
// replication again, and the goal it caught up to. With stopAtTarget, when
// replication stops and is restarted, the goal becomes the committed position of
// the primary it restarts from, and replication stops there. Otherwise
// replication restarts as it did before MySQL 8.4.
func catchUpReplicationForBackup(ctx context.Context, tmc tmclient.TabletManagerClient, topoServer *topo.Server, mysqld mysqlctl.MysqlDaemon, restorePos, primaryPos replication.Position, stopAtTarget bool) (replication.ReplicationStatus, replication.Position, error) {
	// Wait for replication to catch up.
	phase.Set(phaseNameCatchupReplication, int64(1))
	defer phase.Set(phaseNameCatchupReplication, int64(0))

	var (
		lastStatus replication.ReplicationStatus
		status     replication.ReplicationStatus
		statusErr  error

		waitStartTime = time.Now()
	)

	lastErr := vterrors.NewLastError("replication catch up", timeoutWaitingForReplicationStatus)
	for {
		if !lastErr.ShouldRetry() {
			return replication.ReplicationStatus{}, replication.Position{}, fmt.Errorf("timeout waiting for replication status after %.0f seconds", timeoutWaitingForReplicationStatus.Seconds())
		}

		select {
		case <-ctx.Done():
			return replication.ReplicationStatus{}, replication.Position{}, fmt.Errorf("error in replication catch up: %v", ctx.Err())
		case <-time.After(time.Second):
		}

		lastStatus = status
		status, statusErr = mysqld.ReplicationStatus(ctx)
		if statusErr != nil {
			lastErr.Record(statusErr)
			log.Warn(fmt.Sprintf("Error getting replication status: %v", statusErr))
			continue
		}

		if status.Position.AtLeast(primaryPos) {
			// We're caught up on replication to at least the point the primary
			// was at when this vtbackup run started.
			log.Info(fmt.Sprintf("Replication caught up to %v after %v", status.Position, time.Since(waitStartTime)))
			deprecatedDurationByPhase.Set("CatchUpReplication", int64(time.Since(waitStartTime).Seconds()))
			break
		}

		if !lastStatus.Position.IsZero() {
			if status.Position.Equal(lastStatus.Position) {
				phaseStatus.Set([]string{phaseNameCatchupReplication, phaseStatusCatchupReplicationStalled}, 1)
			} else {
				phaseStatus.Set([]string{phaseNameCatchupReplication, phaseStatusCatchupReplicationStalled}, 0)
			}
		}

		if !status.Healthy() {
			errStr := "Replication has stopped before backup could be taken. Trying to restart replication."
			log.Warn(errStr)
			lastErr.Record(errors.New(strings.ToLower(errStr)))

			phaseStatus.Set([]string{phaseNameCatchupReplication, phaseStatusCatchupReplicationStopped}, 1)
			if stopAtTarget {
				target, err := readRestartTarget(ctx, tmc, topoServer, restorePos, status.Position)
				if permanent, ok := errors.AsType[permanentError](err); ok {
					return replication.ReplicationStatus{}, replication.Position{}, permanent.error
				}
				if err != nil {
					// The primary can't be read, as while a reparent replaces one that is
					// down. Wait for one, bounded only by the job's deadline, as the errant
					// GTID checks do, instead of counting the wait against the time that
					// replication may stay stopped. The stop position stays in force.
					log.Warn("Can't read the shard primary to restart replication from; waiting for one", slog.Any("error", err))
					lastErr.Record(nil)
				} else if err := startReplicationAt(ctx, mysqld, target); err != nil {
					log.Warn("Failed to restart replication", slog.Any("error", err))
				} else {
					primaryPos = target.pos
				}
			} else if err := startReplication(ctx, mysqld, topoServer); err != nil {
				log.Warn(fmt.Sprintf("Failed to restart replication: %v", err))
			}
		} else {
			// Clear last error as MySQL's replication is healthy
			lastErr.Record(nil)
			phaseStatus.Set([]string{phaseNameCatchupReplication, phaseStatusCatchupReplicationStopped}, 0)
		}
	}

	phase.Set(phaseNameCatchupReplication, int64(0))

	// Stop replication and see where we are.
	if err := mysqld.StopReplication(ctx, nil); err != nil {
		return replication.ReplicationStatus{}, replication.Position{}, fmt.Errorf("can't stop replication: %v", err)
	}

	// Did we make any progress?
	status, statusErr = mysqld.ReplicationStatus(ctx)
	if statusErr != nil {
		return replication.ReplicationStatus{}, replication.Position{}, fmt.Errorf("can't get replication status: %v", statusErr)
	}

	log.Info(fmt.Sprintf("Replication caught up to %v", status.Position))
	if !status.Position.AtLeast(primaryPos) && status.Position.Equal(restorePos) {
		return replication.ReplicationStatus{}, replication.Position{}, fmt.Errorf("not taking backup: replication did not make any progress from restore point: %v", restorePos)
	}

	phaseStatus.Set([]string{phaseNameCatchupReplication, phaseStatusCatchupReplicationStalled}, 0)
	phaseStatus.Set([]string{phaseNameCatchupReplication, phaseStatusCatchupReplicationStopped}, 0)

	return status, primaryPos, nil
}

func resetReplication(ctx context.Context, pos replication.Position, keepBinlogs bool, mysqld mysqlctl.MysqlDaemon) error {
	if err := mysqld.StopReplication(ctx, nil); err != nil {
		return vterrors.Wrap(err, "failed to stop replication")
	}
	if err := mysqld.ResetReplicationParameters(ctx); err != nil {
		return vterrors.Wrap(err, "failed to reset replication")
	}

	if keepBinlogs {
		// The restored binlogs already put gtid_executed at pos; setting the position
		// or resetting replication would delete them.
		return nil
	}

	// Check if we have a position to resume from, if not reset to the beginning of time
	if !pos.IsZero() {
		// Set the position at which to resume from the replication source.
		if err := mysqld.SetReplicationPosition(ctx, pos); err != nil {
			return vterrors.Wrap(err, "failed to set replica position")
		}
	} else {
		if err := mysqld.ResetReplication(ctx); err != nil {
			return vterrors.Wrap(err, "failed to reset replication")
		}
	}
	return nil
}

// startCatchUp starts replication from the shard primary and returns the
// position to catch up to. With stopAtCommitted, it reads the primary's
// committed position first and stops replication there. Otherwise it starts
// replication without a stop position and then reads the primary's position, as
// vtbackup always did before MySQL 8.4.
func startCatchUp(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, mysqld mysqlctl.MysqlDaemon, stopAtCommitted bool) (replication.Position, error) {
	if stopAtCommitted {
		target, err := readInitialTarget(ctx, tmc, ts)
		if err != nil {
			return replication.Position{}, vterrors.Wrap(err, "can't get the primary replication position")
		}
		if err := startReplicationAt(ctx, mysqld, target); err != nil {
			return replication.Position{}, vterrors.Wrap(err, "error starting replication")
		}
		log.Info("Started replication from the shard primary, up to its committed position",
			slog.String("primary", topoproto.TabletAliasString(target.tablet.Alias)),
		)
		return target.pos, nil
	}

	if err := startReplication(ctx, mysqld, ts); err != nil {
		return replication.Position{}, fmt.Errorf("error starting replication: %v", err)
	}

	log.Info("get the current primary replication position, and wait until we catch up")
	// Get the current primary replication position, and wait until we catch up
	// to that point. We do this instead of looking at ReplicationLag
	// because that value can
	// sometimes lie and tell you there's 0 lag when actually replication is
	// stopped. Also, if replication is making progress but is too slow to ever
	// catch up to live changes, we'd rather take a backup of something rather
	// than timing out.

	// Keep retrying if we can't contact the primary. The primary might be
	// changing, moving, or down temporarily.
	var primaryPos replication.Position
	err := retryOnError(ctx, func() error {
		// Add a per-operation timeout so we re-read topo if the primary is unreachable.
		opCtx, optCancel := context.WithTimeout(ctx, operationTimeout)
		defer optCancel()

		pos, err := getPrimaryPosition(opCtx, tmc, ts)
		if err != nil {
			return fmt.Errorf("can't get the primary replication position: %v", err)
		}

		primaryPos = pos
		return nil
	})
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't get the primary replication position after all retries: %v", err)
	}
	return primaryPos, nil
}

func startReplication(ctx context.Context, mysqld mysqlctl.MysqlDaemon, topoServer *topo.Server) error {
	si, err := topoServer.GetShard(ctx, initKeyspace, initShard)
	if err != nil {
		return vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
		// Normal tablets will sit around waiting to be reparented in this case.
		// Since vtbackup is a batch job, we just have to fail.
		return fmt.Errorf("can't start replication after restore: shard %v/%v has no primary", initKeyspace, initShard)
	}
	// TODO(enisoc): Support replicating from another replica, preferably in the
	//   same cell, preferably rdonly, to reduce load on the primary.
	ti, err := topoServer.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return vterrors.Wrapf(err, "Cannot read primary tablet %v", si.PrimaryAlias)
	}

	// Stop replication (in case we're restarting), set replication source, and start replication.
	if err := mysqld.SetReplicationSource(ctx, ti.MysqlHostname, ti.MysqlPort, 0, true, true); err != nil {
		return vterrors.Wrap(err, "MysqlDaemon.SetReplicationSource failed")
	}
	return nil
}

func getPrimaryPosition(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server) (replication.Position, error) {
	si, err := ts.GetShard(ctx, initKeyspace, initShard)
	if err != nil {
		return replication.Position{}, vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
		// Normal tablets will sit around waiting to be reparented in this case.
		// Since vtbackup is a batch job, we just have to fail.
		return replication.Position{}, fmt.Errorf("shard %v/%v has no primary", initKeyspace, initShard)
	}
	ti, err := ts.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't get primary tablet record %v: %v", topoproto.TabletAliasString(si.PrimaryAlias), err)
	}
	posStr, err := tmc.PrimaryPosition(ctx, ti.Tablet)
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't get primary replication position: %v", err)
	}
	pos, err := replication.DecodePosition(posStr)
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't decode primary replication position %q: %v", posStr, err)
	}
	return pos, nil
}

// primaryTarget is the shard primary that vtbackup replicates from, and the
// position that the primary had committed when vtbackup read it.
type primaryTarget struct {
	tablet *topodatapb.Tablet
	pos    replication.Position
	// uuid is the primary's server UUID.
	uuid string
}

// noPrimaryError reports that the shard record names no primary.
type noPrimaryError struct{ error }

// readInitialTarget reads the primary that replication first starts from, and
// its committed position. It retries while they can't be read, since the
// primary might be changing, moving, or down temporarily, but fails at once if
// the shard has no primary, as tablets wait to be reparented but vtbackup is a
// batch job, or if the primary's position can't be decoded.
func readInitialTarget(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server) (primaryTarget, error) {
	var target primaryTarget
	err := retryOnError(ctx, func() error {
		// Add a per-operation timeout so we re-read topo if the primary is unreachable.
		opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		defer cancel()

		var err error
		target, err = readPrimaryTarget(opCtx, tmc, ts)
		if _, ok := errors.AsType[noPrimaryError](err); ok {
			return permanentError{err}
		}
		return err
	})
	return target, err
}

// readPrimaryTarget reads the shard primary and its committed position, its
// gtid_executed: with semi-sync's default AFTER_SYNC wait point, a transaction
// that is waiting for an acknowledgement is already in the primary's binlog, but
// not in gtid_executed. As the errant GTID checks do, it then confirms that the
// primary is still the shard primary in the same term, since a primary that an
// emergency reparent repointed still answers, with writes that the new primary
// lacks.
func readPrimaryTarget(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server) (primaryTarget, error) {
	si, err := ts.GetShard(ctx, initKeyspace, initShard)
	if err != nil {
		return primaryTarget{}, vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
		return primaryTarget{}, noPrimaryError{vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v has no primary", initKeyspace, initShard)}
	}
	primary := topoproto.TabletAliasString(si.PrimaryAlias)
	ti, err := ts.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return primaryTarget{}, vterrors.Wrapf(err, "can't read primary tablet %v", primary)
	}
	status, err := tmc.PrimaryStatus(ctx, ti.Tablet)
	if err != nil {
		return primaryTarget{}, vterrors.Wrapf(err, "can't get the status of primary %v", primary)
	}
	term := primaryTerm{alias: si.PrimaryAlias, termStart: si.PrimaryTermStartTime}
	if err := confirmPrimary(ctx, ts, term, "the position to catch up to"); err != nil {
		return primaryTarget{}, err
	}
	pos, err := replication.DecodePosition(status.Position)
	if err != nil {
		return primaryTarget{}, permanentError{vterrors.Wrapf(err, "can't decode the position %q of primary %v", status.Position, primary)}
	}
	return primaryTarget{tablet: ti.Tablet, pos: pos, uuid: status.ServerUuid}, nil
}

// stopsAtCommittedPosition reports whether catch-up stops at the primary's
// committed position, which it does when this run's mysqld is MySQL 8.4 or
// later. Earlier versions apply with a single thread until they reach a stop
// position, which can make catch-up much slower, so they replicate without one,
// as before. MariaDB positions aren't MySQL56 GTID sets and never get one.
func stopsAtCommittedPosition(ctx context.Context, mysqld mysqlctl.MysqlDaemon) bool {
	version, err := mysqld.GetVersionString(ctx)
	if err != nil {
		log.Warn("Can't read the MySQL version, so catch-up has no stop position", slog.Any("error", err))
		return false
	}
	flavor, ver, err := mysqlctl.ParseVersionString(version)
	if err != nil {
		log.Warn("Can't parse the MySQL version, so catch-up has no stop position", slog.String("version", version), slog.Any("error", err))
		return false
	}
	if flavor == mysqlctl.FlavorMariaDB {
		return false
	}
	return ver.Major > 8 || (ver.Major == 8 && ver.Minor >= 4)
}

// startReplicationAt points replication at target's primary and starts it. With
// a MySQL56 GTID set, replication stops once it has applied target.pos.
// vtbackup replicates from the primary without acknowledging semi-sync, so its
// relay log can hold transactions that are still waiting for an acknowledgement,
// and an emergency reparent can lose them. Stopping at the committed position
// keeps them out of the backup.
func startReplicationAt(ctx context.Context, mysqld mysqlctl.MysqlDaemon, target primaryTarget) error {
	if _, ok := target.pos.GTIDSet.(replication.Mysql56GTIDSet); !ok {
		// Replicate without a stop position, as before.
		if err := mysqld.SetReplicationSource(ctx, target.tablet.MysqlHostname, target.tablet.MysqlPort, 0, true, true); err != nil {
			return vterrors.Wrap(err, "MysqlDaemon.SetReplicationSource failed")
		}
		return nil
	}
	// Stop replication, in case we're restarting, and set the source before
	// replication starts, so that it applies nothing past target.pos.
	if err := mysqld.SetReplicationSource(ctx, target.tablet.MysqlHostname, target.tablet.MysqlPort, 0, true, false); err != nil {
		return vterrors.Wrap(err, "MysqlDaemon.SetReplicationSource failed")
	}
	if err := mysqld.StartReplicationUntilAfter(ctx, target.pos); err != nil {
		return vterrors.Wrap(err, "can't start replication")
	}
	return nil
}

// readRestartTarget reads the shard primary that replication restarts from
// during catch-up on MySQL 8.4 and later, and its committed position. With
// --enable-errant-gtid-checks,
// it returns a permanentError if the primary lacks GTIDs in applied, so that
// vtbackup fails without changing its source, which can't undo them. The applier
// can still apply transactions after applied was read; the check of the data to
// back up catches those.
func readRestartTarget(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, restorePos, applied replication.Position) (primaryTarget, error) {
	readCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	target, err := readPrimaryTarget(readCtx, tmc, ts)
	if err != nil {
		return primaryTarget{}, err
	}
	if errantGTIDChecks {
		primary := topoproto.TabletAliasString(target.tablet.Alias)
		_, errant, err := compareWithPrimary(applied, target.pos, target.uuid)
		if err != nil {
			return primaryTarget{}, permanentError{vterrors.Wrapf(err, "can't parse the server UUID of primary %v", primary)}
		}
		if len(errant) > 0 {
			return primaryTarget{}, permanentError{errantGTIDsError(positionCheck{what: "the data caught up so far", restorePos: restorePos}, errant, primary, target.pos)}
		}
	}
	return target, nil
}

// positionCheck describes one check that a position has no GTIDs the shard primary lacks.
type positionCheck struct {
	// what names the checked position in messages.
	what string
	// restorePos is the restored backup's position. GTIDs in it came from that backup.
	restorePos replication.Position
	// localUUID is this run's mysqld server UUID. GTIDs under it were written by init SQL.
	localUUID string
	// requireCommitted also waits for the primary to commit the GTIDs under its own
	// UUID, such as transactions waiting for a semi-sync acknowledgement.
	requireCommitted bool
}

// primaryTerm is a shard primary and the start of its term, as the shard record names them.
type primaryTerm struct {
	alias     *topodatapb.TabletAlias
	termStart *vttimepb.Time
}

// equal reports whether p and other are the same primary in the same term.
func (p primaryTerm) equal(other primaryTerm) bool {
	return p.alias != nil && proto.Equal(p.alias, other.alias) && proto.Equal(p.termStart, other.termStart)
}

// verifyPosition checks that pos has no GTIDs that the shard primary lacks. It
// retries while the primary is missing, unreachable or hasn't committed GTIDs
// under its own UUID yet (when check.requireCommitted), and fails at once on
// errant GTIDs. The wait between attempts doubles up to operationTimeout, and an
// attempt against a primary that doesn't answer can take operationTimeout too,
// so a long wait still notices a commit or a reparent within about two minutes.
// It returns the primary it verified pos against, which is zero if pos is.
func verifyPosition(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, pos replication.Position, check positionCheck) (primaryTerm, error) {
	if pos.IsZero() {
		return primaryTerm{}, nil
	}
	// Errant GTID detection supports only MySQL56 GTID sets, as on tablets, so
	// don't wait for a primary that would not be checked against.
	if _, ok := pos.GTIDSet.(replication.Mysql56GTIDSet); !ok {
		log.Info("Not verifying a position that isn't a MySQL56 GTID set",
			slog.String("check", check.what),
		)
		return primaryTerm{}, nil
	}
	// stopped reports that a done Context ended the check, and what it was
	// waiting for.
	stopped := func(lastErr error) error {
		return vterrors.Wrapf(ctx.Err(), "stopped verifying %s against the shard primary (last error: %s)", check.what, lastErr)
	}
	for waitTime := time.Second; ; waitTime = min(2*waitTime, operationTimeout) {
		opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		term, err := checkPositionOnce(opCtx, tmc, ts, pos, check)
		cancel()
		if err == nil {
			log.Info("Verified a position against the shard primary",
				slog.String("check", check.what),
				slog.String("primary", topoproto.TabletAliasString(term.alias)),
			)
			return term, nil
		}
		if permanent, ok := errors.AsType[permanentError](err); ok {
			return primaryTerm{}, permanent.error
		}
		if ctx.Err() != nil {
			return primaryTerm{}, stopped(err)
		}
		log.Warn("Waiting to retry verifying a position against the shard primary",
			slog.String("check", check.what),
			slog.Duration("wait", waitTime),
			slog.Any("error", err),
			slog.String("error_code", vterrors.Code(err).String()),
		)

		select {
		case <-ctx.Done():
			return primaryTerm{}, stopped(err)
		case <-time.After(waitTime):
		}
	}
}

// initPhases sets every phase this run can report to 0, so that each series
// exists from the start of the run. The VerifyErrantGTIDs phase is only
// reported with --enable-errant-gtid-checks.
func initPhases() {
	for _, phaseName := range phaseNames {
		phase.Set(phaseName, int64(0))
	}
	if errantGTIDChecks {
		phase.Set(phaseNameVerifyErrantGTIDs, int64(0))
	}
}

// inVerifyErrantGTIDsPhase runs fn, a check of the restored or to-be-backed-up
// data, with the VerifyErrantGTIDs phase active, so that a check that waits
// shows up in the phase metrics. Check 3 runs within the TakeNewBackup phase.
func inVerifyErrantGTIDsPhase(fn func() error) error {
	phase.Set(phaseNameVerifyErrantGTIDs, int64(1))
	defer phase.Set(phaseNameVerifyErrantGTIDs, int64(0))
	return fn()
}

// permanentError wraps an error that verifyPosition, retryOnError and catch-up
// return at once instead of retrying.
type permanentError struct{ error }

// checkPositionOnce makes one attempt at verifyPosition's check. Errant GTIDs,
// and a primary status that can't be parsed, return a permanentError.
func checkPositionOnce(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, pos replication.Position, check positionCheck) (primaryTerm, error) {
	si, err := ts.GetShard(ctx, initKeyspace, initShard)
	if err != nil {
		return primaryTerm{}, vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
		return primaryTerm{}, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "shard %v/%v has no primary", initKeyspace, initShard)
	}
	primary := topoproto.TabletAliasString(si.PrimaryAlias)
	ti, err := ts.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return primaryTerm{}, vterrors.Wrapf(err, "can't read primary tablet %v", primary)
	}
	status, err := tmc.PrimaryStatus(ctx, ti.Tablet)
	if err != nil {
		return primaryTerm{}, vterrors.Wrapf(err, "can't get the status of primary %v", primary)
	}
	// PrimaryStatus doesn't check the tablet type, so a primary that a reparent
	// replaced or repointed during the call still answers, maybe with GTIDs that
	// the new primary lacks. Check against the new primary instead.
	term := primaryTerm{alias: si.PrimaryAlias, termStart: si.PrimaryTermStartTime}
	if err := confirmPrimary(ctx, ts, term, check.what); err != nil {
		return primaryTerm{}, err
	}
	primaryPos, err := replication.DecodePosition(status.Position)
	if err != nil {
		return primaryTerm{}, permanentError{vterrors.Wrapf(err, "can't decode the position %q of primary %v", status.Position, primary)}
	}

	missing, errant, err := compareWithPrimary(pos, primaryPos, status.ServerUuid)
	if err != nil {
		return primaryTerm{}, permanentError{vterrors.Wrapf(err, "can't parse the server UUID of primary %v", primary)}
	}
	if len(errant) > 0 {
		return primaryTerm{}, permanentError{errantGTIDsError(check, errant, primary, primaryPos)}
	}
	if check.requireCommitted && len(missing) > 0 {
		return primaryTerm{}, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "primary %v has not committed %v from %s yet; waiting for a semi-sync acknowledgement or a reparent", primary, missing, check.what)
	}
	return term, nil
}

// compareWithPrimary returns the GTIDs in pos that primaryPos lacks, and those
// of them that are errant by the tablets' rule, as in
// replication.ErrantGTIDsOnReplica: GTIDs under the primary's own server UUID
// are not errant. Errant GTID detection supports only MySQL56 GTID sets, as on
// tablets, so other positions return no GTIDs.
func compareWithPrimary(pos, primaryPos replication.Position, primaryUUID string) (missing, errant replication.Mysql56GTIDSet, err error) {
	posSet, ok := pos.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return nil, nil, nil
	}
	primarySet, ok := primaryPos.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return nil, nil, nil
	}
	primarySID, err := replication.ParseSID(primaryUUID)
	if err != nil {
		return nil, nil, err
	}
	missing = posSet.Difference(primarySet)
	return missing, missing.RemoveUUID(primarySID), nil
}

// confirmPrimary checks, after a check has read the primary's status, that the
// shard record still names term's primary in the same term, and that the
// primary is still a PRIMARY tablet. An ERS starts repointing a primary that
// still answers before it promotes the new one. The repoint makes the old
// primary a REPLICA in topo, then turns off its primary-side semi-sync, which
// commits writes waiting for an acknowledgement that the new primary never
// gets, and the shard record keeps naming it until the new primary's shard sync
// updates the record. Since the type is published first, a status read before
// this check holds those writes only if the type here says REPLICA. Two cases
// still slip through: the topo write of the type fails, and the tablet retries
// it in the background; or ERS cancels a forced demotion after it turned off
// semi-sync, which commits the writes while the type says PRIMARY until the
// repoint, seconds later. Check 3 then catches the backup if the repoint comes
// before the MANIFEST, and otherwise the next run's check 1 does.
func confirmPrimary(ctx context.Context, ts *topo.Server, term primaryTerm, what string) error {
	primary := topoproto.TabletAliasString(term.alias)
	si, err := ts.GetShard(ctx, initKeyspace, initShard)
	if err != nil {
		return vterrors.Wrap(err, "can't read shard")
	}
	if !term.equal(primaryTerm{alias: si.PrimaryAlias, termStart: si.PrimaryTermStartTime}) {
		return vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "the shard primary changed from %v while checking %s", primary, what)
	}
	ti, err := ts.GetTablet(ctx, term.alias)
	if err != nil {
		return vterrors.Wrapf(err, "can't read primary tablet %v", primary)
	}
	if ti.Type != topodatapb.TabletType_PRIMARY {
		return vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "shard primary %v is a %v tablet, as during a reparent, while checking %s", primary, ti.Type, what)
	}
	return nil
}

// errantGTIDsError reports errant GTIDs, with the remedy for each place they came from.
func errantGTIDsError(check positionCheck, errant replication.Mysql56GTIDSet, primary string, primaryPos replication.Position) error {
	msg := fmt.Sprintf("%s has errant GTIDs %v that primary %s (position %v) does not have", check.what, errant, primary, primaryPos)

	var restored replication.Mysql56GTIDSet
	if restoreSet, ok := check.restorePos.GTIDSet.(replication.Mysql56GTIDSet); ok {
		restored = errant.Difference(errant.Difference(restoreSet))
	}
	rest := errant.Difference(restored)
	var initSQL replication.Mysql56GTIDSet
	if localSID, err := replication.ParseSID(check.localUUID); err == nil {
		initSQL = rest.Difference(rest.RemoveUUID(localSID))
		rest = rest.RemoveUUID(localSID)
	}

	switch {
	case len(restored) == 0:
	case restoreWithClone:
		msg += fmt.Sprintf(". %v came from the donor, which has them too. With --clone-from-primary, the donor was a primary that a reparent replaced, and the next run clones the new one. With --clone-from-tablet, fix the donor's errant GTIDs or clone from another tablet", restored)
	default:
		msg += fmt.Sprintf(`. %v came from the restored backup, and tablets restored from it fail with "Errant GTID detected". vtbackup restores the newest backup, so take a new backup of a healthy replica, one without them, with vtctldclient Backup, and the next run starts from it. Then remove the backups that contain them with vtctldclient RemoveBackup, after copying any whose transactions the shard lost. Injecting an empty transaction for each one on the primary instead is safe only if they change no data, as with init SQL in an older vtbackup run: GTIDs under a former primary's UUID usually hold writes that a reparent lost`, restored)
	}
	if len(initSQL) > 0 {
		msg += fmt.Sprintf(". %v were written by this run's --init-backup-sql-queries. Use the statements' NO_WRITE_TO_BINLOG or LOCAL forms, or start the queries with SET sql_log_bin = 0", initSQL)
	}
	switch {
	case len(rest) == 0:
	case restoreWithClone:
		msg += fmt.Sprintf(". vtbackup applied %v during catch-up, and the primary doesn't have them, for example because a reparent lost them. No backup was completed; the next run clones the donor again. With --clone-from-tablet, if the donor applied them too, that run fails with the donor's errant GTIDs", rest)
	default:
		msg += fmt.Sprintf(". vtbackup applied %v during catch-up, and the primary doesn't have them, for example because a reparent lost them. No backup was completed; the next run starts from the previous backup and should succeed", rest)
	}
	return vterrors.New(vtrpc.Code_FAILED_PRECONDITION, msg)
}

// verifyNewBackup checks the position this run is about to back up (check 2),
// and sets backupParams.VerifyPosition to check the position that the backup's
// MANIFEST will record (check 3). Check 3 reuses check 2's result when the
// shard record still names the primary that check 2 verified against, in the
// same term, and the MANIFEST position is one that check 2 verified. Otherwise,
// including when the shard record can't be read, it runs the full check, which
// for a MySQL56 position waits for topo and then needs a reachable primary.
//
// The checks trust the shard record, which a new primary's shard sync updates
// only after its promotion, and which an ERS doesn't wait for. An old primary
// that still answers is repointed during the ERS, and confirmPrimary then sees
// its REPLICA type, with the exceptions it lists. But a dead old primary's
// tablet record still says PRIMARY, so in that gap, normally milliseconds but up
// to --shard-sync-retry-delay if the new primary's topo write fails, the reuse
// still passes, even if the new primary lacks GTIDs in the backup. As when a
// later ERS loses transactions that a backup holds, the next run's check 1
// catches the backup, and replica restores reject it.
func verifyNewBackup(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, mysqld mysqlctl.MysqlDaemon, restorePos replication.Position, backupParams *mysqlctl.BackupParams) error {
	localPos, err := mysqld.PrimaryPosition(ctx)
	if err != nil {
		return vterrors.Wrap(err, "can't get the position to back up")
	}
	localUUID, err := mysqld.GetServerUUID(ctx)
	if err != nil {
		return vterrors.Wrap(err, "can't get the server UUID")
	}
	check := positionCheck{what: "the data to back up", restorePos: restorePos, localUUID: localUUID, requireCommitted: true}
	verified, err := verifyPosition(ctx, tmc, ts, localPos, check)
	if err != nil {
		return err
	}

	check.what = "the new backup"
	backupParams.VerifyPosition = func(ctx context.Context, pos replication.Position) error {
		if verified.alias != nil && localPos.AtLeast(pos) {
			opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
			err := confirmPrimary(opCtx, ts, verified, check.what)
			cancel()
			if err == nil {
				log.Info("The shard primary is unchanged since the data to back up was verified",
					slog.String("check", check.what),
					slog.String("primary", topoproto.TabletAliasString(verified.alias)),
				)
				return nil
			}
		}
		_, err := verifyPosition(ctx, tmc, ts, pos, check)
		return err
	}
	return nil
}

// retryOnError keeps calling the given function until it succeeds, returns a
// permanentError, or the given Context is done. It waits an exponentially
// increasing amount of time between retries to avoid hot-looping. It returns an
// error only for a permanentError, which it unwraps, and a done Context.
func retryOnError(ctx context.Context, fn func() error) error {
	waitTime := 1 * time.Second

	for {
		err := fn()
		if err == nil {
			return nil
		}
		if permanent, ok := errors.AsType[permanentError](err); ok {
			return permanent.error
		}
		log.Error(fmt.Sprintf("Waiting %v to retry after error: %v", waitTime, err))

		select {
		case <-ctx.Done():
			log.Error(fmt.Sprintf("Not retrying after error: %v", ctx.Err()))
			return ctx.Err()
		case <-time.After(waitTime):
			waitTime *= 2
		}
	}
}

func pruneBackups(ctx context.Context, backupStorage backupstorage.BackupStorage, backupDir string) error {
	if minRetentionTime == 0 {
		log.Info("Pruning of old backups is disabled.")
		return nil
	}
	backups, err := backupStorage.ListBackups(ctx, backupDir)
	if err != nil {
		return fmt.Errorf("can't list backups: %v", err)
	}
	numBackups := len(backups)
	if numBackups <= minRetentionCount {
		log.Info(fmt.Sprintf("Found %v backups. Not pruning any since this is within the min_retention_count of %v.", numBackups, minRetentionCount))
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
		if time.Since(backupTime) < minRetentionTime {
			// The oldest remaining backup is not old enough to prune.
			log.Info(fmt.Sprintf("Oldest backup taken at %v has not reached min_retention_time of %v. Nothing left to prune.", backupTime, minRetentionTime))
			break
		}
		// Remove the backup.
		log.Info(fmt.Sprintf("Removing old backup %v from %v, since it's older than min_retention_time of %v", backup.Name(), backupDir, minRetentionTime))
		if err := backupStorage.RemoveBackup(ctx, backupDir, backup.Name()); err != nil {
			return fmt.Errorf("couldn't remove backup %v from %v: %v", backup.Name(), backupDir, err)
		}
		// We successfully removed one backup. Can we afford to prune any more?
		numBackups--
		if numBackups == minRetentionCount {
			log.Info(fmt.Sprintf("Successfully pruned backup count to min_retention_count of %v.", minRetentionCount))
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
	if initialBackup {
		// Check if any backups for the shard already exist in this backup storage location.
		if lastBackup != nil {
			log.Info("At least one complete backup already exists, so there's no need to seed an empty backup. Doing nothing.")
			return false, nil
		}

		// Check whether the shard exists.
		_, shardErr := topoServer.GetShard(ctx, initKeyspace, initShard)
		switch {
		case shardErr == nil:
			// If the shard exists, we should make sure none of the tablets are
			// already in a serving state, because then they might have data
			// that conflicts with the initial backup we're about to take.
			tablets, err := topoServer.GetTabletMapForShard(ctx, initKeyspace, initShard)
			if err != nil {
				// We don't know for sure whether any tablets are serving,
				// so it's not safe to continue.
				return false, fmt.Errorf("failed to check whether shard %v/%v has serving tablets before doing initial backup: %v", initKeyspace, initShard, err)
			}
			for tabletAlias, tablet := range tablets {
				// Check if any tablet has its type set to one of the serving types.
				// If so, it's too late to do an initial backup.
				if tablet.IsInServingGraph() {
					return false, fmt.Errorf("refusing to upload initial backup of empty database: the shard %v/%v already has at least one tablet that may be serving (%v); you must take a backup from a live tablet instead", initKeyspace, initShard, tabletAlias)
				}
			}
			log.Info(fmt.Sprintf("Shard %v/%v exists but has no serving tablets.", initKeyspace, initShard))
		case topo.IsErrType(shardErr, topo.NoNode):
			// The shard doesn't exist, so we know no tablets are running.
			log.Info(fmt.Sprintf("Shard %v/%v doesn't exist; assuming it has no serving tablets.", initKeyspace, initShard))
		default:
			// If we encounter any other error, we don't know for sure whether
			// the shard exists, so it's not safe to continue.
			return false, fmt.Errorf("failed to check whether shard %v/%v exists before doing initial backup: %v", initKeyspace, initShard, err)
		}

		log.Info(fmt.Sprintf("Shard %v/%v has no existing backups. Creating initial backup.", initKeyspace, initShard))
		return true, nil
	}

	// We need at least one backup so we can restore first, unless the user explicitly says we don't
	if len(backups) == 0 && !allowFirstBackup {
		return false, errors.New("no existing backups to restore from; backup is not possible since --allow-first-backup flag was not enabled")
	}
	if lastBackup == nil {
		if allowFirstBackup {
			// There's no complete backup, but we were told to take one from scratch anyway.
			return true, nil
		}
		return false, errors.New("no complete backups to restore from; backup is not possible since --allow-first-backup flag was not enabled")
	}

	// Has it been long enough since the last complete backup to need a new one?
	if minBackupInterval == 0 {
		// No minimum interval is set, so always backup.
		return true, nil
	}
	lastBackupTime, err := parseBackupTime(lastBackup.Name())
	if err != nil {
		return false, fmt.Errorf("can't check last backup time: %v", err)
	}
	if elapsedTime := time.Since(lastBackupTime); elapsedTime < minBackupInterval {
		// It hasn't been long enough yet.
		log.Info(fmt.Sprintf("Skipping backup since only %v has elapsed since the last backup at %v, which is less than the min_backup_interval of %v.", elapsedTime, lastBackupTime, minBackupInterval))
		return false, nil
	}
	// It has been long enough.
	log.Info(fmt.Sprintf("The last backup was taken at %v, which is older than the min_backup_interval of %v.", lastBackupTime, minBackupInterval))
	return true, nil
}

func lastCompleteBackup(ctx context.Context, backups []backupstorage.BackupHandle) backupstorage.BackupHandle {
	if len(backups) == 0 {
		return nil
	}

	// Backups are sorted in ascending order by start time. Start at the end.
	for _, backup := range slices.Backward(backups) {
		// Check if this backup is complete by looking for the MANIFEST file,
		// which is written at the end after all files are uploaded.
		if err := checkBackupComplete(ctx, backup); err != nil {
			log.Warn(fmt.Sprintf("Ignoring backup %v because it's incomplete: %v", backup.Name(), err))
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

	log.Info(fmt.Sprintf("Found complete backup %v taken at position %v", backup.Name(), manifest.Position.String()))
	return nil
}
