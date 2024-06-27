package mysqlctl

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// location to store the mysql shell backup
	mysqlShellBackupLocation = flag.String("mysql_shell_backup_location", "", "location where the backup will be stored")
	mysqlShellFlags          = flag.String("mysql_shell_flags", "--defaults-file=/dev/null --js -h localhost", "execution flags to pass to mysqlsh binary")
	// flags to pass through to backup phase
	mysqlShellDumpFlags = flag.String("mysql_shell_dump_flags",
		`{"threads": 2}`,
		"flags to pass to mysql shell dump utility. This should be a JSON string and will be saved in the MANIFEST")
	// flags to pass through to extract phase of restore
	mysqlShellLoadFlags = flag.String("mysql_shell_load_flags",
		`{"threads": 2, "updateGtidSet": "replace", "skipBinlog": true, "progressFile": ""}`,
		"flags to pass to mysql shell load utility. This should be a JSON string")
	// additional flags
	mysqlShellBackupShouldDrain = flag.Bool("mysql_shell_should_drain",
		false, "decide if we should drain while taking a backup or continue to serving traffic")
	mysqlShellSpeedUpRestore = flag.Bool("mysql_shell_speedup_restore",
		false, "speed up restore by disabling redo logging and double write buffer during the restore process")

	MySQLShellPreCheckError = errors.New("MySQLShellPreCheckError")
)

// MySQLShellBackupManifest represents a backup.
type MySQLShellBackupManifest struct {
	// BackupManifest is an anonymous embedding of the base manifest struct.
	// Note that the manifest itself doesn't fill the Position field, as we have
	// no way of fetching that information from mysqlsh at the moment.
	BackupManifest

	// Location of the backup directory
	BackupLocation string
	// Params are the parameters that backup was created with
	Params string
}

// MySQLShellBackupEngine encapsulates the logic to implement the restoration
// of a mysql-shell based backup.
type MySQLShellBackupEngine struct {
}

const (
	mysqlShellBackupBinaryName = "mysqlsh"
	mysqlShellBackupEngineName = "mysqlshell"
)

func (be *MySQLShellBackupEngine) ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (result BackupResult, finalErr error) {
	params.Logger.Infof("Starting ExecuteBackup in %s", params.TabletAlias)

	err := be.backupPreCheck()
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "failed backup precheck")
	}

	start := time.Now().UTC()
	location := path.Join(*mysqlShellBackupLocation, params.Keyspace, params.Shard, start.Format("2006-01-02_15-04-05"))

	args := []string{}

	if *mysqlShellFlags != "" {
		args = append(args, strings.Fields(*mysqlShellFlags)...)
	}

	args = append(args, "-e", fmt.Sprintf("util.dumpSchemas([\"vt_%s\"], %q, %s)",
		params.Keyspace,
		location,
		*mysqlShellDumpFlags,
	))

	cmd := exec.CommandContext(ctx, mysqlShellBackupBinaryName, args...)

	params.Logger.Infof("running %s", cmd.String())

	cmdOut, err := cmd.StdoutPipe()
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "cannot create stdout pipe")
	}
	cmdErr, err := cmd.StderrPipe()
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "cannot create stderr pipe")
	}
	if err := cmd.Start(); err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't start xbstream")
	}

	cmdWg := &sync.WaitGroup{}
	cmdWg.Add(2)
	go scanLinesToLogger(mysqlShellBackupEngineName+" stdout", cmdOut, params.Logger, cmdWg.Done)
	go scanLinesToLogger(mysqlShellBackupEngineName+" stderr", cmdErr, params.Logger, cmdWg.Done)
	cmdWg.Wait()

	// Get exit status.
	if err := cmd.Wait(); err != nil {
		return BackupUnusable, vterrors.Wrap(err, mysqlShellBackupEngineName+" failed")
	}

	// open the MANIFEST
	params.Logger.Infof("Writing backup MANIFEST")
	mwc, err := bh.AddFile(ctx, backupManifestFileName, backupstorage.FileSizeUnknown)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot add %v to backup", backupManifestFileName)
	}
	defer closeFile(mwc, backupManifestFileName, params.Logger, &finalErr)

	// JSON-encode and write the MANIFEST
	bm := &MySQLShellBackupManifest{
		// Common base fields
		BackupManifest: BackupManifest{
			BackupMethod: mysqlShellBackupEngineName,
			// the position is empty here because we have no way of capturing it from mysqlsh
			// we will capture it when doing the restore as mysqlsh can replace the GTIDs with
			// what it has stored in the backup.
			Position:     replication.Position{},
			BackupTime:   start.Format(time.RFC3339),
			FinishedTime: time.Now().UTC().Format(time.RFC3339),
		},

		// mysql shell backup specific fields
		BackupLocation: location,
		Params:         *mysqlShellLoadFlags,
	}

	data, err := json.MarshalIndent(bm, "", "  ")
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot JSON encode %v", backupManifestFileName)
	}
	if _, err := mwc.Write([]byte(data)); err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot write %v", backupManifestFileName)
	}

	params.Logger.Infof("Backup completed")
	return BackupUsable, nil
}

func (be *MySQLShellBackupEngine) ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (*BackupManifest, error) {
	params.Logger.Infof("Calling ExecuteRestore for %s (DeleteBeforeRestore: %v)", params.DbName, params.DeleteBeforeRestore)

	err := be.restorePreCheck()
	if err != nil {
		return nil, vterrors.Wrap(err, "failed restore precheck")
	}

	var bm MySQLShellBackupManifest
	if err := getBackupManifestInto(ctx, bh, &bm); err != nil {
		return nil, err
	}

	// mark restore as in progress
	if err := createStateFile(params.Cnf); err != nil {
		return nil, err
	}

	// make sure semi-sync is disabled, otherwise we will wait forever for acknowledgements
	err = params.Mysqld.SetSemiSyncEnabled(ctx, false, false)
	if err != nil {
		return nil, vterrors.Wrap(err, "disable semi-sync failed")
	}

	// if we received a RestoreFromBackup API call instead of it being a command line argument,
	// we need to first clean the host before we start the restore.
	if params.DeleteBeforeRestore {
		params.Logger.Infof("restoring on an existing tablet, so dropping database %q", params.DbName)

		err = params.Mysqld.ExecuteSuperQueryList(ctx,
			[]string{fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", params.DbName)},
		)
		if err != nil {
			return nil, vterrors.Wrap(err, fmt.Sprintf("dropping database %q failed", params.DbName))
		}

	}

	// we need to get rid of all the current replication information on the host.
	err = params.Mysqld.ResetReplication(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to reset replication")
	}

	// this is required so we can load the backup generated by MySQL Shell. we will disable it afterwards.
	err = params.Mysqld.ExecuteSuperQueryList(ctx, []string{"SET GLOBAL LOCAL_INFILE=1"})
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to set local_infile=1")
	}

	if *mysqlShellSpeedUpRestore {
		// disable redo logging and double write buffer if we are configured to do so.
		err = params.Mysqld.ExecuteSuperQueryList(ctx, []string{"ALTER INSTANCE DISABLE INNODB REDO_LOG"})
		if err != nil {
			return nil, vterrors.Wrap(err, "unable to disable REDO_LOG")
		}
		params.Logger.Infof("Disabled REDO_LOG")

		defer func() { // re-enable once we are done with the restore.
			err := params.Mysqld.ExecuteSuperQueryList(ctx, []string{"ALTER INSTANCE ENABLE INNODB REDO_LOG"})
			if err != nil {
				params.Logger.Errorf("unable to re-enable REDO_LOG: %v", err)
			} else {
				params.Logger.Infof("Disabled REDO_LOG")
			}
		}()
	}

	args := []string{}

	if *mysqlShellFlags != "" {
		args = append(args, strings.Fields(*mysqlShellFlags)...)
	}

	args = append(args, "-e", fmt.Sprintf("util.loadDump(%q, %s)",
		bm.BackupLocation,
		*mysqlShellLoadFlags,
	))

	cmd := exec.CommandContext(ctx, "mysqlsh", args...)

	params.Logger.Infof("running %s", cmd.String())

	cmdOut, err := cmd.StdoutPipe()
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create stdout pipe")
	}
	cmdErr, err := cmd.StderrPipe()
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create stderr pipe")
	}
	if err := cmd.Start(); err != nil {
		return nil, vterrors.Wrap(err, "can't start xbstream")
	}

	cmdWg := &sync.WaitGroup{}
	cmdWg.Add(2)
	go scanLinesToLogger(mysqlShellBackupEngineName+" stdout", cmdOut, params.Logger, cmdWg.Done)
	go scanLinesToLogger(mysqlShellBackupEngineName+" stderr", cmdErr, params.Logger, cmdWg.Done)
	cmdWg.Wait()

	// Get the exit status.
	if err := cmd.Wait(); err != nil {
		return nil, vterrors.Wrap(err, mysqlShellBackupEngineName+" failed")
	}
	params.Logger.Infof("%s completed successfully", mysqlShellBackupBinaryName)

	// disable local_infile now that the restore is done.
	err = params.Mysqld.ExecuteSuperQueryList(ctx, []string{
		"SET GLOBAL LOCAL_INFILE=0",
	})
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to set local_infile=0")
	}
	params.Logger.Infof("set local_infile=0")

	// since MySQL Shell backups do not store the Executed GTID position in the manifest, we need to
	// fetch it and override in the manifest we are returning so Vitess can set it again. alternatively
	// we could have a flag in the future where the backup engine controls if it needs to be set or not.
	pos, err := params.Mysqld.PrimaryPosition(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, "failure getting restored position")
	}

	params.Logger.Infof("retrieved primary position after restore")
	bm.BackupManifest.Position = pos

	params.Logger.Infof("Restore completed")

	return &bm.BackupManifest, nil
}

// ShouldDrainForBackup satisfies the BackupEngine interface
// MySQL Shell backups can be taken while MySQL is running so we can control this via a flag.
func (be *MySQLShellBackupEngine) ShouldDrainForBackup(req *tabletmanagerdatapb.BackupRequest) bool {
	return *mysqlShellBackupShouldDrain
}

func (be *MySQLShellBackupEngine) backupPreCheck() error {
	if *mysqlShellBackupLocation == "" {
		return fmt.Errorf("%w: no backup location set via --mysql_shell_location", MySQLShellPreCheckError)
	}

	if *mysqlShellFlags == "" || !strings.Contains(*mysqlShellFlags, "--js") {
		return fmt.Errorf("%w: at least the --js flag is required", MySQLShellPreCheckError)
	}

	return nil
}

func (be *MySQLShellBackupEngine) restorePreCheck() error {
	if *mysqlShellFlags == "" {
		return fmt.Errorf("%w: at least the --js flag is required", MySQLShellPreCheckError)
	}

	loadFlags := map[string]interface{}{}
	err := json.Unmarshal([]byte(*mysqlShellLoadFlags), &loadFlags)
	if err != nil {
		return fmt.Errorf("%w: unable to parse JSON of load flags", MySQLShellPreCheckError)
	}

	if val, ok := loadFlags["updateGtidSet"]; !ok || val != "replace" {
		return fmt.Errorf("%w: mysql-shell needs to restore with updateGtidSet set to \"replace\" to work with Vitess", MySQLShellPreCheckError)
	}

	if val, ok := loadFlags["progressFile"]; !ok || val != "" {
		return fmt.Errorf("%w: \"progressFile\" needs to be empty as vitess always starts a restore from scratch", MySQLShellPreCheckError)
	}

	return nil
}

func init() {
	BackupRestoreEngineMap[mysqlShellBackupEngineName] = &MySQLShellBackupEngine{}
}
