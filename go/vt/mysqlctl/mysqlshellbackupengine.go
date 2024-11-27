/*
Copyright 2024 The Vitess Authors.

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

package mysqlctl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// location to store the mysql shell backup
	mysqlShellBackupLocation = ""
	// flags passed to the mysql shell utility, used both on dump/restore
	mysqlShellFlags = "--defaults-file=/dev/null --js -h localhost"
	// flags passed to the Dump command, as a JSON string
	mysqlShellDumpFlags = `{"threads": 4}`
	// flags passed to the Load command, as a JSON string
	mysqlShellLoadFlags = `{"threads": 4, "loadUsers": true, "updateGtidSet": "replace", "skipBinlog": true, "progressFile": ""}`
	// drain a tablet when taking a backup
	mysqlShellBackupShouldDrain = false
	// disable redo logging and double write buffer
	mysqlShellSpeedUpRestore = false

	// use when checking if we need to create the directory on the local filesystem or not.
	knownObjectStoreParams = []string{"s3BucketName", "osBucketName", "azureContainerName"}

	MySQLShellPreCheckError = errors.New("MySQLShellPreCheckError")

	// internal databases not backed up by MySQL Shell
	internalDBs = []string{
		"information_schema", "mysql", "ndbinfo", "performance_schema", "sys",
	}
	// reserved MySQL users https://dev.mysql.com/doc/refman/8.0/en/reserved-accounts.html
	reservedUsers = []string{
		"mysql.sys@localhost", "mysql.session@localhost", "mysql.infoschema@localhost",
	}
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

func init() {
	BackupRestoreEngineMap[mysqlShellBackupEngineName] = &MySQLShellBackupEngine{
		binaryName: "mysqlsh",
	}

	for _, cmd := range []string{"vtcombo", "vttablet", "vtbackup", "vttestserver", "vtctldclient"} {
		servenv.OnParseFor(cmd, registerMysqlShellBackupEngineFlags)
	}
}

func registerMysqlShellBackupEngineFlags(fs *pflag.FlagSet) {
	fs.StringVar(&mysqlShellBackupLocation, "mysql-shell-backup-location", mysqlShellBackupLocation, "location where the backup will be stored")
	fs.StringVar(&mysqlShellFlags, "mysql-shell-flags", mysqlShellFlags, "execution flags to pass to mysqlsh binary to be used during dump/load")
	fs.StringVar(&mysqlShellDumpFlags, "mysql-shell-dump-flags", mysqlShellDumpFlags, "flags to pass to mysql shell dump utility. This should be a JSON string and will be saved in the MANIFEST")
	fs.StringVar(&mysqlShellLoadFlags, "mysql-shell-load-flags", mysqlShellLoadFlags, "flags to pass to mysql shell load utility. This should be a JSON string")
	fs.BoolVar(&mysqlShellBackupShouldDrain, "mysql-shell-should-drain", mysqlShellBackupShouldDrain, "decide if we should drain while taking a backup or continue to serving traffic")
	fs.BoolVar(&mysqlShellSpeedUpRestore, "mysql-shell-speedup-restore", mysqlShellSpeedUpRestore, "speed up restore by disabling redo logging and double write buffer during the restore process")
}

// MySQLShellBackupEngine encapsulates the logic to implement the restoration
// of a mysql-shell based backup.
type MySQLShellBackupEngine struct {
	binaryName string
}

const (
	mysqlShellBackupEngineName = "mysqlshell"
	mysqlShellLockMessage      = "Global read lock has been released"
)

func (be *MySQLShellBackupEngine) ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (result BackupResult, finalErr error) {
	params.Logger.Infof("Starting ExecuteBackup in %s", params.TabletAlias)

	location := path.Join(mysqlShellBackupLocation, bh.Directory(), bh.Name())

	err := be.backupPreCheck(location)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "failed backup precheck")
	}

	serverUUID, err := params.Mysqld.GetServerUUID(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get server uuid")
	}

	mysqlVersion, err := params.Mysqld.GetVersionString(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get MySQL version")
	}

	args := []string{}
	if mysqlShellFlags != "" {
		args = append(args, strings.Fields(mysqlShellFlags)...)
	}

	args = append(args, "-e", fmt.Sprintf("util.dumpInstance(%q, %s)",
		location,
		mysqlShellDumpFlags,
	))

	// to be able to get the consistent GTID sets, we will acquire a global read lock before starting mysql shell.
	// oncce we have the lock, we start it and wait unti it has acquired and release its global read lock, which
	// should guarantee that both use and mysql shell are seeing the same executed GTID sets.
	// after this we release the lock so that replication can continue. this usually should take just a few seconds.
	params.Logger.Infof("acquiring a global read lock before fetching the executed GTID sets")
	err = params.Mysqld.AcquireGlobalReadLock(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "failed to acquire read lock to start backup")
	}
	lockAcquired := time.Now() // we will report how long we hold the lock for

	// we need to release the global read lock in case the backup fails to start and
	// the lock wasn't released by releaseReadLock() yet. context might be expired,
	// so we pass a new one.
	defer func() { _ = params.Mysqld.ReleaseGlobalReadLock(context.Background()) }()

	posBeforeBackup, err := params.Mysqld.PrimaryPosition(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "failed to fetch position")
	}

	cmd := exec.CommandContext(ctx, be.binaryName, args...)

	params.Logger.Infof("running %s", cmd.String())

	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()
	lockWaiterReader, lockWaiterWriter := io.Pipe()

	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	combinedErr := io.TeeReader(stderrReader, lockWaiterWriter)

	cmdWg := &sync.WaitGroup{}
	cmdWg.Add(3)
	go releaseReadLock(ctx, lockWaiterReader, params, cmdWg, lockAcquired)
	go scanLinesToLogger(mysqlShellBackupEngineName+" stdout", stdoutReader, params.Logger, cmdWg.Done)
	go scanLinesToLogger(mysqlShellBackupEngineName+" stderr", combinedErr, params.Logger, cmdWg.Done)

	// we run the command, wait for it to complete and close all pipes so the goroutines can complete on their own.
	// after that we can process if an error has happened or not.
	err = cmd.Run()

	stdoutWriter.Close()
	stderrWriter.Close()
	lockWaiterWriter.Close()
	cmdWg.Wait()

	if err != nil {
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
			Position:       posBeforeBackup,
			PurgedPosition: posBeforeBackup,
			BackupTime:     FormatRFC3339(params.BackupTime.UTC()),
			FinishedTime:   FormatRFC3339(time.Now().UTC()),
			ServerUUID:     serverUUID,
			TabletAlias:    params.TabletAlias,
			Keyspace:       params.Keyspace,
			Shard:          params.Shard,
			MySQLVersion:   mysqlVersion,
			UpgradeSafe:    true,
		},

		// mysql shell backup specific fields
		BackupLocation: location,
		Params:         mysqlShellLoadFlags,
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

	shouldDeleteUsers, err := be.restorePreCheck(ctx, params)
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

	params.Logger.Infof("restoring on an existing tablet, so dropping database %q", params.DbName)

	readonly, err := params.Mysqld.IsSuperReadOnly(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, fmt.Sprintf("checking if mysqld has super_read_only=enable: %v", err))
	}

	if readonly {
		resetFunc, err := params.Mysqld.SetSuperReadOnly(ctx, false)
		if err != nil {
			return nil, vterrors.Wrap(err, fmt.Sprintf("unable to disable super-read-only: %v", err))
		}

		defer func() {
			err := resetFunc()
			if err != nil {
				params.Logger.Errorf("Not able to set super_read_only to its original value after restore")
			}
		}()
	}

	err = cleanupMySQL(ctx, params, shouldDeleteUsers)
	if err != nil {
		log.Errorf(err.Error())
		// time.Sleep(time.Minute * 2)
		return nil, vterrors.Wrap(err, "error cleaning MySQL")
	}

	// we need to get rid of all the current replication information on the host.
	err = params.Mysqld.ResetReplication(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to reset replication")
	}

	// this is required so we can load the backup generated by MySQL Shell. we will disable it afterwards.
	err = params.Mysqld.ExecuteSuperQuery(ctx, "SET GLOBAL LOCAL_INFILE=1")
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to set local_infile=1")
	}

	if mysqlShellSpeedUpRestore {
		// disable redo logging and double write buffer if we are configured to do so.
		err = params.Mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE DISABLE INNODB REDO_LOG")
		if err != nil {
			return nil, vterrors.Wrap(err, "unable to disable REDO_LOG")
		}
		params.Logger.Infof("Disabled REDO_LOG")

		defer func() { // re-enable once we are done with the restore.
			err := params.Mysqld.ExecuteSuperQuery(ctx, "ALTER INSTANCE ENABLE INNODB REDO_LOG")
			if err != nil {
				params.Logger.Errorf("unable to re-enable REDO_LOG: %v", err)
			} else {
				params.Logger.Infof("Disabled REDO_LOG")
			}
		}()
	}

	// we need to disable SuperReadOnly otherwise we won't be able to restore the backup properly.
	// once the backups is complete, we will restore it to its previous state.
	resetFunc, err := be.handleSuperReadOnly(ctx, params)
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to disable super-read-only")
	}
	defer resetFunc()

	args := []string{}

	if mysqlShellFlags != "" {
		args = append(args, strings.Fields(mysqlShellFlags)...)
	}

	args = append(args, "-e", fmt.Sprintf("util.loadDump(%q, %s)",
		bm.BackupLocation,
		mysqlShellLoadFlags,
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
	params.Logger.Infof("%s completed successfully", be.binaryName)

	// disable local_infile now that the restore is done.
	err = params.Mysqld.ExecuteSuperQuery(ctx, "SET GLOBAL LOCAL_INFILE=0")
	if err != nil {
		return nil, vterrors.Wrap(err, "unable to set local_infile=0")
	}
	params.Logger.Infof("set local_infile=0")

	params.Logger.Infof("Restore completed")

	return &bm.BackupManifest, nil
}

// ShouldDrainForBackup satisfies the BackupEngine interface
// MySQL Shell backups can be taken while MySQL is running so we can control this via a flag.
func (be *MySQLShellBackupEngine) ShouldDrainForBackup(req *tabletmanagerdatapb.BackupRequest) bool {
	return mysqlShellBackupShouldDrain
}

// ShouldStartMySQLAfterRestore signifies if this backup engine needs to restart MySQL once the restore is completed.
// Since MySQL Shell operates on a live MySQL instance, there is no need to start it once the restore is completed
func (be *MySQLShellBackupEngine) ShouldStartMySQLAfterRestore() bool {
	return false
}

func (be *MySQLShellBackupEngine) Name() string { return mysqlShellBackupEngineName }

func (be *MySQLShellBackupEngine) backupPreCheck(location string) error {
	if mysqlShellBackupLocation == "" {
		return fmt.Errorf("%w: no backup location set via --mysql-shell-backup-location", MySQLShellPreCheckError)
	}

	if mysqlShellFlags == "" || !strings.Contains(mysqlShellFlags, "--js") {
		return fmt.Errorf("%w: at least the --js flag is required in the value of the flag --mysql-shell-flags", MySQLShellPreCheckError)
	}

	// make sure the targe directory exists if the target location for the backup is not an object store
	// (e.g. is the local filesystem) as MySQL Shell doesn't create the entire path beforehand:
	isObjectStorage := false
	for _, objStore := range knownObjectStoreParams {
		if strings.Contains(mysqlShellDumpFlags, objStore) {
			isObjectStorage = true
			break
		}
	}

	if !isObjectStorage {
		err := os.MkdirAll(location, 0o750)
		if err != nil {
			return fmt.Errorf("failure creating directory %s: %w", location, err)
		}
	}

	return nil
}

func (be *MySQLShellBackupEngine) restorePreCheck(ctx context.Context, params RestoreParams) (shouldDeleteUsers bool, err error) {
	if mysqlShellFlags == "" {
		return shouldDeleteUsers, fmt.Errorf("%w: at least the --js flag is required in the value of the flag --mysql-shell-flags", MySQLShellPreCheckError)
	}

	loadFlags := map[string]interface{}{}
	err = json.Unmarshal([]byte(mysqlShellLoadFlags), &loadFlags)
	if err != nil {
		return false, fmt.Errorf("%w: unable to parse JSON of load flags", MySQLShellPreCheckError)
	}

	if val, ok := loadFlags["updateGtidSet"]; !ok || val != "replace" {
		return false, fmt.Errorf("%w: mysql-shell needs to restore with updateGtidSet set to \"replace\" to work with Vitess", MySQLShellPreCheckError)
	}

	if val, ok := loadFlags["progressFile"]; !ok || val != "" {
		return false, fmt.Errorf("%w: \"progressFile\" needs to be empty as vitess always starts a restore from scratch", MySQLShellPreCheckError)
	}

	if val, ok := loadFlags["skipBinlog"]; !ok || val != true {
		return false, fmt.Errorf("%w: \"skipBinlog\" needs to set to true", MySQLShellPreCheckError)
	}

	if val, ok := loadFlags["loadUsers"]; ok && val == true {
		shouldDeleteUsers = true
	}

	if mysqlShellSpeedUpRestore {
		version, err := params.Mysqld.GetVersionString(ctx)
		if err != nil {
			return false, fmt.Errorf("%w: failed to fetch MySQL version: %v", MySQLShellPreCheckError, err)
		}

		capableOf := mysql.ServerVersionCapableOf(version)
		capable, err := capableOf(capabilities.DisableRedoLogFlavorCapability)
		if err != nil {
			return false, fmt.Errorf("%w: error checking if server supports disabling redo log: %v", MySQLShellPreCheckError, err)
		}

		if !capable {
			return false, fmt.Errorf("%w: MySQL version doesn't support disabling the redo log (must be >=8.0.21)", MySQLShellPreCheckError)
		}
	}

	return shouldDeleteUsers, nil
}

func (be *MySQLShellBackupEngine) handleSuperReadOnly(ctx context.Context, params RestoreParams) (func(), error) {
	readonly, err := params.Mysqld.IsSuperReadOnly(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, fmt.Sprintf("checking if mysqld has super_read_only=enable: %v", err))
	}

	params.Logger.Infof("Is Super Read Only: %v", readonly)

	if readonly {
		resetFunc, err := params.Mysqld.SetSuperReadOnly(ctx, false)
		if err != nil {
			return nil, vterrors.Wrap(err, fmt.Sprintf("unable to disable super-read-only: %v", err))
		}

		return func() {
			err := resetFunc()
			if err != nil {
				params.Logger.Errorf("Not able to set super_read_only to its original value after restore")
			}
		}, nil
	}

	return func() {}, nil
}

// releaseReadLock will keep reading the MySQL Shell STDERR waiting until the point it has acquired its lock
func releaseReadLock(ctx context.Context, reader io.Reader, params BackupParams, wg *sync.WaitGroup, lockAcquired time.Time) {
	defer wg.Done()

	scanner := bufio.NewScanner(reader)
	released := false
	for scanner.Scan() {
		line := scanner.Text()

		if !released {

			if !strings.Contains(line, mysqlShellLockMessage) {
				continue
			}
			released = true

			params.Logger.Infof("mysql shell released its global read lock, doing the same")

			err := params.Mysqld.ReleaseGlobalReadLock(ctx)
			if err != nil {
				params.Logger.Errorf("unable to release global read lock: %v", err)
			}

			params.Logger.Infof("global read lock released after %v", time.Since(lockAcquired))
		}
	}
	if err := scanner.Err(); err != nil {
		params.Logger.Errorf("error reading from reader: %v", err)
	}

	if !released {
		params.Logger.Errorf("could not release global lock earlier")
	}
}

func cleanupMySQL(ctx context.Context, params RestoreParams, shouldDeleteUsers bool) error {
	params.Logger.Infof("Cleaning up MySQL ahead of a restore")
	result, err := params.Mysqld.FetchSuperQuery(ctx, "SHOW DATABASES")
	if err != nil {
		return err
	}

	// drop all databases
	for _, row := range result.Rows {
		dbName := row[0].ToString()
		if slices.Contains(internalDBs, dbName) {
			continue // not dropping internal DBs
		}

		params.Logger.Infof("Dropping DB %q", dbName)
		err = params.Mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", row[0].ToString()))
		if err != nil {
			return fmt.Errorf("error droppping database %q: %w", row[0].ToString(), err)
		}
	}

	if shouldDeleteUsers {
		// get current user
		var currentUser string
		result, err = params.Mysqld.FetchSuperQuery(ctx, "SELECT user()")
		if err != nil {
			return fmt.Errorf("error fetching current user: %w", err)
		}

		for _, row := range result.Rows {
			currentUser = row[0].ToString()
		}

		// drop all users except reserved ones
		result, err = params.Mysqld.FetchSuperQuery(ctx, "SELECT user, host FROM mysql.user")
		if err != nil {
			return err
		}

		for _, row := range result.Rows {
			user := fmt.Sprintf("%s@%s", row[0].ToString(), row[1].ToString())

			if user == currentUser {
				continue // we don't drop the current user
			}
			if slices.Contains(reservedUsers, user) {
				continue // we skip reserved MySQL users
			}

			params.Logger.Infof("Dropping User %q", user)
			err = params.Mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf("DROP USER '%s'@'%s'", row[0].ToString(), row[1].ToString()))
			if err != nil {
				return fmt.Errorf("error droppping user %q: %w", user, err)
			}
		}
	}

	return err
}
