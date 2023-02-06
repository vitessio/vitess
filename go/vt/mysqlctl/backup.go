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

package mysqlctl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file handles the backup and restore related code

const (
	// the three bases for files to restore
	backupInnodbDataHomeDir     = "InnoDBData"
	backupInnodbLogGroupHomeDir = "InnoDBLog"
	backupBinlogDir             = "BinLog"
	backupData                  = "Data"

	// backupManifestFileName is the MANIFEST file name within a backup.
	backupManifestFileName = "MANIFEST"
	// RestoreState is the name of the sentinel file used to detect whether a previous restore
	// terminated abnormally
	RestoreState = "restore_in_progress"
	// BackupTimestampFormat is the format in which we save BackupTime and FinishedTime
	BackupTimestampFormat = "2006-01-02.150405"
)

const (
	// replicationStartDeadline is the deadline for starting replication
	replicationStartDeadline = 30
)

var (
	// ErrNoBackup is returned when there is no backup.
	ErrNoBackup = errors.New("no available backup")

	// ErrNoCompleteBackup is returned when there is at least one backup,
	// but none of them are complete.
	ErrNoCompleteBackup = errors.New("backup(s) found but none are complete")

	// backupStorageCompress can be set to false to not use gzip
	// on the backups.
	backupStorageCompress = true

	// backupCompressBlockSize is the splitting size for each
	// compressed block
	backupCompressBlockSize = 250000

	// backupCompressBlocks is the number of blocks that are processed
	// once before the writer blocks
	backupCompressBlocks = 2

	backupDuration  = stats.NewGauge("backup_duration_seconds", "How long it took to complete the last backup operation (in seconds)")
	restoreDuration = stats.NewGauge("restore_duration_seconds", "How long it took to complete the last restore operation (in seconds)")
)

func init() {
	for _, cmd := range []string{"vtcombo", "vttablet", "vttestserver", "vtbackup", "vtctld"} {
		servenv.OnParseFor(cmd, registerBackupFlags)
	}
}

func registerBackupFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&backupStorageCompress, "backup_storage_compress", backupStorageCompress, "if set, the backup files will be compressed.")
	fs.IntVar(&backupCompressBlockSize, "backup_storage_block_size", backupCompressBlockSize, "if backup_storage_compress is true, backup_storage_block_size sets the byte size for each block while compressing (default is 250000).")
	fs.IntVar(&backupCompressBlocks, "backup_storage_number_blocks", backupCompressBlocks, "if backup_storage_compress is true, backup_storage_number_blocks sets the number of blocks that can be processed, at once, before the writer blocks, during compression (default is 2). It should be equal to the number of CPUs available for compression.")
}

// Backup is the main entry point for a backup:
// - uses the BackupStorage service to store a new backup
// - shuts down Mysqld during the backup
// - remember if we were replicating, restore the exact same state
func Backup(ctx context.Context, params BackupParams) error {
	startTs := time.Now()
	backupDir := GetBackupDir(params.Keyspace, params.Shard)
	name := fmt.Sprintf("%v.%v", params.BackupTime.UTC().Format(BackupTimestampFormat), params.TabletAlias)
	// Start the backup with the BackupStorage.
	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return vterrors.Wrap(err, "unable to get backup storage")
	}
	defer bs.Close()
	bh, err := bs.StartBackup(ctx, backupDir, name)
	if err != nil {
		return vterrors.Wrap(err, "StartBackup failed")
	}

	be, err := GetBackupEngine()
	if err != nil {
		return vterrors.Wrap(err, "failed to find backup engine")
	}

	// Take the backup, and either AbortBackup or EndBackup.
	usable, err := be.ExecuteBackup(ctx, params, bh)
	logger := params.Logger
	var finishErr error
	if usable {
		finishErr = bh.EndBackup(ctx)
	} else {
		logger.Errorf2(err, "backup is not usable, aborting it")
		finishErr = bh.AbortBackup(ctx)
	}
	if err != nil {
		if finishErr != nil {
			// We have a backup error, and we also failed
			// to finish the backup: just log the backup
			// finish error, return the backup error.
			logger.Errorf2(finishErr, "failed to finish backup: %v")
		}
		return err
	}

	// The backup worked, so just return the finish error, if any.
	backupDuration.Set(int64(time.Since(startTs).Seconds()))
	return finishErr
}

// ParseBackupName parses the backup name for a given dir/name, according to
// the format generated by mysqlctl.Backup. An error is returned only if the
// backup name does not have the expected number of parts; errors parsing the
// timestamp and tablet alias are logged, and a nil value is returned for those
// fields in case of error.
func ParseBackupName(dir string, name string) (backupTime *time.Time, alias *topodatapb.TabletAlias, err error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cannot backup name %s, expected <date>.<time>.<tablet_alias>", name)
	}

	// parts[0]: date part of BackupTimestampFormat
	// parts[1]: time part of BackupTimestampFormat
	// parts[2]: tablet alias
	timestamp := strings.Join(parts[:2], ".")
	aliasStr := parts[2]

	btime, err := time.Parse(BackupTimestampFormat, timestamp)
	if err != nil {
		log.Errorf("error parsing backup time for %s/%s: %s", dir, name, err)
	} else {
		backupTime = &btime
	}

	alias, err = topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		log.Errorf("error parsing tablet alias for %s/%s: %s", dir, name, err)
		alias = nil
	}

	return backupTime, alias, nil
}

// checkNoDB makes sure there is no user data already there.
// Used by Restore, as we do not want to destroy an existing DB.
// The user's database name must be given since we ignore all others.
// Returns (true, nil) if the specified DB doesn't exist.
// Returns (false, nil) if the check succeeds but the condition is not
// satisfied (there is a DB).
// Returns (false, non-nil error) if one occurs while trying to perform the check.
func checkNoDB(ctx context.Context, mysqld MysqlDaemon, dbName string) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW DATABASES")
	if err != nil {
		return false, vterrors.Wrap(err, "checkNoDB failed")
	}

	for _, row := range qr.Rows {
		if row[0].ToString() == dbName {
			// found active db
			log.Warningf("checkNoDB failed, found active db %v", dbName)
			return false, nil
		}
	}
	return true, nil
}

// removeExistingFiles will delete existing files in the data dir to prevent
// conflicts with the restored archive. In particular, binlogs can be created
// even during initial bootstrap, and these can interfere with configuring
// replication if kept around after the restore.
func removeExistingFiles(cnf *Mycnf) error {
	paths := map[string]string{
		"BinLogPath.*":          cnf.BinLogPath,
		"DataDir":               cnf.DataDir,
		"InnodbDataHomeDir":     cnf.InnodbDataHomeDir,
		"InnodbLogGroupHomeDir": cnf.InnodbLogGroupHomeDir,
		"RelayLogPath.*":        cnf.RelayLogPath,
		"RelayLogIndexPath":     cnf.RelayLogIndexPath,
		"RelayLogInfoPath":      cnf.RelayLogInfoPath,
	}
	for name, path := range paths {
		if path == "" {
			return vterrors.Errorf(vtrpc.Code_UNKNOWN, "can't remove existing files: %v is unknown", name)
		}

		if strings.HasSuffix(name, ".*") {
			// These paths are actually filename prefixes, not directories.
			// An extension of the form ".###" is appended by mysqld.
			path += ".*"
			log.Infof("Restore: removing files in %v (%v)", name, path)
			matches, err := filepath.Glob(path)
			if err != nil {
				return vterrors.Wrapf(err, "can't expand path glob %q", path)
			}
			for _, match := range matches {
				if err := os.Remove(match); err != nil {
					return vterrors.Wrapf(err, "can't remove existing file from %v (%v)", name, match)
				}
			}
			continue
		}

		// Regular directory: delete recursively.
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Infof("Restore: skipping removal of nonexistent %v (%v)", name, path)
			continue
		}
		log.Infof("Restore: removing files in %v (%v)", name, path)
		if err := os.RemoveAll(path); err != nil {
			return vterrors.Wrapf(err, "can't remove existing files in %v (%v)", name, path)
		}
	}
	return nil
}

// ShouldRestore checks whether a database with tables already exists
// and returns whether a restore action should be performed
func ShouldRestore(ctx context.Context, params RestoreParams) (bool, error) {
	if params.DeleteBeforeRestore || RestoreWasInterrupted(params.Cnf) {
		return true, nil
	}
	params.Logger.Infof("Restore: No %v file found, checking no existing data is present", RestoreState)
	// Wait for mysqld to be ready, in case it was launched in parallel with us.
	// If this doesn't succeed, we should not attempt a restore
	if err := params.Mysqld.Wait(ctx, params.Cnf); err != nil {
		return false, err
	}
	return checkNoDB(ctx, params.Mysqld, params.DbName)
}

// Restore is the main entry point for backup restore.  If there is no
// appropriate backup on the BackupStorage, Restore logs an error
// and returns ErrNoBackup. Any other error is returned.
func Restore(ctx context.Context, params RestoreParams) (*BackupManifest, error) {
	startTs := time.Now()
	// find the right backup handle: most recent one, with a MANIFEST
	params.Logger.Infof("Restore: looking for a suitable backup to restore")
	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return nil, err
	}
	defer bs.Close()

	// Backups are stored in a directory structure that starts with
	// <keyspace>/<shard>
	backupDir := GetBackupDir(params.Keyspace, params.Shard)
	bhs, err := bs.ListBackups(ctx, backupDir)
	if err != nil {
		return nil, vterrors.Wrap(err, "ListBackups failed")
	}

	if len(bhs) == 0 {
		// There are no backups (not even broken/incomplete ones).
		params.Logger.Errorf("no backup to restore on BackupStorage for directory %v. Starting up empty.", backupDir)
		// Wait for mysqld to be ready, in case it was launched in parallel with us.
		if err = params.Mysqld.Wait(ctx, params.Cnf); err != nil {
			params.Logger.Errorf("mysqld is not running: %v", err)
			return nil, err
		}
		// Since this is an empty database make sure we start replication at the beginning
		if err := params.Mysqld.ResetReplication(ctx); err != nil {
			params.Logger.Errorf("error resetting replication: %v. Continuing", err)
		}

		// Always return ErrNoBackup
		return nil, ErrNoBackup
	}

	restorePath, err := FindBackupToRestore(ctx, params, bhs)
	if err != nil {
		return nil, err
	}
	if restorePath.IsEmpty() {
		// This condition should not happen; but we validate for sanity
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "empty restore path")
	}
	bh := restorePath.FullBackupHandle()
	re, err := GetRestoreEngine(ctx, bh)
	if err != nil {
		return nil, vterrors.Wrap(err, "Failed to find restore engine")
	}
	params.Logger.Infof("Restore: %v", restorePath.String())
	if params.DryRun {
		return nil, nil
	}
	manifest, err := re.ExecuteRestore(ctx, params, bh)
	if err != nil {
		return nil, err
	}

	// mysqld needs to be running in order for mysql_upgrade to work.
	// If we've just restored from a backup from previous MySQL version then mysqld
	// may fail to start due to a different structure of mysql.* tables. The flag
	// --skip-grant-tables ensures that these tables are not read until mysql_upgrade
	// is executed. And since with --skip-grant-tables anyone can connect to MySQL
	// without password, we are passing --skip-networking to greatly reduce the set
	// of those who can connect.
	params.Logger.Infof("Restore: starting mysqld for mysql_upgrade")
	// Note Start will use dba user for waiting, this is fine, it will be allowed.
	err = params.Mysqld.Start(context.Background(), params.Cnf, "--skip-grant-tables", "--skip-networking")
	if err != nil {
		return nil, err
	}

	// We disable super_read_only, in case it is in the default MySQL startup
	// parameters and will be blocking the writes we need to do in
	// PopulateMetadataTables().  We do it blindly, since
	// this will fail on MariaDB, which doesn't have super_read_only
	// This is safe, since we're restarting MySQL after the restore anyway
	params.Logger.Infof("Restore: disabling super_read_only")
	if err := params.Mysqld.SetSuperReadOnly(false); err != nil {
		if strings.Contains(err.Error(), strconv.Itoa(mysql.ERUnknownSystemVariable)) {
			params.Logger.Warningf("Restore: server does not know about super_read_only, continuing anyway...")
		} else {
			params.Logger.Errorf("Restore: unexpected error while trying to set super_read_only: %v", err)
			return nil, err
		}
	}

	params.Logger.Infof("Restore: running mysql_upgrade")
	if err := params.Mysqld.RunMysqlUpgrade(); err != nil {
		return nil, vterrors.Wrap(err, "mysql_upgrade failed")
	}

	// The MySQL manual recommends restarting mysqld after running mysql_upgrade,
	// so that any changes made to system tables take effect.
	params.Logger.Infof("Restore: restarting mysqld after mysql_upgrade")
	err = params.Mysqld.Shutdown(context.Background(), params.Cnf, true)
	if err != nil {
		return nil, err
	}
	err = params.Mysqld.Start(context.Background(), params.Cnf)
	if err != nil {
		return nil, err
	}

	if handles := restorePath.IncrementalBackupHandles(); len(handles) > 0 {
		params.Logger.Infof("Restore: applying %v incremental backups", len(handles))
		for _, bh := range handles {
			manifest, err := re.ExecuteRestore(ctx, params, bh)
			if err != nil {
				return nil, err
			}
			params.Logger.Infof("Restore: applied incremental backup: %v", manifest.Position)
		}
		params.Logger.Infof("Restore: done applying incremental backups")
	}

	params.Logger.Infof("Restore: removing state file")
	if err = removeStateFile(params.Cnf); err != nil {
		return nil, err
	}

	restoreDuration.Set(int64(time.Since(startTs).Seconds()))
	params.Logger.Infof("Restore: complete")
	return manifest, nil
}
