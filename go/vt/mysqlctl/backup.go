/*
Copyright 2017 Google Inc.

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
	"flag"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file handles the backup and restore related code

const (
	// the three bases for files to restore
	backupInnodbDataHomeDir     = "InnoDBData"
	backupInnodbLogGroupHomeDir = "InnoDBLog"
	backupData                  = "Data"

	// the manifest file name
	backupManifest = "MANIFEST"
)

const (
	// slaveStartDeadline is the deadline for starting a slave
	slaveStartDeadline = 30
)

var (
	// ErrNoBackup is returned when there is no backup.
	ErrNoBackup = errors.New("no available backup")

	// ErrExistingDB is returned when there's already an active DB.
	ErrExistingDB = errors.New("skipping restore due to existing database")

	// backupStorageHook contains the hook name to use to process
	// backup files. If not set, we will not process the files. It is
	// only used at backup time. Then it is put in the manifest,
	// and when decoding a backup, it is read from the manifest,
	// and used as the transform hook name again.
	backupStorageHook = flag.String("backup_storage_hook", "", "if set, we send the contents of the backup files through this hook.")

	// backupStorageCompress can be set to false to not use gzip
	// on the backups. Usually would be set if a hook is used, and
	// the hook compresses the data.
	backupStorageCompress = flag.Bool("backup_storage_compress", true, "if set, the backup files will be compressed (default is true). Set to false for instance if a backup_storage_hook is specified and it compresses the data.")

	// backupCompressBlockSize is the splitting size for each
	// compressed block
	backupCompressBlockSize = flag.Int("backup_storage_block_size", 250000, "if backup_storage_compress is true, backup_storage_block_size sets the byte size for each block while compressing (default is 250000).")

	// backupCompressBlocks is the number of blocks that are processed
	// once before the writer blocks
	backupCompressBlocks = flag.Int("backup_storage_number_blocks", 2, "if backup_storage_compress is true, backup_storage_number_blocks sets the number of blocks that can be processed, at once, before the writer blocks, during compression (default is 2). It should be equal to the number of CPUs available for compression")
)

// Backup is the main entry point for a backup:
// - uses the BackupStorage service to store a new backup
// - shuts down Mysqld during the backup
// - remember if we were replicating, restore the exact same state
func Backup(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, dir, name string, backupConcurrency int, hookExtraEnv map[string]string, backupTime time.Time) error {
	// Start the backup with the BackupStorage.
	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return vterrors.Wrap(err, "unable to get backup storage")
	}
	defer bs.Close()
	bh, err := bs.StartBackup(ctx, dir, name)
	if err != nil {
		return vterrors.Wrap(err, "StartBackup failed")
	}

	be, err := GetBackupEngine()
	if err != nil {
		return vterrors.Wrap(err, "failed to find backup engine")
	}

	// Take the backup, and either AbortBackup or EndBackup.
	usable, err := be.ExecuteBackup(ctx, cnf, mysqld, logger, bh, backupConcurrency, hookExtraEnv, backupTime)
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
	return finishErr
}

// checkNoDB makes sure there is no user data already there.
// Used by Restore, as we do not want to destroy an existing DB.
// The user's database name must be given since we ignore all others.
// Returns true if the specified DB either doesn't exist, or has no tables.
// Returns (false, nil) if the check succeeds but the condition is not
// satisfied (there is a DB with tables).
// Returns non-nil error if one occurs while trying to perform the check.
func checkNoDB(ctx context.Context, mysqld MysqlDaemon, dbName string) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW DATABASES")
	if err != nil {
		return false, vterrors.Wrap(err, "checkNoDB failed")
	}

	backtickDBName := sqlescape.EscapeID(dbName)
	for _, row := range qr.Rows {
		if row[0].ToString() == dbName {
			tableQr, err := mysqld.FetchSuperQuery(ctx, "SHOW TABLES FROM "+backtickDBName)
			if err != nil {
				return false, vterrors.Wrap(err, "checkNoDB failed")
			}
			if len(tableQr.Rows) == 0 {
				// no tables == empty db, all is well
				continue
			}
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

// Restore is the main entry point for backup restore.  If there is no
// appropriate backup on the BackupStorage, Restore logs an error
// and returns ErrNoBackup. Any other error is returned.
func Restore(
	ctx context.Context,
	cnf *Mycnf,
	mysqld MysqlDaemon,
	dir string,
	restoreConcurrency int,
	hookExtraEnv map[string]string,
	localMetadata map[string]string,
	logger logutil.Logger,
	deleteBeforeRestore bool,
	dbName string,
	snapshotTime time.Time) (mysql.Position, error) {

	rval := mysql.Position{}

	// Wait for mysqld to be ready, in case it was launched in parallel with us.
	if err := mysqld.Wait(ctx, cnf); err != nil {
		return mysql.Position{}, err
	}

	if !deleteBeforeRestore {
		logger.Infof("Restore: checking no existing data is present")
		ok, err := checkNoDB(ctx, mysqld, dbName)
		if err != nil {
			return mysql.Position{}, err
		}
		if !ok {
			logger.Infof("Auto-restore is enabled, but mysqld already contains data. Assuming vttablet was just restarted.")
			if err = PopulateMetadataTables(mysqld, localMetadata, dbName); err == nil {
				err = ErrExistingDB
			}
			return mysql.Position{}, err
		}
	}

	// find the right backup handle: most recent one, with a MANIFEST
	logger.Infof("Restore: looking for a suitable backup to restore")
	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return mysql.Position{}, err
	}
	defer bs.Close()

	bhs, err := bs.ListBackups(ctx, dir)
	if err != nil {
		return mysql.Position{}, vterrors.Wrap(err, "ListBackups failed")
	}

	if len(bhs) == 0 {
		// There are no backups (not even broken/incomplete ones).
		logger.Errorf("no backup to restore on BackupStorage for directory %v. Starting up empty.", dir)
		// Since this is an empty database make sure we start replication at the beginning
		if err = mysqld.ResetReplication(ctx); err == nil {
			logger.Errorf("error reseting slave replication: %v. Continuing", err)
			err = ErrNoBackup
		}

		if err2 := PopulateMetadataTables(mysqld, localMetadata, dbName); err2 == nil {
			err = ErrNoBackup
		}
		return mysql.Position{}, err
	}

	be, err := GetBackupEngine()
	if err != nil {
		return mysql.Position{}, vterrors.Wrap(err, "Failed to find backup engine")
	}
	if rval, err = be.ExecuteRestore(ctx, cnf, mysqld, logger, dir, bhs, restoreConcurrency, hookExtraEnv, snapshotTime); err != nil {
		return rval, err
	}

	// mysqld needs to be running in order for mysql_upgrade to work.
	// If we've just restored from a backup from previous MySQL version then mysqld
	// may fail to start due to a different structure of mysql.* tables. The flag
	// --skip-grant-tables ensures that these tables are not read until mysql_upgrade
	// is executed. And since with --skip-grant-tables anyone can connect to MySQL
	// without password, we are passing --skip-networking to greatly reduce the set
	// of those who can connect.
	logger.Infof("Restore: starting mysqld for mysql_upgrade")
	// Note Start will use dba user for waiting, this is fine, it will be allowed.
	err = mysqld.Start(context.Background(), cnf, "--skip-grant-tables", "--skip-networking")
	if err != nil {
		return mysql.Position{}, err
	}

	logger.Infof("Restore: running mysql_upgrade")
	if err := mysqld.RunMysqlUpgrade(); err != nil {
		return mysql.Position{}, vterrors.Wrap(err, "mysql_upgrade failed")
	}

	// Populate local_metadata before starting without --skip-networking,
	// so it's there before we start announcing ourselves.
	logger.Infof("Restore: populating local_metadata")
	err = PopulateMetadataTables(mysqld, localMetadata, dbName)
	if err != nil {
		return mysql.Position{}, err
	}

	// The MySQL manual recommends restarting mysqld after running mysql_upgrade,
	// so that any changes made to system tables take effect.
	logger.Infof("Restore: restarting mysqld after mysql_upgrade")
	err = mysqld.Shutdown(context.Background(), cnf, true)
	if err != nil {
		return mysql.Position{}, err
	}
	err = mysqld.Start(context.Background(), cnf)
	if err != nil {
		return mysql.Position{}, err
	}

	return rval, nil
}
