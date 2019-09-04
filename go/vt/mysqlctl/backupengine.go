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

package mysqlctl

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// BackupEngineImplementation is the implementation to use for BackupEngine
	backupEngineImplementation = flag.String("backup_engine_implementation", builtinBackupEngineName, "Specifies which implementation to use for creating new backups (builtin or xtrabackup). Restores will always be done with whichever engine created a given backup.")
)

// BackupEngine is the interface to take a backup with a given engine.
type BackupEngine interface {
	ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle, backupTime time.Time) (bool, error)
	ShouldDrainForBackup() bool
}

// BackupParams is the struct that holds all params passed to ExecuteBackup
type BackupParams struct {
	Cnf    *Mycnf
	Mysqld MysqlDaemon
	Logger logutil.Logger
	// Concurrency is the value of -concurrency flag given to Backup command
	// It determines how many files are processed in parallel
	Concurrency int
	// Extra env variables for pre-backup and post-backup transform hooks
	HookExtraEnv map[string]string
	// TopoServer, Keyspace and Shard are used to discover master tablet
	TopoServer *topo.Server
	Keyspace   string
	Shard      string
}

// RestoreParams is the struct that holds all params passed to ExecuteRestore
type RestoreParams struct {
	Cnf    *Mycnf
	Mysqld MysqlDaemon
	Logger logutil.Logger
	// Concurrency is the value of -restore_concurrency flag (init restore parameter)
	// It determines how many files are processed in parallel
	Concurrency int
	// Extra env variables for pre-restore and post-restore transform hooks
	HookExtraEnv map[string]string
	// Metadata to write into database after restore. See PopulateMetadataTables
	LocalMetadata map[string]string
	// DeleteBeforeRestore tells us whether existing data should be deleted before
	// restoring. This is always set to false when starting a tablet with -restore_from_backup,
	// but is set to true when executing a RestoreFromBackup command on an already running vttablet
	DeleteBeforeRestore bool
	// Name of the managed database / schema
	DbName string
	// Directory location to search for a usable backup
	Dir string
}

// RestoreEngine is the interface to restore a backup with a given engine.
type RestoreEngine interface {
	ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (mysql.Position, error)
}

// BackupRestoreEngine is a combination of BackupEngine and RestoreEngine.
type BackupRestoreEngine interface {
	BackupEngine
	RestoreEngine
}

// BackupRestoreEngineMap contains the registered implementations for
// BackupEngine and RestoreEngine.
var BackupRestoreEngineMap = make(map[string]BackupRestoreEngine)

// GetBackupEngine returns the BackupEngine implementation that should be used
// to create new backups.
//
// To restore a backup, you should instead get the appropriate RestoreEngine for
// a particular backup by calling GetRestoreEngine().
//
// This must only be called after flags have been parsed.
func GetBackupEngine() (BackupEngine, error) {
	name := *backupEngineImplementation
	be, ok := BackupRestoreEngineMap[name]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "unknown BackupEngine implementation %q", name)
	}
	return be, nil
}

// GetRestoreEngine returns the RestoreEngine implementation to restore a given backup.
// It reads the MANIFEST file from the backup to check which engine was used to create it.
func GetRestoreEngine(ctx context.Context, backup backupstorage.BackupHandle) (RestoreEngine, error) {
	manifest, err := GetBackupManifest(ctx, backup)
	if err != nil {
		return nil, vterrors.Wrap(err, "can't get backup MANIFEST")
	}
	engine := manifest.BackupMethod
	if engine == "" {
		// The builtin engine is the only one that ever left BackupMethod unset.
		engine = builtinBackupEngineName
	}
	re, ok := BackupRestoreEngineMap[engine]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "can't restore backup created with %q engine; no such BackupEngine implementation is registered", manifest.BackupMethod)
	}
	return re, nil
}

// GetBackupManifest returns the common fields of the MANIFEST file for a given backup.
func GetBackupManifest(ctx context.Context, backup backupstorage.BackupHandle) (*BackupManifest, error) {
	manifest := &BackupManifest{}
	if err := getBackupManifestInto(ctx, backup, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

// getBackupManifestInto fetches and decodes a MANIFEST file into the specified object.
func getBackupManifestInto(ctx context.Context, backup backupstorage.BackupHandle, outManifest interface{}) error {
	file, err := backup.ReadFile(ctx, backupManifestFileName)
	if err != nil {
		return vterrors.Wrap(err, "can't read MANIFEST")
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(outManifest); err != nil {
		return vterrors.Wrap(err, "can't decode MANIFEST")
	}
	return nil
}

// BackupManifest defines the common fields in the MANIFEST file.
// All backup engines must include at least these fields. They are free to add
// their own custom fields by embedding this struct anonymously into their own
// custom struct, as long as their custom fields don't have conflicting names.
type BackupManifest struct {
	// BackupMethod is the name of the backup engine that created this backup.
	// If this is empty, the backup engine is assumed to be "builtin" since that
	// was the only engine that ever left this field empty. All new backup
	// engines are required to set this field to the backup engine name.
	BackupMethod string

	// Position is the replication position at which the backup was taken.
	Position mysql.Position

	// BackupTime is when the backup was taken in UTC time
	// format: "2006-01-02.150405"
	BackupTime time.Time
	// FinishedTime is the time (in RFC 3339 format, UTC) at which the backup finished, if known.
	// Some backups may not set this field if they were created before the field was added.
	FinishedTime string
}

// FindBackupToRestore returns a selected candidate backup to be restored.
// It returns the most recent backup that is complete, meaning it has a valid
// MANIFEST file.
func FindBackupToRestore(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, dir string, bhs []backupstorage.BackupHandle, snapshotTime time.Time) (backupstorage.BackupHandle, error) {
	var bh backupstorage.BackupHandle
	var index int
	unixZeroTime := time.Unix(0, 0).UTC()

	for index = len(bhs) - 1; index >= 0; index-- {
		bh = bhs[index]
		// Check that the backup MANIFEST exists and can be successfully decoded.
		bm, err := GetBackupManifest(ctx, bh)
		if err != nil {
			log.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage: can't read MANIFEST: %v)", bh.Name(), dir, err)
			continue
		}

		if snapshotTime.Equal(unixZeroTime) /* uninitialized or not snapshot */ || bm.BackupTime.Before(snapshotTime) {
			logger.Infof("Restore: found backup %v %v to restore", bh.Directory(), bh.Name())
			break
		}
	}
	if index < 0 {
		if snapshotTime.After(unixZeroTime) {
			log.Errorf("No valid backup found before time %v", snapshotTime.Format("2006-01-02.150405"))
		}
		// There is at least one attempted backup, but none could be read.
		// This implies there is data we ought to have, so it's not safe to start
		// up empty.
		return nil, ErrNoCompleteBackup
	}

	return bh, nil
}

func prepareToRestore(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger) error {
	// shutdown mysqld if it is running
	logger.Infof("Restore: shutdown mysqld")
	if err := mysqld.Shutdown(ctx, cnf, true); err != nil {
		return err
	}

	logger.Infof("Restore: deleting existing files")
	if err := removeExistingFiles(cnf); err != nil {
		return err
	}

	logger.Infof("Restore: reinit config file")
	if err := mysqld.ReinitConfig(ctx, cnf); err != nil {
		return err
	}
	return nil
}

// create restore state file
func createStateFile(cnf *Mycnf) error {
	// if we start writing content to this file:
	// change RD_ONLY to RDWR
	// change Create to Open
	// rename func to openStateFile
	// change to return a *File
	fname := filepath.Join(cnf.TabletDir(), RestoreState)
	fd, err := os.Create(fname)
	if err != nil {
		return fmt.Errorf("unable to create file: %v", err)
	}
	if err = fd.Close(); err != nil {
		return fmt.Errorf("unable to close file: %v", err)
	}
	return nil
}

// delete restore state file
func removeStateFile(cnf *Mycnf) error {
	fname := filepath.Join(cnf.TabletDir(), RestoreState)
	if err := os.Remove(fname); err != nil {
		return fmt.Errorf("unable to delete file: %v", err)
	}
	return nil
}

// RestoreWasInterrupted tells us whether a previous restore
// was interrupted and we are now retrying it
func RestoreWasInterrupted(cnf *Mycnf) bool {
	name := filepath.Join(cnf.TabletDir(), RestoreState)
	_, err := os.Stat(name)
	return err == nil
}
