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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
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
	ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (bool, error)
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
	// Keyspace and Shard are used to infer the directory where backups should be stored
	Keyspace string
	Shard    string
	// TabletAlias is used along with backupTime to construct the backup name
	TabletAlias string
	// BackupTime is the time at which the backup is being started
	BackupTime time.Time
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
	// DbName is the name of the managed database / schema
	DbName string
	// Keyspace and Shard are used to infer the directory where backups are stored
	Keyspace string
	Shard    string
	// StartTime: if non-zero, look for a backup that was taken at or before this time
	// Otherwise, find the most recent backup
	StartTime time.Time
}

// RestoreEngine is the interface to restore a backup with a given engine.
// Returns the manifest of a backup if successful, otherwise returns an error
type RestoreEngine interface {
	ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (*BackupManifest, error)
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

	// BackupTime is when the backup was taken in UTC time (RFC 3339 format)
	BackupTime string

	// FinishedTime is the time (in RFC 3339 format, UTC) at which the backup finished, if known.
	// Some backups may not set this field if they were created before the field was added.
	FinishedTime string
}

// FindBackupToRestore returns a selected candidate backup to be restored.
// It returns the most recent backup that is complete, meaning it has a valid
// MANIFEST file.
func FindBackupToRestore(ctx context.Context, params RestoreParams, bhs []backupstorage.BackupHandle) (backupstorage.BackupHandle, error) {
	var bh backupstorage.BackupHandle
	var index int
	// if a StartTime is provided in params, then find a backup that was taken at or before that time
	checkBackupTime := !params.StartTime.IsZero()
	backupDir := GetBackupDir(params.Keyspace, params.Shard)

	for index = len(bhs) - 1; index >= 0; index-- {
		bh = bhs[index]
		// Check that the backup MANIFEST exists and can be successfully decoded.
		bm, err := GetBackupManifest(ctx, bh)
		if err != nil {
			params.Logger.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage: can't read MANIFEST: %v)", bh.Name(), backupDir, err)
			continue
		}

		var backupTime time.Time
		if checkBackupTime {
			backupTime, err = time.Parse(time.RFC3339, bm.BackupTime)
			if err != nil {
				params.Logger.Warningf("Restore: skipping backup %v/%v with invalid time %v: %v", backupDir, bh.Name(), bm.BackupTime, err)
				continue
			}
		}
		if !checkBackupTime /* not snapshot */ || backupTime.Equal(params.StartTime) || backupTime.Before(params.StartTime) {
			params.Logger.Infof("Restore: found backup %v %v to restore", bh.Directory(), bh.Name())
			break
		}
	}
	if index < 0 {
		if checkBackupTime {
			params.Logger.Errorf("No valid backup found before time %v", params.StartTime.Format(BackupTimestampFormat))
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

// GetBackupDir returns the directory where backups for the
// given keyspace/shard are (or will be) stored
func GetBackupDir(keyspace, shard string) string {
	return fmt.Sprintf("%v/%v", keyspace, shard)
}

// isDbDir returns true if the given directory contains a DB
func isDbDir(p string) bool {
	// db.opt is there
	if _, err := os.Stat(path.Join(p, "db.opt")); err == nil {
		return true
	}

	// Look for at least one database file
	fis, err := ioutil.ReadDir(p)
	if err != nil {
		return false
	}
	for _, fi := range fis {
		if strings.HasSuffix(fi.Name(), ".frm") {
			return true
		}

		// the MyRocks engine stores data in RocksDB .sst files
		// https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format
		if strings.HasSuffix(fi.Name(), ".sst") {
			return true
		}

		// .frm files were removed in MySQL 8, so we need to check for two other file types
		// https://dev.mysql.com/doc/refman/8.0/en/data-dictionary-file-removal.html
		if strings.HasSuffix(fi.Name(), ".ibd") {
			return true
		}
		// https://dev.mysql.com/doc/refman/8.0/en/serialized-dictionary-information.html
		if strings.HasSuffix(fi.Name(), ".sdi") {
			return true
		}
	}

	return false
}

func addDirectory(fes []FileEntry, base string, baseDir string, subDir string) ([]FileEntry, int64, error) {
	p := path.Join(baseDir, subDir)
	var size int64

	fis, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, 0, err
	}
	for _, fi := range fis {
		fes = append(fes, FileEntry{
			Base: base,
			Name: path.Join(subDir, fi.Name()),
		})
		size = size + fi.Size()
	}
	return fes, size, nil
}

// addMySQL8DataDictionary checks to see if the new data dictionary introduced in MySQL 8 exists
// and adds it to the backup manifest if it does
// https://dev.mysql.com/doc/refman/8.0/en/data-dictionary-transactional-storage.html
func addMySQL8DataDictionary(fes []FileEntry, base string, baseDir string) ([]FileEntry, int64, error) {
	filePath := path.Join(baseDir, dataDictionaryFile)

	// no-op if this file doesn't exist
	fi, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return fes, 0, nil
	}

	fes = append(fes, FileEntry{
		Base: base,
		Name: dataDictionaryFile,
	})

	return fes, fi.Size(), nil
}

func findFilesToBackup(cnf *Mycnf) ([]FileEntry, int64, error) {
	var err error
	var result []FileEntry
	var totalSize int64

	// first add inno db files
	result, totalSize, err = addDirectory(result, backupInnodbDataHomeDir, cnf.InnodbDataHomeDir, "")
	if err != nil {
		return nil, 0, err
	}
	result, size, err := addDirectory(result, backupInnodbLogGroupHomeDir, cnf.InnodbLogGroupHomeDir, "")
	if err != nil {
		return nil, 0, err
	}
	totalSize = totalSize + size
	// then add the transactional data dictionary if it exists
	result, size, err = addMySQL8DataDictionary(result, backupData, cnf.DataDir)
	if err != nil {
		return nil, 0, err
	}
	totalSize = totalSize + size

	// then add DB directories
	fis, err := ioutil.ReadDir(cnf.DataDir)
	if err != nil {
		return nil, 0, err
	}

	for _, fi := range fis {
		p := path.Join(cnf.DataDir, fi.Name())
		if isDbDir(p) {
			result, size, err = addDirectory(result, backupData, cnf.DataDir, fi.Name())
			if err != nil {
				return nil, 0, err
			}
			totalSize = totalSize + size
		}
	}
	return result, totalSize, nil
}
