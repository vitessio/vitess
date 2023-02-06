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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// BackupEngineImplementation is the implementation to use for BackupEngine
	backupEngineImplementation = builtinBackupEngineName
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
	// Extra env variables used while stopping and starting mysqld
	HookExtraEnv map[string]string
	// TopoServer, Keyspace and Shard are used to discover primary tablet
	TopoServer *topo.Server
	// Keyspace and Shard are used to infer the directory where backups should be stored
	Keyspace string
	Shard    string
	// TabletAlias is used along with backupTime to construct the backup name
	TabletAlias string
	// BackupTime is the time at which the backup is being started
	BackupTime time.Time
	// Position of last known backup. If non empty, then this value indicates the backup should be incremental
	// and as of this position
	IncrementalFromPos string
}

// RestoreParams is the struct that holds all params passed to ExecuteRestore
type RestoreParams struct {
	Cnf    *Mycnf
	Mysqld MysqlDaemon
	Logger logutil.Logger
	// Concurrency is the value of --restore_concurrency flag (init restore parameter)
	// It determines how many files are processed in parallel
	Concurrency int
	// Extra env variables for pre-restore and post-restore transform hooks
	HookExtraEnv map[string]string
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
	// RestoreToPos hints that a point in time recovery is requested, to recover up to the specific given pos.
	// When empty, the restore is a normal from full backup
	RestoreToPos mysql.Position
	// When DryRun is set, no restore actually takes place; but some of its steps are validated.
	DryRun bool
}

func (p *RestoreParams) IsIncrementalRecovery() bool {
	return !p.RestoreToPos.IsZero()
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

func init() {
	for _, cmd := range []string{"vtcombo", "vttablet", "vttestserver", "vtctld", "vtbackup"} {
		servenv.OnParseFor(cmd, registerBackupEngineFlags)
	}
}

func registerBackupEngineFlags(fs *pflag.FlagSet) {
	fs.StringVar(&backupEngineImplementation, "backup_engine_implementation", backupEngineImplementation, "Specifies which implementation to use for creating new backups (builtin or xtrabackup). Restores will always be done with whichever engine created a given backup.")
}

// GetBackupEngine returns the BackupEngine implementation that should be used
// to create new backups.
//
// To restore a backup, you should instead get the appropriate RestoreEngine for
// a particular backup by calling GetRestoreEngine().
//
// This must only be called after flags have been parsed.
func GetBackupEngine() (BackupEngine, error) {
	name := backupEngineImplementation
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
func getBackupManifestInto(ctx context.Context, backup backupstorage.BackupHandle, outManifest any) error {
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

	// PurgedPosition stands for purged GTIDs, information that is necessary for PITR recovery. This is specific to MySQL56
	PurgedPosition mysql.Position

	// FromPosition is only applicable to incremental backups, and stands for the position from
	// which incremental changes are backed up.
	FromPosition mysql.Position

	// Incremental indicates whether this is an incremental backup
	Incremental bool

	// BackupTime is when the backup was taken in UTC time (RFC 3339 format)
	BackupTime string

	// FinishedTime is the time (in RFC 3339 format, UTC) at which the backup finished, if known.
	// Some backups may not set this field if they were created before the field was added.
	FinishedTime string

	// ServerUUID identifies the server from which backup was taken
	ServerUUID string

	TabletAlias string

	Keyspace string

	Shard string
}

func (m *BackupManifest) HashKey() string {
	return fmt.Sprintf("%v/%v/%v/%t/%v", m.BackupMethod, m.Position, m.FromPosition, m.Incremental, m.BackupTime)
}

// ManifestHandleMap is a utility container to map manifests to handles, making it possible to search for, and iterate, handles based on manifests.
type ManifestHandleMap struct {
	mp map[string]backupstorage.BackupHandle
}

func NewManifestHandleMap() *ManifestHandleMap {
	return &ManifestHandleMap{
		mp: map[string]backupstorage.BackupHandle{},
	}
}

// Map assigns a handle to a manifest
func (m *ManifestHandleMap) Map(manifest *BackupManifest, handle backupstorage.BackupHandle) {
	if manifest == nil {
		return
	}
	m.mp[manifest.HashKey()] = handle
}

// Handle returns the backup handles assigned to given manifest
func (m *ManifestHandleMap) Handle(manifest *BackupManifest) (handle backupstorage.BackupHandle) {
	return m.mp[manifest.HashKey()]
}

// Handles returns an ordered list of handles, by given list of manifests
func (m *ManifestHandleMap) Handles(manifests []*BackupManifest) (handles []backupstorage.BackupHandle) {
	handles = make([]backupstorage.BackupHandle, 0, len(manifests))
	for _, manifest := range manifests {
		handles = append(handles, m.mp[manifest.HashKey()])
	}
	return handles
}

// RestorePath is an ordered sequence of backup handles & manifests, that can be used to restore from backup.
// The path could be empty, in which case it's invalid, there's no way to restore. Otherwise, the path
// consists of exactly one full backup, followed by zero or more incremental backups.
type RestorePath struct {
	manifests         []*BackupManifest
	manifestHandleMap *ManifestHandleMap
}

func (p *RestorePath) IsEmpty() bool {
	return len(p.manifests) == 0
}

func (p *RestorePath) Len() int {
	return len(p.manifests)
}

func (p *RestorePath) Add(m *BackupManifest) {
	p.manifests = append(p.manifests, m)
}

// FullBackupHandle returns the single (if any) full backup handle, which is always the first handle in the sequence
func (p *RestorePath) FullBackupHandle() backupstorage.BackupHandle {
	if p.IsEmpty() {
		return nil
	}
	return p.manifestHandleMap.Handle(p.manifests[0])
}

// IncrementalBackupHandles returns an ordered list of backup handles comprising of the incremental (non-full) path
func (p *RestorePath) IncrementalBackupHandles() []backupstorage.BackupHandle {
	if p.IsEmpty() {
		return nil
	}
	return p.manifestHandleMap.Handles(p.manifests[1:])
}

func (p *RestorePath) String() string {
	var sb strings.Builder
	sb.WriteString("RestorePath: [")
	for i, m := range p.manifests {
		if i > 0 {
			sb.WriteString(", ")
		}
		if m.Incremental {
			sb.WriteString("incremental:")
		} else {
			sb.WriteString("full:")
		}
		sb.WriteString(p.manifestHandleMap.Handle(m).Name())
	}
	sb.WriteString("]")
	return sb.String()
}

// FindLatestSuccessfulBackup returns the handle and manifest for the last good backup,
// which can be either full or increment
func FindLatestSuccessfulBackup(ctx context.Context, logger logutil.Logger, bhs []backupstorage.BackupHandle) (backupstorage.BackupHandle, *BackupManifest, error) {
	for index := len(bhs) - 1; index >= 0; index-- {
		bh := bhs[index]
		// Check that the backup MANIFEST exists and can be successfully decoded.
		bm, err := GetBackupManifest(ctx, bh)
		if err != nil {
			logger.Warningf("Possibly incomplete backup %v on BackupStorage: can't read MANIFEST: %v)", bh.Name(), err)
			continue
		}
		return bh, bm, nil
	}
	return nil, nil, ErrNoCompleteBackup
}

// FindBackupToRestore returns a path, a sequence of backup handles, to be restored.
// The returned handles stand for valid backups with complete manifests.
func FindBackupToRestore(ctx context.Context, params RestoreParams, bhs []backupstorage.BackupHandle) (*RestorePath, error) {
	// if a StartTime is provided in params, then find a backup that was taken at or before that time
	checkBackupTime := !params.StartTime.IsZero()
	backupDir := GetBackupDir(params.Keyspace, params.Shard)

	manifests := make([]*BackupManifest, len(bhs))
	manifestHandleMap := NewManifestHandleMap()

	fullBackupIndex := func() int {
		for index := len(bhs) - 1; index >= 0; index-- {
			bh := bhs[index]
			// Check that the backup MANIFEST exists and can be successfully decoded.
			bm, err := GetBackupManifest(ctx, bh)
			if err != nil {
				params.Logger.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage: can't read MANIFEST: %v)", bh.Name(), backupDir, err)
				continue
			}
			// the manifest is valid
			manifests[index] = bm // manifests's order is insignificant, it will be sorted later on
			manifestHandleMap.Map(bm, bh)
			if bm.Incremental {
				// We're looking for a full backup
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

			switch {
			case checkBackupTime:
				// restore to specific time
				if backupTime.Equal(params.StartTime) || backupTime.Before(params.StartTime) {
					params.Logger.Infof("Restore: found backup %v %v to restore using the specified timestamp of '%v'", bh.Directory(), bh.Name(), params.StartTime.Format(BackupTimestampFormat))
					return index
				}
			case !params.RestoreToPos.IsZero():
				// restore to specific pos
				if params.RestoreToPos.GTIDSet.Contains(bm.Position.GTIDSet) {
					// this is the most recent backup which is <= desired position
					return index
				}
			default:
				// restore latest full backup
				params.Logger.Infof("Restore: found latest backup %v %v to restore", bh.Directory(), bh.Name())
				return index
			}
		}
		return -1
	}()
	if fullBackupIndex < 0 {
		if checkBackupTime {
			params.Logger.Errorf("No valid backup found before time %v", params.StartTime.Format(BackupTimestampFormat))
		}
		// There is at least one attempted backup, but none could be read.
		// This implies there is data we ought to have, so it's not safe to start
		// up empty.
		return nil, ErrNoCompleteBackup
	}
	// Anything taken before the full backup that we picked, is not of interest:
	manifests = manifests[fullBackupIndex:]
	restorePath := &RestorePath{
		manifestHandleMap: manifestHandleMap,
	}
	if params.RestoreToPos.IsZero() {
		// restoring from a single full backup:
		restorePath.Add(manifests[0])
		return restorePath, nil
	}
	// restore to a position (using incremental backups):
	// we calculate a possible restore path based on the manifests. The resulting manifests are
	// a sorted subsequence, with the full backup first, and zero or more incremental backups to follow.
	manifests, err := FindPITRPath(params.RestoreToPos.GTIDSet, manifests)
	if err != nil {
		return nil, err
	}
	restorePath.manifests = manifests
	return restorePath, nil
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
	fis, err := os.ReadDir(p)
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

	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, 0, err
	}
	for _, entry := range entries {
		fi, err := entry.Info()
		if err != nil {
			return nil, 0, err
		}

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

func hasDynamicRedoLog(cnf *Mycnf) bool {
	dynamicRedoLogPath := path.Join(cnf.InnodbLogGroupHomeDir, mysql.DynamicRedoLogSubdir)
	info, err := os.Stat(dynamicRedoLogPath)
	return !os.IsNotExist(err) && info.IsDir()
}

func findFilesToBackup(cnf *Mycnf) ([]FileEntry, int64, error) {
	var err error
	var result []FileEntry
	var size, totalSize int64

	// first add innodb files
	result, totalSize, err = addDirectory(result, backupInnodbDataHomeDir, cnf.InnodbDataHomeDir, "")
	if err != nil {
		return nil, 0, err
	}

	if hasDynamicRedoLog(cnf) {
		result, size, err = addDirectory(result, backupInnodbLogGroupHomeDir, cnf.InnodbLogGroupHomeDir, mysql.DynamicRedoLogSubdir)
	} else {
		result, size, err = addDirectory(result, backupInnodbLogGroupHomeDir, cnf.InnodbLogGroupHomeDir, "")
	}
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
	fis, err := os.ReadDir(cnf.DataDir)
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

// binlogFilesToBackup returns the file entries for given binlog files (identified by file name, no path)
func binlogFilesToBackup(cnf *Mycnf, binlogFiles []string) (result []FileEntry, totalSize int64, err error) {
	binlogsDirectory := filepath.Dir(cnf.BinLogPath)
	entries, err := os.ReadDir(binlogsDirectory)
	if err != nil {
		return nil, 0, err
	}
	binlogFilesMap := map[string]bool{}
	for _, b := range binlogFiles {
		binlogFilesMap[b] = true
	}
	for _, entry := range entries {
		if !binlogFilesMap[entry.Name()] {
			// not a file we're looking for
			continue
		}
		fi, err := entry.Info()
		if err != nil {
			return nil, 0, err
		}

		result = append(result, FileEntry{
			Base: backupBinlogDir,
			Name: fi.Name(),
		})
		totalSize = totalSize + fi.Size()
	}
	return result, totalSize, nil
}
