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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/klauspost/pgzip"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

// BuiltinBackupEngine encapsulates the logic of the builtin engine
// it implements the BackupEngine interface and contains all the logic
// required to implement a backup/restore by copying files from and to
// the correct location / storage bucket
type BuiltinBackupEngine struct {
}

// BackupManifest represents the backup. It lists all the files, the
// Position that the backup was taken at, and the transform hook used,
// if any.
type BackupManifest struct {
	// FileEntries contains all the files in the backup
	FileEntries []FileEntry

	// Position is the position at which the backup was taken
	Position mysql.Position

	// TransformHook that was used on the files, if any.
	TransformHook string

	// SkipCompress can be set if the backup files were not run
	// through gzip. It is the negative of the flag, so old
	// backups that don't have this flag are assumed to be
	// compressed.
	SkipCompress bool
}

// FileEntry is one file to backup
type FileEntry struct {
	// Base is one of:
	// - backupInnodbDataHomeDir for files that go into Mycnf.InnodbDataHomeDir
	// - backupInnodbLogGroupHomeDir for files that go into Mycnf.InnodbLogGroupHomeDir
	// - backupData for files that go into Mycnf.DataDir
	Base string

	// Name is the file name, relative to Base
	Name string

	// Hash is the hash of the final data (transformed and
	// compressed if specified) stored in the BackupStorage.
	Hash string
}

func (fe *FileEntry) open(cnf *Mycnf, readOnly bool) (*os.File, error) {
	// find the root to use
	var root string
	switch fe.Base {
	case backupInnodbDataHomeDir:
		root = cnf.InnodbDataHomeDir
	case backupInnodbLogGroupHomeDir:
		root = cnf.InnodbLogGroupHomeDir
	case backupData:
		root = cnf.DataDir
	default:
		return nil, fmt.Errorf("unknown base: %v", fe.Base)
	}

	// and open the file
	name := path.Join(root, fe.Name)
	var fd *os.File
	var err error
	if readOnly {
		if fd, err = os.Open(name); err != nil {
			return nil, fmt.Errorf("cannot open source file %v: %v", name, err)
		}
	} else {
		dir := path.Dir(name)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create destination directory %v: %v", dir, err)
		}
		if fd, err = os.Create(name); err != nil {
			return nil, fmt.Errorf("cannot create destination file %v: %v", name, err)
		}
	}
	return fd, nil
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

func addDirectory(fes []FileEntry, base string, baseDir string, subDir string) ([]FileEntry, error) {
	p := path.Join(baseDir, subDir)

	fis, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, err
	}
	for _, fi := range fis {
		fes = append(fes, FileEntry{
			Base: base,
			Name: path.Join(subDir, fi.Name()),
		})
	}
	return fes, nil
}

// addMySQL8DataDictionary checks to see if the new data dictionary introduced in MySQL 8 exists
// and adds it to the backup manifest if it does
// https://dev.mysql.com/doc/refman/8.0/en/data-dictionary-transactional-storage.html
func addMySQL8DataDictionary(fes []FileEntry, base string, baseDir string) ([]FileEntry, error) {
	const dataDictionaryFile = "mysql.ibd"
	filePath := path.Join(baseDir, dataDictionaryFile)

	// no-op if this file doesn't exist
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fes, nil
	}

	fes = append(fes, FileEntry{
		Base: base,
		Name: dataDictionaryFile,
	})

	return fes, nil
}

func findFilesToBackup(cnf *Mycnf) ([]FileEntry, error) {
	var err error
	var result []FileEntry

	// first add inno db files
	result, err = addDirectory(result, backupInnodbDataHomeDir, cnf.InnodbDataHomeDir, "")
	if err != nil {
		return nil, err
	}
	result, err = addDirectory(result, backupInnodbLogGroupHomeDir, cnf.InnodbLogGroupHomeDir, "")
	if err != nil {
		return nil, err
	}

	// then add the transactional data dictionary if it exists
	result, err = addMySQL8DataDictionary(result, backupData, cnf.DataDir)
	if err != nil {
		return nil, err
	}

	// then add DB directories
	fis, err := ioutil.ReadDir(cnf.DataDir)
	if err != nil {
		return nil, err
	}

	for _, fi := range fis {
		p := path.Join(cnf.DataDir, fi.Name())
		if isDbDir(p) {
			result, err = addDirectory(result, backupData, cnf.DataDir, fi.Name())
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// ExecuteBackup returns a boolean that indicates if the backup is usable,
// and an overall error.
func (be *BuiltinBackupEngine) ExecuteBackup(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, backupConcurrency int, hookExtraEnv map[string]string) (bool, error) {

	logger.Infof("Hook: %v, Compress: %v", *backupStorageHook, *backupStorageCompress)

	// Save initial state so we can restore.
	slaveStartRequired := false
	sourceIsMaster := false
	readOnly := true
	var replicationPosition mysql.Position
	semiSyncMaster, semiSyncSlave := mysqld.SemiSyncEnabled()

	// See if we need to restart replication after backup.
	logger.Infof("getting current replication status")
	slaveStatus, err := mysqld.SlaveStatus()
	switch err {
	case nil:
		slaveStartRequired = slaveStatus.SlaveRunning()
	case mysql.ErrNotSlave:
		// keep going if we're the master, might be a degenerate case
		sourceIsMaster = true
	default:
		return false, fmt.Errorf("can't get slave status: %v", err)
	}

	// get the read-only flag
	readOnly, err = mysqld.IsReadOnly()
	if err != nil {
		return false, fmt.Errorf("can't get read-only status: %v", err)
	}

	// get the replication position
	if sourceIsMaster {
		if !readOnly {
			logger.Infof("turning master read-only before backup")
			if err = mysqld.SetReadOnly(true); err != nil {
				return false, fmt.Errorf("can't set read-only status: %v", err)
			}
		}
		replicationPosition, err = mysqld.MasterPosition()
		if err != nil {
			return false, fmt.Errorf("can't get master position: %v", err)
		}
	} else {
		if err = mysqld.StopSlave(hookExtraEnv); err != nil {
			return false, fmt.Errorf("can't stop slave: %v", err)
		}
		var slaveStatus mysql.SlaveStatus
		slaveStatus, err = mysqld.SlaveStatus()
		if err != nil {
			return false, fmt.Errorf("can't get slave status: %v", err)
		}
		replicationPosition = slaveStatus.Position
	}
	logger.Infof("using replication position: %v", replicationPosition)

	// shutdown mysqld
	err = mysqld.Shutdown(ctx, cnf, true)
	if err != nil {
		return false, fmt.Errorf("can't shutdown mysqld: %v", err)
	}

	// Backup everything, capture the error.
	backupErr := be.backupFiles(ctx, cnf, mysqld, logger, bh, replicationPosition, backupConcurrency, hookExtraEnv)
	usable := backupErr == nil

	// Try to restart mysqld
	err = mysqld.Start(ctx, cnf)
	if err != nil {
		return usable, fmt.Errorf("can't restart mysqld: %v", err)
	}

	// Restore original mysqld state that we saved above.
	if semiSyncMaster || semiSyncSlave {
		// Only do this if one of them was on, since both being off could mean
		// the plugin isn't even loaded, and the server variables don't exist.
		logger.Infof("restoring semi-sync settings from before backup: master=%v, slave=%v",
			semiSyncMaster, semiSyncSlave)
		err := mysqld.SetSemiSyncEnabled(semiSyncMaster, semiSyncSlave)
		if err != nil {
			return usable, err
		}
	}
	if slaveStartRequired {
		logger.Infof("restarting mysql replication")
		if err := mysqld.StartSlave(hookExtraEnv); err != nil {
			return usable, fmt.Errorf("cannot restart slave: %v", err)
		}

		// this should be quick, but we might as well just wait
		if err := WaitForSlaveStart(mysqld, slaveStartDeadline); err != nil {
			return usable, fmt.Errorf("slave is not restarting: %v", err)
		}
	}

	// And set read-only mode
	logger.Infof("resetting mysqld read-only to %v", readOnly)
	if err := mysqld.SetReadOnly(readOnly); err != nil {
		return usable, err
	}

	return usable, backupErr
}

// backupFiles finds the list of files to backup, and creates the backup.
func (be *BuiltinBackupEngine) backupFiles(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, replicationPosition mysql.Position, backupConcurrency int, hookExtraEnv map[string]string) (err error) {
	// Get the files to backup.
	fes, err := findFilesToBackup(cnf)
	if err != nil {
		return fmt.Errorf("can't find files to backup: %v", err)
	}
	logger.Infof("found %v files to backup", len(fes))

	// Backup with the provided concurrency.
	sema := sync2.NewSemaphore(backupConcurrency, 0)
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for i := range fes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Wait until we are ready to go, skip if we already
			// encountered an error.
			sema.Acquire()
			defer sema.Release()
			if rec.HasErrors() {
				return
			}

			// Backup the individual file.
			name := fmt.Sprintf("%v", i)
			rec.RecordError(be.backupFile(ctx, cnf, mysqld, logger, bh, &fes[i], name, hookExtraEnv))
		}(i)
	}

	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// open the MANIFEST
	wc, err := bh.AddFile(ctx, backupManifest, 0)
	if err != nil {
		return fmt.Errorf("cannot add %v to backup: %v", backupManifest, err)
	}
	defer func() {
		if closeErr := wc.Close(); err == nil {
			err = closeErr
		}
	}()

	// JSON-encode and write the MANIFEST
	bm := &BackupManifest{
		FileEntries:   fes,
		Position:      replicationPosition,
		TransformHook: *backupStorageHook,
		SkipCompress:  !*backupStorageCompress,
	}
	data, err := json.MarshalIndent(bm, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot JSON encode %v: %v", backupManifest, err)
	}
	if _, err := wc.Write([]byte(data)); err != nil {
		return fmt.Errorf("cannot write %v: %v", backupManifest, err)
	}

	return nil
}

// backupFile backs up an individual file.
func (be *BuiltinBackupEngine) backupFile(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, fe *FileEntry, name string, hookExtraEnv map[string]string) (err error) {
	// Open the source file for reading.
	var source *os.File
	source, err = fe.open(cnf, true)
	if err != nil {
		return err
	}
	defer source.Close()

	fi, err := source.Stat()
	if err != nil {
		return err
	}

	// Open the destination file for writing, and a buffer.
	wc, err := bh.AddFile(ctx, name, fi.Size())
	if err != nil {
		return fmt.Errorf("cannot add file: %v", err)
	}
	defer func() {
		if rerr := wc.Close(); rerr != nil {
			if err != nil {
				// We already have an error, just log this one.
				logger.Errorf2(rerr, "failed to close file %v", name)
			} else {
				err = rerr
			}
		}
	}()
	dst := bufio.NewWriterSize(wc, 2*1024*1024)

	// Create the hasher and the tee on top.
	hasher := newHasher()
	writer := io.MultiWriter(dst, hasher)

	// Create the external write pipe, if any.
	var pipe io.WriteCloser
	var wait hook.WaitFunc
	if *backupStorageHook != "" {
		h := hook.NewHook(*backupStorageHook, []string{"-operation", "write"})
		h.ExtraEnv = hookExtraEnv
		pipe, wait, _, err = h.ExecuteAsWritePipe(writer)
		if err != nil {
			return fmt.Errorf("'%v' hook returned error: %v", *backupStorageHook, err)
		}
		writer = pipe
	}

	// Create the gzip compression pipe, if necessary.
	var gzip *pgzip.Writer
	if *backupStorageCompress {
		gzip, err = pgzip.NewWriterLevel(writer, pgzip.BestSpeed)
		if err != nil {
			return fmt.Errorf("cannot create gziper: %v", err)
		}
		gzip.SetConcurrency(*backupCompressBlockSize, *backupCompressBlocks)
		writer = gzip
	}

	// Copy from the source file to writer (optional gzip,
	// optional pipe, tee, output file and hasher).
	_, err = io.Copy(writer, source)
	if err != nil {
		return fmt.Errorf("cannot copy data: %v", err)
	}

	// Close gzip to flush it, after that all data is sent to writer.
	if gzip != nil {
		if err = gzip.Close(); err != nil {
			return fmt.Errorf("cannot close gzip: %v", err)
		}
	}

	// Close the hook pipe if necessary.
	if pipe != nil {
		if err := pipe.Close(); err != nil {
			return fmt.Errorf("cannot close hook pipe: %v", err)
		}
		stderr, err := wait()
		if stderr != "" {
			logger.Infof("'%v' hook returned stderr: %v", *backupStorageHook, stderr)
		}
		if err != nil {
			return fmt.Errorf("'%v' returned error: %v", *backupStorageHook, err)
		}
	}

	// Flush the buffer to finish writing on destination.
	if err = dst.Flush(); err != nil {
		return fmt.Errorf("cannot flush dst: %v", err)
	}

	// Save the hash.
	fe.Hash = hasher.HashString()
	return nil
}

// ExecuteRestore restores from a backup. If the restore is successful
// we return the position from which replication should start
// otherwise an error is returned
func (be *BuiltinBackupEngine) ExecuteRestore(
	ctx context.Context,
	cnf *Mycnf,
	mysqld MysqlDaemon,
	logger logutil.Logger,
	dir string,
	bhs []backupstorage.BackupHandle,
	restoreConcurrency int,
	hookExtraEnv map[string]string) (mysql.Position, error) {

	var bh backupstorage.BackupHandle
	var bm BackupManifest
	var toRestore int

	for toRestore = len(bhs) - 1; toRestore >= 0; toRestore-- {
		bh = bhs[toRestore]
		rc, err := bh.ReadFile(ctx, backupManifest)
		if err != nil {
			log.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage: can't read MANIFEST: %v)", bh.Name(), dir, err)
			continue
		}

		err = json.NewDecoder(rc).Decode(&bm)
		rc.Close()
		if err != nil {
			log.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage (cannot JSON decode MANIFEST: %v)", bh.Name(), dir, err)
			continue
		}

		logger.Infof("Restore: found backup %v %v to restore with %v files", bh.Directory(), bh.Name(), len(bm.FileEntries))
		break
	}
	if toRestore < 0 {
		// There is at least one attempted backup, but none could be read.
		// This implies there is data we ought to have, so it's not safe to start
		// up empty.
		return mysql.Position{}, errors.New("backup(s) found but none could be read, unsafe to start up empty, restart to retry restore")
	}

	// Starting from here we won't be able to recover if we get stopped by a cancelled
	// context. Thus we use the background context to get through to the finish.

	logger.Infof("Restore: shutdown mysqld")
	err := mysqld.Shutdown(context.Background(), cnf, true)
	if err != nil {
		return mysql.Position{}, err
	}

	logger.Infof("Restore: deleting existing files")
	if err := removeExistingFiles(cnf); err != nil {
		return mysql.Position{}, err
	}

	logger.Infof("Restore: reinit config file")
	err = mysqld.ReinitConfig(context.Background(), cnf)
	if err != nil {
		return mysql.Position{}, err
	}

	logger.Infof("Restore: copying all files")
	if err := be.restoreFiles(context.Background(), cnf, bh, bm.FileEntries, bm.TransformHook, !bm.SkipCompress, restoreConcurrency, hookExtraEnv); err != nil {
		return mysql.Position{}, err
	}

	return bm.Position, nil
}

// restoreFiles will copy all the files from the BackupStorage to the
// right place.
func (be *BuiltinBackupEngine) restoreFiles(ctx context.Context, cnf *Mycnf, bh backupstorage.BackupHandle, fes []FileEntry, transformHook string, compress bool, restoreConcurrency int, hookExtraEnv map[string]string) error {
	sema := sync2.NewSemaphore(restoreConcurrency, 0)
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for i := range fes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Wait until we are ready to go, skip if we already
			// encountered an error.
			sema.Acquire()
			defer sema.Release()
			if rec.HasErrors() {
				return
			}

			// And restore the file.
			name := fmt.Sprintf("%v", i)
			rec.RecordError(be.restoreFile(ctx, cnf, bh, &fes[i], transformHook, compress, name, hookExtraEnv))
		}(i)
	}
	wg.Wait()
	return rec.Error()
}

// restoreFile restores an individual file.
func (be *BuiltinBackupEngine) restoreFile(ctx context.Context, cnf *Mycnf, bh backupstorage.BackupHandle, fe *FileEntry, transformHook string, compress bool, name string, hookExtraEnv map[string]string) (err error) {
	// Open the source file for reading.
	var source io.ReadCloser
	source, err = bh.ReadFile(ctx, name)
	if err != nil {
		return err
	}
	defer source.Close()

	// Open the destination file for writing.
	dstFile, err := fe.open(cnf, false)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dstFile.Close(); cerr != nil {
			if err != nil {
				// We already have an error, just log this one.
				log.Errorf("failed to close file %v: %v", name, cerr)
			} else {
				err = cerr
			}
		}
	}()

	// Create a buffering output.
	dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

	// Create hash to write the compressed data to.
	hasher := newHasher()

	// Create a Tee: we split the input into the hasher
	// and into the gunziper.
	reader := io.TeeReader(source, hasher)

	// Create the external read pipe, if any.
	var wait hook.WaitFunc
	if transformHook != "" {
		h := hook.NewHook(transformHook, []string{"-operation", "read"})
		h.ExtraEnv = hookExtraEnv
		reader, wait, _, err = h.ExecuteAsReadPipe(reader)
		if err != nil {
			return fmt.Errorf("'%v' hook returned error: %v", transformHook, err)
		}
	}

	// Create the uncompresser if needed.
	if compress {
		gz, err := pgzip.NewReader(reader)
		if err != nil {
			return err
		}
		defer func() {
			if cerr := gz.Close(); cerr != nil {
				if err != nil {
					// We already have an error, just log this one.
					log.Errorf("failed to close gunziper %v: %v", name, cerr)
				} else {
					err = cerr
				}
			}
		}()
		reader = gz
	}

	// Copy the data. Will also write to the hasher.
	if _, err = io.Copy(dst, reader); err != nil {
		return err
	}

	// Close the Pipe.
	if wait != nil {
		stderr, err := wait()
		if stderr != "" {
			log.Infof("'%v' hook returned stderr: %v", transformHook, stderr)
		}
		if err != nil {
			return fmt.Errorf("'%v' returned error: %v", transformHook, err)
		}
	}

	// Check the hash.
	hash := hasher.HashString()
	if hash != fe.Hash {
		return fmt.Errorf("hash mismatch for %v, got %v expected %v", fe.Name, hash, fe.Hash)
	}

	// Flush the buffer.
	return dst.Flush()
}

func init() {
	BackupEngineMap["builtin"] = &BuiltinBackupEngine{}
}
