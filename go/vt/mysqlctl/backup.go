// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/cgzip"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
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
	// ErrNoBackup is returned when there is no backup
	ErrNoBackup = errors.New("no available backup")
)

// FileEntry is one file to backup
type FileEntry struct {
	// Base is one of:
	// - backupInnodbDataHomeDir for files that go into Mycnf.InnodbDataHomeDir
	// - backupInnodbLogGroupHomeDir for files that go into Mycnf.InnodbLogGroupHomeDir
	// - backupData for files that go into Mycnf.DataDir
	Base string

	// Name is the file name, relative to Base
	Name string

	// Hash is the hash of the gzip compressed data stored in the
	// BackupStorage.
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
		fd, err = os.Open(name)
	} else {
		dir := path.Dir(name)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create destination directory %v: %v", dir, err)
		}
		fd, err = os.Create(name)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot open source file %v: %v", name, err)
	}
	return fd, nil
}

// BackupManifest represents the backup. It lists all the files, and
// the ReplicationPosition that the backup was taken at.
type BackupManifest struct {
	// FileEntries contains all the files in the backup
	FileEntries []FileEntry

	// ReplicationPosition is the position at which the backup was taken
	ReplicationPosition proto.ReplicationPosition
}

// isDbDir returns true if the given directory contains a DB
func isDbDir(p string) bool {
	// db.opt is there
	if _, err := os.Stat(path.Join(p, "db.opt")); err == nil {
		return true
	}

	// Look for at least one .frm file
	fis, err := ioutil.ReadDir(p)
	if err != nil {
		return false
	}
	for _, fi := range fis {
		if strings.HasSuffix(fi.Name(), ".frm") {
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

func findFilesTobackup(cnf *Mycnf) ([]FileEntry, error) {
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

// Backup is the main entry point for a backup:
// - uses the BackupStorage service to store a new backup
// - shuts down Mysqld during the backup
// - remember if we were replicating, restore the exact same state
func Backup(ctx context.Context, mysqld MysqlDaemon, logger logutil.Logger, bucket, name string, backupConcurrency int, hookExtraEnv map[string]string) error {

	// start the backup with the BackupStorage
	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return err
	}
	bh, err := bs.StartBackup(bucket, name)
	if err != nil {
		return fmt.Errorf("StartBackup failed: %v", err)
	}

	if err = backup(ctx, mysqld, logger, bh, backupConcurrency, hookExtraEnv); err != nil {
		if abortErr := bh.AbortBackup(); abortErr != nil {
			logger.Errorf("failed to abort backup: %v", abortErr)
		}
		return err
	}
	return bh.EndBackup()
}

func backup(ctx context.Context, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, backupConcurrency int, hookExtraEnv map[string]string) error {

	// save initial state so we can restore
	slaveStartRequired := false
	sourceIsMaster := false
	readOnly := true
	var replicationPosition proto.ReplicationPosition

	// see if we need to restart replication after backup
	logger.Infof("getting current replication status")
	slaveStatus, err := mysqld.SlaveStatus()
	switch err {
	case nil:
		slaveStartRequired = slaveStatus.SlaveRunning()
	case ErrNotSlave:
		// keep going if we're the master, might be a degenerate case
		sourceIsMaster = true
	default:
		return fmt.Errorf("cannot get slave status: %v", err)
	}

	// get the read-only flag
	readOnly, err = mysqld.IsReadOnly()
	if err != nil {
		return fmt.Errorf("cannot get read only status: %v", err)
	}

	// get the replication position
	if sourceIsMaster {
		if !readOnly {
			logger.Infof("turning master read-onyl before backup")
			if err = mysqld.SetReadOnly(true); err != nil {
				return fmt.Errorf("cannot get read only status: %v", err)
			}
		}
		replicationPosition, err = mysqld.MasterPosition()
		if err != nil {
			return fmt.Errorf("cannot get master position: %v", err)
		}
	} else {
		if err = StopSlave(mysqld, hookExtraEnv); err != nil {
			return fmt.Errorf("cannot stop slave: %v", err)
		}
		var slaveStatus proto.ReplicationStatus
		slaveStatus, err = mysqld.SlaveStatus()
		if err != nil {
			return fmt.Errorf("cannot get slave status: %v", err)
		}
		replicationPosition = slaveStatus.Position
	}
	logger.Infof("using replication position: %v", replicationPosition)

	// shutdown mysqld
	err = mysqld.Shutdown(ctx, true)
	if err != nil {
		return fmt.Errorf("cannot shutdown mysqld: %v", err)
	}

	// get the files to backup
	fes, err := findFilesTobackup(mysqld.Cnf())
	if err != nil {
		return fmt.Errorf("cannot find files to backup: %v", err)
	}
	logger.Infof("found %v files to backup", len(fes))

	// backup everything
	if err := backupFiles(mysqld, logger, bh, fes, replicationPosition, backupConcurrency); err != nil {
		return fmt.Errorf("cannot backup files: %v", err)
	}

	// Try to restart mysqld
	err = mysqld.Start(ctx)
	if err != nil {
		return fmt.Errorf("cannot restart mysqld: %v", err)
	}

	// Restore original mysqld state that we saved above.
	if slaveStartRequired {
		logger.Infof("restarting mysql replication")
		if err := StartSlave(mysqld, hookExtraEnv); err != nil {
			return fmt.Errorf("cannot restart slave: %v", err)
		}

		// this should be quick, but we might as well just wait
		if err := WaitForSlaveStart(mysqld, slaveStartDeadline); err != nil {
			return fmt.Errorf("slave is not restarting: %v", err)
		}
	}

	// And set read-only mode
	logger.Infof("resetting mysqld read-only to %v", readOnly)
	if err := mysqld.SetReadOnly(readOnly); err != nil {
		return err
	}

	return nil
}

func backupFiles(mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, fes []FileEntry, replicationPosition proto.ReplicationPosition, backupConcurrency int) (err error) {
	sema := sync2.NewSemaphore(backupConcurrency, 0)
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for i, fe := range fes {
		wg.Add(1)
		go func(i int, fe FileEntry) {
			defer wg.Done()

			// wait until we are ready to go, skip if we already
			// encountered an error
			sema.Acquire()
			defer sema.Release()
			if rec.HasErrors() {
				return
			}

			// open the source file for reading
			source, err := fe.open(mysqld.Cnf(), true)
			if err != nil {
				rec.RecordError(err)
				return
			}
			defer source.Close()

			// open the destination file for writing, and a buffer
			name := fmt.Sprintf("%v", i)
			wc, err := bh.AddFile(name)
			if err != nil {
				rec.RecordError(fmt.Errorf("cannot add file: %v", err))
				return
			}
			defer func() { rec.RecordError(wc.Close()) }()
			dst := bufio.NewWriterSize(wc, 2*1024*1024)

			// create the hasher and the tee on top
			hasher := newHasher()
			tee := io.MultiWriter(dst, hasher)

			// create the gzip compression filter
			gzip, err := cgzip.NewWriterLevel(tee, cgzip.Z_BEST_SPEED)
			if err != nil {
				rec.RecordError(fmt.Errorf("cannot create gziper: %v", err))
				return
			}

			// copy from the source file to gzip to tee to output file and hasher
			_, err = io.Copy(gzip, source)
			if err != nil {
				rec.RecordError(fmt.Errorf("cannot copy data: %v", err))
				return
			}

			// close gzip to flush it, after that the hash is good
			if err = gzip.Close(); err != nil {
				rec.RecordError(fmt.Errorf("cannot close gzip: %v", err))
				return
			}

			// flush the buffer to finish writing, save the hash
			rec.RecordError(dst.Flush())
			fes[i].Hash = hasher.HashString()
		}(i, fe)
	}

	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// open the MANIFEST
	wc, err := bh.AddFile(backupManifest)
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
		FileEntries:         fes,
		ReplicationPosition: replicationPosition,
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

// checkNoDB makes sure there is no vt_ db already there. Used by Restore,
// we do not wnat to destroy an existing DB.
func checkNoDB(mysqld MysqlDaemon) error {
	qr, err := mysqld.FetchSuperQuery("SHOW DATABASES")
	if err != nil {
		return fmt.Errorf("checkNoDB failed: %v", err)
	}

	for _, row := range qr.Rows {
		if strings.HasPrefix(row[0].String(), "vt_") {
			dbName := row[0].String()
			tableQr, err := mysqld.FetchSuperQuery("SHOW TABLES FROM " + dbName)
			if err != nil {
				return fmt.Errorf("checkNoDB failed: %v", err)
			} else if len(tableQr.Rows) == 0 {
				// no tables == empty db, all is well
				continue
			}
			return fmt.Errorf("checkNoDB failed, found active db %v", dbName)
		}
	}

	return nil
}

// restoreFiles will copy all the files from the BackupStorage to the
// right place
func restoreFiles(cnf *Mycnf, bh backupstorage.BackupHandle, fes []FileEntry, restoreConcurrency int) error {
	sema := sync2.NewSemaphore(restoreConcurrency, 0)
	rec := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}
	for i, fe := range fes {
		wg.Add(1)
		go func(i int, fe FileEntry) {
			defer wg.Done()

			// wait until we are ready to go, skip if we already
			// encountered an error
			sema.Acquire()
			defer sema.Release()
			if rec.HasErrors() {
				return
			}

			// open the source file for reading
			name := fmt.Sprintf("%v", i)
			source, err := bh.ReadFile(name)
			if err != nil {
				rec.RecordError(err)
				return
			}
			defer source.Close()

			// open the destination file for writing
			dstFile, err := fe.open(cnf, false)
			if err != nil {
				rec.RecordError(err)
				return
			}
			defer func() { rec.RecordError(dstFile.Close()) }()

			// create a buffering output
			dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

			// create hash to write the compressed data to
			hasher := newHasher()

			// create a Tee: we split the input into the hasher
			// and into the gunziper
			tee := io.TeeReader(source, hasher)

			// create the uncompresser
			gz, err := cgzip.NewReader(tee)
			if err != nil {
				rec.RecordError(err)
				return
			}
			defer func() { rec.RecordError(gz.Close()) }()

			// copy the data. Will also write to the hasher
			if _, err = io.Copy(dst, gz); err != nil {
				rec.RecordError(err)
				return
			}

			// check the hash
			hash := hasher.HashString()
			if hash != fe.Hash {
				rec.RecordError(fmt.Errorf("hash mismatch for %v, got %v expected %v", fe.Name, hash, fe.Hash))
				return
			}

			// flush the buffer
			rec.RecordError(dst.Flush())
		}(i, fe)
	}
	wg.Wait()
	return rec.Error()
}

// Restore is the main entry point for backup restore.  If there is no
// appropriate backup on the BackupStorage, Restore logs an error
// and returns ErrNoBackup. Any other error is returned.
func Restore(ctx context.Context, mysqld MysqlDaemon, bucket string, restoreConcurrency int, hookExtraEnv map[string]string) (proto.ReplicationPosition, error) {
	// find the right backup handle: most recent one, with a MANIFEST
	log.Infof("Restore: looking for a suitable backup to restore")
	bs, err := backupstorage.GetBackupStorage()
	if err != nil {
		return proto.ReplicationPosition{}, err
	}
	bhs, err := bs.ListBackups(bucket)
	if err != nil {
		return proto.ReplicationPosition{}, fmt.Errorf("ListBackups failed: %v", err)
	}
	toRestore := len(bhs) - 1
	var bh backupstorage.BackupHandle
	var bm BackupManifest
	for toRestore >= 0 {
		bh = bhs[toRestore]
		if rc, err := bh.ReadFile(backupManifest); err == nil {
			dec := json.NewDecoder(rc)
			err := dec.Decode(&bm)
			rc.Close()
			if err != nil {
				log.Warningf("Possibly incomplete backup %v in bucket %v on BackupStorage (cannot JSON decode MANIFEST: %v)", bh.Name(), bucket, err)
			} else {
				log.Infof("Restore: found backup %v %v to restore with %v files", bh.Bucket(), bh.Name(), len(bm.FileEntries))
				break
			}
		} else {
			log.Warningf("Possibly incomplete backup %v in bucket %v on BackupStorage (cannot read MANIFEST)", bh.Name(), bucket)
		}
		toRestore--
	}
	if toRestore < 0 {
		log.Errorf("No backup to restore on BackupStorage for bucket %v", bucket)
		return proto.ReplicationPosition{}, ErrNoBackup
	}

	log.Infof("Restore: checking no existing data is present")
	if err := checkNoDB(mysqld); err != nil {
		return proto.ReplicationPosition{}, err
	}

	log.Infof("Restore: shutdown mysqld")
	err = mysqld.Shutdown(ctx, true)
	if err != nil {
		return proto.ReplicationPosition{}, err
	}

	log.Infof("Restore: copying all files")
	if err := restoreFiles(mysqld.Cnf(), bh, bm.FileEntries, restoreConcurrency); err != nil {
		return proto.ReplicationPosition{}, err
	}

	log.Infof("Restore: running mysql_upgrade if necessary")
	if err := mysqld.RunMysqlUpgrade(); err != nil {
		return proto.ReplicationPosition{}, err
	}

	log.Infof("Restore: restart mysqld")
	err = mysqld.Start(ctx)
	if err != nil {
		return proto.ReplicationPosition{}, err
	}

	return bm.ReplicationPosition, nil
}
