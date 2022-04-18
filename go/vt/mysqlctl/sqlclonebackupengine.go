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
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/planetscale/pargzip"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	sqlCloneBackupEngineName = "sqlclonebackup"
	// sqlCloneWriterBufferSize = 2 * 1024 * 1024
)

var (
	// BuiltinBackupMysqldTimeout is how long ExecuteBackup should wait for response from mysqld.Shutdown.
	// It can later be extended for other calls to mysqld during backup functions.
	// Exported for testing.
	// sqlCloneBackupMysqldTimeout = flag.Duration("sqlclonebackup_mysqld_timeout", 10*time.Minute, "how long to wait for mysqld to shutdown at the start of the backup")

	sqlCloneBackupProgress = flag.Duration("sqlclonebackup_progress", 5*time.Second, "how often to send progress updates when backing up large files")
	// path where backup is stored
	sqlCloneBackupEnginePath = flag.String("sqlclonebackup_root_path", "/tmp/clone", "directory location of the xtrabackup and xbstream executables, e.g., /usr/bin")

	sqlCloneBackupEngineDonorHost = flag.String("sqlclonebackup_donor_host", "localhost", "host address of donor machine")
	sqlCloneBackupEngineDonorPort = flag.String("sqlclonebackup_donor_port", "17100", "port address of donor machine")
)

// BuiltinBackupEngine encapsulates the logic of the builtin engine
// it implements the BackupEngine interface and contains all the logic
// required to implement a backup/restore by copying files from and to
// the correct location / storage bucket
type SQLCloneBackupEngine struct {
}

// builtinBackupManifest represents the backup. It lists all the files, the
// Position that the backup was taken at, and the transform hook used,
// if any.
type sqlCloneBackupManifest struct {
	// BackupManifest is an anonymous embedding of the base manifest struct.
	BackupManifest

	// FileEntries contains all the files in the backup
	FileEntries []FileEntry

	// TransformHook that was used on the files, if any.
	TransformHook string

	// SkipCompress is true if the backup files were NOT run through gzip.
	// The field is expressed as a negative because it will come through as
	// false for backups that were created before the field existed, and those
	// backups all had compression enabled.
	SkipCompress bool
}

// ExecuteBackup returns a boolean that indicates if the backup is usable,
// and an overall error.
func (be *SQLCloneBackupEngine) ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (bool, error) {

	params.Logger.Infof("Hook: %v, Compress: %v", *backupStorageHook, *backupStorageCompress)

	// Save initial state so we can restore.
	//replicaStartRequired := false
	sourceIsPrimary := false
	readOnly := true //nolint
	var replicationPosition mysql.Position
	//semiSyncSource, semiSyncReplica := params.Mysqld.SemiSyncEnabled()

	// See if we need to restart replication after backup.
	/*params.Logger.Infof("getting current replication status")
	replicaStatus, err := params.Mysqld.ReplicationStatus()
	switch err {
	case nil:
		replicaStartRequired = replicaStatus.Healthy() && !*DisableActiveReparents
	case mysql.ErrNotReplica:
		// keep going if we're the primary, might be a degenerate case
		sourceIsPrimary = true
	default:
		return false, vterrors.Wrap(err, "can't get replica status")
	}*/

	// get the read-only flag
	readOnly, err := params.Mysqld.IsReadOnly()
	if err != nil {
		return false, vterrors.Wrap(err, "can't get read-only status")
	}

	// get the replication position
	if sourceIsPrimary {
		if !readOnly {
			params.Logger.Infof("turning primary read-only before backup")
			if err = params.Mysqld.SetReadOnly(true); err != nil {
				return false, vterrors.Wrap(err, "can't set read-only status")
			}
		}
		replicationPosition, err = params.Mysqld.PrimaryPosition()
		if err != nil {
			return false, vterrors.Wrap(err, "can't get position on primary")
		}
	} else {
		if err = params.Mysqld.StopReplication(params.HookExtraEnv); err != nil {
			return false, vterrors.Wrapf(err, "can't stop replica")
		}
		var replicaStatus mysql.ReplicationStatus
		replicaStatus, err = params.Mysqld.ReplicationStatus()
		if err != nil {
			return false, vterrors.Wrap(err, "can't get replica status")
		}
		replicationPosition = replicaStatus.Position
	}
	params.Logger.Infof("using replication position: %v", replicationPosition)

	// shutdown mysqld
	/*shutdownCtx, cancel := context.WithTimeout(ctx, *BuiltinBackupMysqldTimeout)
	err = params.Mysqld.Shutdown(shutdownCtx, params.Cnf, true)
	defer cancel()
	if err != nil {
		return false, vterrors.Wrap(err, "can't shutdown mysqld")
	}*/

	params.Logger.Infof("path for backup is %s", *sqlCloneBackupEnginePath)
	//_, err = slqLocalCloneCall(ctx, params)
	_, err = slqRemoteCloneCall(ctx, params)
	if err != nil {
		return false, vterrors.Wrap(err, "error running clone plugin")
	}

	// Backup everything, capture the error.
	backupErr := be.backupFiles(ctx, params, bh, replicationPosition)
	usable := backupErr == nil

	// Try to restart mysqld, use background context in case we timed out the original context
	/*err = params.Mysqld.Start(context.Background(), params.Cnf)
	if err != nil {
		return usable, vterrors.Wrap(err, "can't restart mysqld")
	}

	// And set read-only mode
	params.Logger.Infof("resetting mysqld read-only to %v", readOnly)
	if err := params.Mysqld.SetReadOnly(readOnly); err != nil {
		return usable, err
	}*/

	// Restore original mysqld state that we saved above.
	/*if semiSyncSource || semiSyncReplica {
		// Only do this if one of them was on, since both being off could mean
		// the plugin isn't even loaded, and the server variables don't exist.
		params.Logger.Infof("restoring semi-sync settings from before backup: primary=%v, replica=%v",
			semiSyncSource, semiSyncReplica)
		err := params.Mysqld.SetSemiSyncEnabled(semiSyncSource, semiSyncReplica)
		if err != nil {
			return usable, err
		}
	}
	if replicaStartRequired {
		params.Logger.Infof("restarting mysql replication")
		if err := params.Mysqld.StartReplication(params.HookExtraEnv); err != nil {
			return usable, vterrors.Wrap(err, "cannot restart replica")
		}

		// this should be quick, but we might as well just wait
		if err := WaitForReplicationStart(params.Mysqld, replicationStartDeadline); err != nil {
			return usable, vterrors.Wrap(err, "replica is not restarting")
		}

		// Wait for a reliable value for ReplicationLagSeconds from ReplicationStatus()

		// We know that we stopped at replicationPosition.
		// If PrimaryPosition is the same, that means no writes
		// have happened to primary, so we are up-to-date.
		// Otherwise, we wait for replica's Position to change from
		// the saved replicationPosition before proceeding
		tmc := tmclient.NewTabletManagerClient()
		defer tmc.Close()
		remoteCtx, remoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer remoteCancel()

		pos, err := sqllCloneGetPrimaryPosition(remoteCtx, tmc, params.TopoServer, params.Keyspace, params.Shard)
		// If we are unable to get the primary's position, return error.
		if err != nil {
			return usable, err
		}
		if !replicationPosition.Equal(pos) {
			for {
				if err := ctx.Err(); err != nil {
					return usable, err
				}
				status, err := params.Mysqld.ReplicationStatus()
				if err != nil {
					return usable, err
				}
				newPos := status.Position
				if !newPos.Equal(replicationPosition) {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}
	}*/

	return usable, backupErr
}

// backupFiles finds the list of files to backup, and creates the backup.
func (be *SQLCloneBackupEngine) backupFiles(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle, replicationPosition mysql.Position) (finalErr error) {

	// Get the files to backup.
	// We don't care about totalSize because we add each file separately.
	fes, size, err := SQLCloneFindFilesToBackup(params.Cnf)
	if err != nil {
		return vterrors.Wrap(err, "can't find files to backup")
	}
	params.Logger.Infof("found %v files to backup. Total size is %d", len(fes), size)
	params.Logger.Infof("***** Listing all files *****")
	for i := range fes {
		params.Logger.Infof("%s", fes[i])
	}

	// Backup with the provided concurrency.
	sema := sync2.NewSemaphore(params.Concurrency, 0)
	wg := sync.WaitGroup{}
	for i := range fes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Wait until we are ready to go, skip if we already
			// encountered an error.
			sema.Acquire()
			defer sema.Release()
			if bh.HasErrors() {
				return
			}

			// Backup the individual file.
			name := fmt.Sprintf("%v", i)
			params.Logger.Infof("backing file %s into %s", fes[i], name)
			bh.RecordError(be.backupFile(ctx, params, bh, &fes[i], name))
			//fileName := filepath.Base(fes[i].Name)
			//bh.RecordError(be.backupFile(ctx, params, bh, &fes[i], fileName))
		}(i)
	}

	wg.Wait()

	// BackupHandle supports the ErrorRecorder interface for tracking errors
	// across any goroutines that fan out to take the backup. This means that we
	// don't need a local error recorder and can put everything through the bh.
	//
	// This handles the scenario where bh.AddFile() encounters an error asynchronously,
	// which ordinarily would be lost in the context of `be.backupFile`, i.e. if an
	// error were encountered
	// [here](https://github.com/vitessio/vitess/blob/d26b6c7975b12a87364e471e2e2dfa4e253c2a5b/go/vt/mysqlctl/s3backupstorage/s3.go#L139-L142).
	if bh.HasErrors() {
		return bh.Error()
	}

	// open the MANIFEST
	wc, err := bh.AddFile(ctx, backupManifestFileName, backupstorage.FileSizeUnknown)
	if err != nil {
		return vterrors.Wrapf(err, "cannot add %v to backup", backupManifestFileName)
	}
	defer func() {
		if closeErr := wc.Close(); finalErr == nil {
			finalErr = closeErr
		}
	}()

	// JSON-encode and write the MANIFEST
	bm := &sqlCloneBackupManifest{
		// Common base fields
		BackupManifest: BackupManifest{
			BackupMethod: sqlCloneBackupEngineName,
			Position:     replicationPosition,
			BackupTime:   params.BackupTime.UTC().Format(time.RFC3339),
			FinishedTime: time.Now().UTC().Format(time.RFC3339),
		},

		// Builtin-specific fields
		FileEntries:   fes,
		TransformHook: *backupStorageHook,
		SkipCompress:  !*backupStorageCompress,
	}
	data, err := json.MarshalIndent(bm, "", "  ")
	if err != nil {
		return vterrors.Wrapf(err, "cannot JSON encode %v", backupManifestFileName)
	}
	if _, err := wc.Write([]byte(data)); err != nil {
		return vterrors.Wrapf(err, "cannot write %v", backupManifestFileName)
	}

	return nil
}

type sqlCloneBackupPipe struct {
	filename string
	maxSize  int64

	r io.Reader
	w *bufio.Writer

	crc32  hash.Hash32
	nn     int64
	done   chan struct{}
	closed int32
}

/*func sqlCloneNewBackupWriter(filename string, maxSize int64, w io.Writer) *backupPipe {
	return &backupPipe{
		crc32:    crc32.NewIEEE(),
		w:        bufio.NewWriterSize(w, writerBufferSize),
		filename: filename,
		maxSize:  maxSize,
		done:     make(chan struct{}),
	}
}*/

/*func sqlCloneNewBackupReader(filename string, r io.Reader) *backupPipe {
	return &backupPipe{
		crc32:    crc32.NewIEEE(),
		r:        r,
		filename: filename,
		done:     make(chan struct{}),
	}
}*/

func (bp *sqlCloneBackupPipe) Read(p []byte) (int, error) {
	nn, err := bp.r.Read(p)
	_, _ = bp.crc32.Write(p[:nn])
	atomic.AddInt64(&bp.nn, int64(nn))
	return nn, err
}

func (bp *sqlCloneBackupPipe) Write(p []byte) (int, error) {
	nn, err := bp.w.Write(p)
	_, _ = bp.crc32.Write(p[:nn])
	atomic.AddInt64(&bp.nn, int64(nn))
	return nn, err
}

func (bp *sqlCloneBackupPipe) Close() error {
	if atomic.CompareAndSwapInt32(&bp.closed, 0, 1) {
		close(bp.done)
		if bp.w != nil {
			if err := bp.w.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bp *sqlCloneBackupPipe) HashString() string {
	return hex.EncodeToString(bp.crc32.Sum(nil))
}

func (bp *sqlCloneBackupPipe) SQLCloneReportProgress(period time.Duration, logger logutil.Logger) {
	tick := time.NewTicker(period)
	defer tick.Stop()

	for {
		select {
		case <-bp.done:
			return
		case <-tick.C:
			written := float64(atomic.LoadInt64(&bp.nn))
			if bp.maxSize == 0 {
				logger.Infof("Backup %q: %.02fkb", bp.filename, written/1024.0)
			} else {
				maxSize := float64(bp.maxSize)
				logger.Infof("Backup %q: %.02f%% (%.02f/%.02fkb)", bp.filename, 100.0*written/maxSize, written/1024.0, maxSize/1024.0)
			}
		}
	}
}

// backupFile backs up an individual file.
func (be *SQLCloneBackupEngine) backupFile(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle, fe *FileEntry, name string) (finalErr error) {
	// Open the source file for reading.
	source, err := fe.freeOpen(params.Cnf, true)
	if err != nil {
		return err
	}
	defer source.Close()

	fi, err := source.Stat()
	if err != nil {
		return err
	}

	params.Logger.Infof("Backing up file: %v", fe.Name)
	// Open the destination file for writing, and a buffer.
	wc, err := bh.AddFile(ctx, name, fi.Size())
	if err != nil {
		return vterrors.Wrapf(err, "cannot add file: %v,%v", name, fe.Name)
	}
	defer func(name, fileName string) {
		if rerr := wc.Close(); rerr != nil {
			if finalErr != nil {
				// We already have an error, just log this one.
				params.Logger.Errorf2(rerr, "failed to close file %v,%v", name, fe.Name)
			} else {
				finalErr = rerr
			}
		}
	}(name, fe.Name)

	bw := newBackupWriter(fe.Name, fi.Size(), wc)
	go bw.ReportProgress(*sqlCloneBackupProgress, params.Logger)

	var writer io.Writer = bw

	// Create the external write pipe, if any.
	var pipe io.WriteCloser
	var wait hook.WaitFunc
	if *backupStorageHook != "" {
		h := hook.NewHook(*backupStorageHook, []string{"-operation", "write"})
		h.ExtraEnv = params.HookExtraEnv
		pipe, wait, _, err = h.ExecuteAsWritePipe(writer)
		if err != nil {
			return vterrors.Wrapf(err, "'%v' hook returned error", *backupStorageHook)
		}
		writer = pipe
	}

	// Create the gzip compression pipe, if necessary.
	var gzip *pargzip.Writer
	if *backupStorageCompress {
		gzip = pargzip.NewWriter(writer)
		gzip.ChunkSize = *backupCompressBlockSize
		gzip.Parallel = *backupCompressBlocks
		gzip.CompressionLevel = pargzip.BestSpeed
		writer = gzip
	}

	// Copy from the source file to writer (optional gzip,
	// optional pipe, tee, output file and hasher).
	_, err = io.Copy(writer, source)
	if err != nil {
		return vterrors.Wrap(err, "cannot copy data")
	}

	// Close gzip to flush it, after that all data is sent to writer.
	if gzip != nil {
		if err = gzip.Close(); err != nil {
			return vterrors.Wrap(err, "cannot close gzip")
		}
	}

	// Close the hook pipe if necessary.
	if pipe != nil {
		if err := pipe.Close(); err != nil {
			return vterrors.Wrap(err, "cannot close hook pipe")
		}
		stderr, err := wait()
		if stderr != "" {
			params.Logger.Infof("'%v' hook returned stderr: %v", *backupStorageHook, stderr)
		}
		if err != nil {
			return vterrors.Wrapf(err, "'%v' returned error", *backupStorageHook)
		}
	}

	// Close the backupPipe to finish writing on destination.
	if err = bw.Close(); err != nil {
		return vterrors.Wrapf(err, "cannot flush destination: %v", name)
	}

	// Save the hash.
	fe.Hash = bw.HashString()
	return nil
}

// ExecuteRestore restores from a backup. If the restore is successful
// we return the position from which replication should start
// otherwise an error is returned
func (be *SQLCloneBackupEngine) ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (*BackupManifest, error) {

	var bm builtinBackupManifest

	if err := getBackupManifestInto(ctx, bh, &bm); err != nil {
		return nil, err
	}

	// mark restore as in progress
	if err := createStateFile(params.Cnf); err != nil {
		return nil, err
	}

	if err := prepareToRestore(ctx, params.Cnf, params.Mysqld, params.Logger); err != nil {
		return nil, err
	}

	params.Logger.Infof("Restore: copying %v files", len(bm.FileEntries))

	if err := be.restoreFiles(context.Background(), params, bh, bm); err != nil {
		// don't delete the file here because that is how we detect an interrupted restore
		return nil, vterrors.Wrap(err, "failed to restore files")
	}

	params.Logger.Infof("Restore: returning replication position %v", bm.Position)
	return &bm.BackupManifest, nil
}

// restoreFiles will copy all the files from the BackupStorage to the
// right place.
func (be *SQLCloneBackupEngine) restoreFiles(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle, bm builtinBackupManifest) error {
	fes := bm.FileEntries
	sema := sync2.NewSemaphore(params.Concurrency, 0)
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
			params.Logger.Infof("Copying file %v: %v", name, fes[i].Name)
			err := be.restoreFile(ctx, params, bh, &fes[i], bm.TransformHook, !bm.SkipCompress, name)
			if err != nil {
				rec.RecordError(vterrors.Wrapf(err, "can't restore file %v to %v", name, fes[i].Name))
			}
		}(i)
	}
	wg.Wait()
	return rec.Error()
}

// restoreFile restores an individual file.
func (be *SQLCloneBackupEngine) restoreFile(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle, fe *FileEntry, transformHook string, compress bool, name string) (finalErr error) {
	// Open the source file for reading.
	source, err := bh.ReadFile(ctx, name)
	if err != nil {
		return vterrors.Wrap(err, "can't open source file for reading")
	}
	defer source.Close()

	// Open the destination file for writing.
	dstFile, err := fe.open(params.Cnf, false)
	params.Logger.Infof("backing up file location: ", dstFile.Name())
	if err != nil {
		return vterrors.Wrap(err, "can't open destination file for writing")
	}
	defer func() {
		if cerr := dstFile.Close(); cerr != nil {
			if finalErr != nil {
				// We already have an error, just log this one.
				log.Errorf("failed to close file %v: %v", name, cerr)
			} else {
				finalErr = vterrors.Wrap(cerr, "failed to close destination file")
			}
		}
	}()

	bp := newBackupReader(name, source)
	go bp.ReportProgress(*sqlCloneBackupProgress, params.Logger)

	dst := bufio.NewWriterSize(dstFile, writerBufferSize)
	var reader io.Reader = bp

	// Create the external read pipe, if any.
	var wait hook.WaitFunc
	if transformHook != "" {
		h := hook.NewHook(transformHook, []string{"-operation", "read"})
		h.ExtraEnv = params.HookExtraEnv
		reader, wait, _, err = h.ExecuteAsReadPipe(reader)
		if err != nil {
			return vterrors.Wrapf(err, "'%v' hook returned error", transformHook)
		}
	}

	// Create the uncompresser if needed.
	if compress {
		params.Logger.Infof("inside unzip")
		gz, err := pgzip.NewReader(reader)
		if err != nil {
			return vterrors.Wrap(err, "can't open gzip decompressor")
		}
		defer func() {
			if cerr := gz.Close(); cerr != nil {
				if finalErr != nil {
					// We already have an error, just log this one.
					log.Errorf("failed to close gzip decompressor %v: %v", name, cerr)
				} else {
					finalErr = vterrors.Wrap(err, "failed to close gzip decompressor")
				}
			}
		}()
		reader = gz
	}

	// Copy the data. Will also write to the hasher.
	if _, err = io.Copy(dst, reader); err != nil {
		return vterrors.Wrap(err, "failed to copy file contents")
	}

	// Close the Pipe.
	if wait != nil {
		stderr, err := wait()
		if stderr != "" {
			log.Infof("'%v' hook returned stderr: %v", transformHook, stderr)
		}
		if err != nil {
			return vterrors.Wrapf(err, "'%v' returned error", transformHook)
		}
	}

	// Check the hash.
	hash := bp.HashString()
	if hash != fe.Hash {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "hash mismatch for %v, got %v expected %v", fe.Name, hash, fe.Hash)
	}

	// Flush the buffer.
	if err := dst.Flush(); err != nil {
		return vterrors.Wrap(err, "failed to flush destination buffer")
	}

	if err := bp.Close(); err != nil {
		return vterrors.Wrap(err, "failed to close the source reader")
	}

	return nil
}

// ShouldDrainForBackup satisfies the BackupEngine interface
// backup requires query service to be stopped, hence true
func (be *SQLCloneBackupEngine) ShouldDrainForBackup() bool {
	return true
}

func SQLCloneFindFilesToBackup(cnf *Mycnf) ([]FileEntry, int64, error) {
	var err error
	var result []FileEntry
	var totalSize int64

	// first add inno db files
	result, totalSize, err = addAllFiles(result, *sqlCloneBackupEnginePath)
	if err != nil {
		return nil, 0, err
	}

	return result, totalSize, nil
}

/*func slqLocalCloneCall(ctx context.Context, params BackupParams) (complete bool, finalErr error) {
	//ctx := context.Background()
	conn, err := params.Mysqld.GetDbaConnection(ctx)
	//conn, err := mysql.Connect(ctx, &vtParams)
	if conn != nil && err == nil {
		defer conn.Close()
	}

	if err != nil {
		return false, vterrors.Wrap(err, "unable to obtain a connection to the database")
	}
	insertSmt := fmt.Sprintf("CLONE LOCAL DATA DIRECTORY = '%s';", *sqlCloneBackupEnginePath)
	_, err = conn.ExecuteFetch(insertSmt, 1000, true)

	return true, err
}*/

func slqRemoteCloneCall(ctx context.Context, params BackupParams) (complete bool, finalErr error) {
	//ctx := context.Background()
	conn, err := params.Mysqld.GetDbaConnection(ctx)
	//conn, err := mysql.Connect(ctx, &vtParams)
	if conn != nil && err == nil {
		defer conn.Close()
	}

	if err != nil {
		return false, vterrors.Wrap(err, "unable to obtain a connection to the database")
	}
	insertSmt := fmt.Sprintf("CLONE INSTANCE FROM 'donor_clone_user'@'%s':%s IDENTIFIED BY '' DATA DIRECTORY = '%s';", *sqlCloneBackupEngineDonorHost, *sqlCloneBackupEngineDonorPort, *sqlCloneBackupEnginePath)
	params.Logger.Infof(fmt.Sprintf("query is %s", insertSmt))
	_, err = conn.ExecuteFetch(insertSmt, 1000, true)

	return true, err
}

/*func sqllCloneGetPrimaryPosition(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, keyspace, shard string) (mysql.Position, error) {
	si, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return mysql.Position{}, vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
		return mysql.Position{}, fmt.Errorf("shard %v/%v has no primary", keyspace, shard)
	}
	ti, err := ts.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("can't get primary tablet record %v: %v", topoproto.TabletAliasString(si.PrimaryAlias), err)
	}
	posStr, err := tmc.PrimaryPosition(ctx, ti.Tablet)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("can't get primary replication position: %v", err)
	}
	pos, err := mysql.DecodePosition(posStr)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("can't decode primary replication position %q: %v", posStr, err)
	}
	return pos, nil
}*/

func init() {
	BackupRestoreEngineMap[sqlCloneBackupEngineName] = &SQLCloneBackupEngine{}
}
