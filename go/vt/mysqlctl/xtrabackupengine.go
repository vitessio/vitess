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
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/pgzip"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// XtrabackupEngine encapsulates the logic of the xtrabackup engine
// it implements the BackupEngine interface and contains all the logic
// required to implement a backup/restore by invoking xtrabackup with
// the appropriate parameters
type XtrabackupEngine struct {
}

var (
	// path where backup engine program is located
	xtrabackupEnginePath = flag.String("xtrabackup_root_path", "", "directory location of the xtrabackup executable, e.g., /usr/bin")
	// flags to pass through to backup engine
	xtrabackupBackupFlags = flag.String("xtrabackup_backup_flags", "", "flags to pass to backup command. These should be space separated and will be added to the end of the command")
	// flags to pass through to restore phase
	xbstreamRestoreFlags = flag.String("xbstream_restore_flags", "", "flags to pass to xbstream command during restore. These should be space separated and will be added to the end of the command. These need to match the ones used for backup e.g. --compress / --decompress, --encrypt / --decrypt")
	// streaming mode
	xtrabackupStreamMode = flag.String("xtrabackup_stream_mode", "tar", "which mode to use if streaming, valid values are tar and xbstream")
	xtrabackupUser       = flag.String("xtrabackup_user", "", "User that xtrabackup will use to connect to the database server. This user must have all necessary privileges. For details, please refer to xtrabackup documentation.")
	// striping mode
	xtrabackupStripes         = flag.Uint("xtrabackup_stripes", 0, "If greater than 0, use data striping across this many destination files to parallelize data transfer and decompression")
	xtrabackupStripeBlockSize = flag.Uint("xtrabackup_stripe_block_size", 102400, "Size in bytes of each block that gets sent to a given stripe before rotating to the next stripe")
)

const (
	streamModeTar        = "tar"
	xtrabackupBinaryName = "xtrabackup"
	xtrabackupEngineName = "xtrabackup"
	xbstream             = "xbstream"

	// closeTimeout is the timeout for closing backup files after writing.
	closeTimeout = 10 * time.Minute
)

// xtraBackupManifest represents a backup.
// It stores the name of the backup file, the replication position,
// whether the backup is compressed using gzip, and any extra
// command line parameters used while invoking it.
type xtraBackupManifest struct {
	// BackupManifest is an anonymous embedding of the base manifest struct.
	BackupManifest

	// Name of the backup file
	FileName string
	// Params are the parameters that backup was run with
	Params string `json:"ExtraCommandLineParams"`
	// StreamMode is the stream mode used to create this backup.
	StreamMode string
	// NumStripes is the number of stripes the file is split across, if any.
	NumStripes int32
	// StripeBlockSize is the size in bytes of each stripe block.
	StripeBlockSize int32

	// SkipCompress is true if the backup files were NOT run through gzip.
	// The field is expressed as a negative because it will come through as
	// false for backups that were created before the field existed, and those
	// backups all had compression enabled.
	SkipCompress bool
}

func (be *XtrabackupEngine) backupFileName() string {
	fileName := "backup"
	if *xtrabackupStreamMode != "" {
		fileName += "."
		fileName += *xtrabackupStreamMode
	}
	if *backupStorageCompress {
		fileName += ".gz"
	}
	return fileName
}

func closeFile(wc io.WriteCloser, fileName string, logger logutil.Logger, finalErr *error) {
	logger.Infof("Closing backup file %v", fileName)
	if closeErr := wc.Close(); *finalErr == nil {
		*finalErr = closeErr
	} else if closeErr != nil {
		// since we already have an error just log this
		logger.Errorf("error closing file %v: %v", fileName, closeErr)
	}
}

// ExecuteBackup returns a boolean that indicates if the backup is usable,
// and an overall error.
func (be *XtrabackupEngine) ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (complete bool, finalErr error) {
	// extract all params from BackupParams
	cnf := params.Cnf
	mysqld := params.Mysqld
	logger := params.Logger

	if *xtrabackupUser == "" {
		return false, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "xtrabackupUser must be specified.")
	}
	// use a mysql connection to detect flavor at runtime
	conn, err := mysqld.GetDbaConnection()
	if conn != nil && err == nil {
		defer conn.Close()
	}

	if err != nil {
		return false, vterrors.Wrap(err, "unable to obtain a connection to the database")
	}
	pos, err := conn.MasterPosition()
	if err != nil {
		return false, vterrors.Wrap(err, "unable to obtain master position")
	}
	flavor := pos.GTIDSet.Flavor()
	logger.Infof("Detected MySQL flavor: %v", flavor)

	backupFileName := be.backupFileName()
	numStripes := int(*xtrabackupStripes)

	// Perform backups in a separate function, so deferred calls to Close() are
	// all done before we continue to write the MANIFEST. This ensures that we
	// do not write the MANIFEST unless all files were closed successfully,
	// maintaining the contract that a MANIFEST file should only exist if the
	// backup was created successfully.
	logger.Infof("Starting backup with %v stripe(s)", numStripes)
	replicationPosition, err := be.backupFiles(ctx, cnf, logger, bh, backupFileName, numStripes, flavor)
	if err != nil {
		return false, err
	}

	// open the MANIFEST
	logger.Infof("Writing backup MANIFEST")
	mwc, err := bh.AddFile(ctx, backupManifestFileName, 0)
	if err != nil {
		return false, vterrors.Wrapf(err, "cannot add %v to backup", backupManifestFileName)
	}
	defer closeFile(mwc, backupManifestFileName, logger, &finalErr)

	// JSON-encode and write the MANIFEST
	bm := &xtraBackupManifest{
		// Common base fields
		BackupManifest: BackupManifest{
			BackupMethod: xtrabackupEngineName,
			Position:     replicationPosition,
			FinishedTime: time.Now().UTC().Format(time.RFC3339),
		},

		// XtraBackup-specific fields
		FileName:        backupFileName,
		StreamMode:      *xtrabackupStreamMode,
		SkipCompress:    !*backupStorageCompress,
		Params:          *xtrabackupBackupFlags,
		NumStripes:      int32(numStripes),
		StripeBlockSize: int32(*xtrabackupStripeBlockSize),
	}

	data, err := json.MarshalIndent(bm, "", "  ")
	if err != nil {
		return false, vterrors.Wrapf(err, "cannot JSON encode %v", backupManifestFileName)
	}
	if _, err := mwc.Write([]byte(data)); err != nil {
		return false, vterrors.Wrapf(err, "cannot write %v", backupManifestFileName)
	}

	logger.Infof("Backup completed")
	return true, nil
}

func (be *XtrabackupEngine) backupFiles(ctx context.Context, cnf *Mycnf, logger logutil.Logger, bh backupstorage.BackupHandle, backupFileName string, numStripes int, flavor string) (replicationPosition mysql.Position, finalErr error) {
	backupProgram := path.Join(*xtrabackupEnginePath, xtrabackupBinaryName)

	flagsToExec := []string{"--defaults-file=" + cnf.path,
		"--backup",
		"--socket=" + cnf.SocketFile,
		"--slave-info",
		"--user=" + *xtrabackupUser,
		"--target-dir=" + cnf.TmpDir,
	}
	if *xtrabackupStreamMode != "" {
		flagsToExec = append(flagsToExec, "--stream="+*xtrabackupStreamMode)
	}
	if *xtrabackupBackupFlags != "" {
		flagsToExec = append(flagsToExec, strings.Fields(*xtrabackupBackupFlags)...)
	}

	// Create a cancellable Context for calls to bh.AddFile().
	// This allows us to decide later if we need to cancel an attempt to Close()
	// the file returned from AddFile(), since Close() itself does not accept a
	// Context value. We can't use a context.WithTimeout() here because that
	// would impose a timeout that starts counting right now, so it would
	// include the time spent uploading the file content. We only want to impose
	// a timeout on the final Close() step.
	addFilesCtx, cancelAddFiles := context.WithCancel(ctx)
	defer cancelAddFiles()
	destFiles, err := addStripeFiles(addFilesCtx, bh, backupFileName, numStripes, logger)
	if err != nil {
		return replicationPosition, vterrors.Wrapf(err, "cannot create backup file %v", backupFileName)
	}
	defer func() {
		// Impose a timeout on the process of closing files.
		go func() {
			timer := time.NewTimer(closeTimeout)

			select {
			case <-addFilesCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
				logger.Errorf("Timed out waiting for Close() on backup file to complete")
				// Cancelling the Context that was originally passed to bh.AddFile()
				// should hopefully cause Close() calls on the file that AddFile()
				// returned to abort. If the underlying implementation doesn't
				// respect cancellation of the AddFile() Context while inside
				// Close(), then we just hang because it's unsafe to return and
				// leave Close() running indefinitely in the background.
				cancelAddFiles()
			}
		}()

		filename := backupFileName
		for i, file := range destFiles {
			if numStripes > 1 {
				filename = stripeFileName(backupFileName, i)
			}
			closeFile(file, filename, logger, &finalErr)
		}
	}()

	backupCmd := exec.CommandContext(ctx, backupProgram, flagsToExec...)
	backupOut, err := backupCmd.StdoutPipe()
	if err != nil {
		return replicationPosition, vterrors.Wrap(err, "cannot create stdout pipe")
	}
	backupErr, err := backupCmd.StderrPipe()
	if err != nil {
		return replicationPosition, vterrors.Wrap(err, "cannot create stderr pipe")
	}

	destWriters := []io.Writer{}
	destBuffers := []*bufio.Writer{}
	destCompressors := []*pgzip.Writer{}
	for _, file := range destFiles {
		buffer := bufio.NewWriterSize(file, writerBufferSize)
		destBuffers = append(destBuffers, buffer)
		writer := io.Writer(buffer)

		// Create the gzip compression pipe, if necessary.
		if *backupStorageCompress {
			compressor, err := pgzip.NewWriterLevel(writer, pgzip.BestSpeed)
			if err != nil {
				return replicationPosition, vterrors.Wrap(err, "cannot create gzip compressor")
			}
			compressor.SetConcurrency(*backupCompressBlockSize, *backupCompressBlocks)
			writer = compressor
			destCompressors = append(destCompressors, compressor)
		}

		destWriters = append(destWriters, writer)
	}

	if err = backupCmd.Start(); err != nil {
		return replicationPosition, vterrors.Wrap(err, "unable to start backup")
	}

	// Read stderr in the background, so we can log progress as xtrabackup runs.
	// Also save important lines of the output so we can parse it later to find
	// the replication position. Note that if we don't read stderr as we go, the
	// xtrabackup process gets blocked when the write buffer fills up.
	stderrBuilder := &strings.Builder{}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)

		scanner := bufio.NewScanner(backupErr)
		capture := false
		for scanner.Scan() {
			line := scanner.Text()
			logger.Infof("xtrabackup stderr: %s", line)

			// Wait until we see the first line of the binlog position.
			// Then capture all subsequent lines. We need multiple lines since
			// the value we're looking for has newlines in it.
			if !capture {
				if !strings.Contains(line, "MySQL binlog position") {
					continue
				}
				capture = true
			}
			fmt.Fprintln(stderrBuilder, line)
		}
		if err := scanner.Err(); err != nil {
			logger.Errorf("error reading from xtrabackup stderr: %v", err)
		}
	}()

	// Copy from the stream output to destination file (optional gzip)
	blockSize := int64(*xtrabackupStripeBlockSize)
	if blockSize < 1024 {
		// Enforce minimum block size.
		blockSize = 1024
	}
	if _, err := copyToStripes(destWriters, backupOut, blockSize); err != nil {
		return replicationPosition, vterrors.Wrap(err, "cannot copy output from xtrabackup command")
	}

	// Close compressor to flush it. After that all data is sent to the buffer.
	for _, compressor := range destCompressors {
		if err := compressor.Close(); err != nil {
			return replicationPosition, vterrors.Wrap(err, "cannot close gzip compressor")
		}
	}

	// Flush the buffer to finish writing on destination.
	for _, buffer := range destBuffers {
		if err = buffer.Flush(); err != nil {
			return replicationPosition, vterrors.Wrapf(err, "cannot flush destination: %v", backupFileName)
		}
	}

	// Wait for stderr scanner to stop.
	<-stderrDone
	// Get the final (filtered) stderr output.
	sterrOutput := stderrBuilder.String()

	if err := backupCmd.Wait(); err != nil {
		return replicationPosition, vterrors.Wrap(err, "xtrabackup failed with error")
	}

	replicationPosition, rerr := findReplicationPosition(sterrOutput, flavor, logger)
	if rerr != nil {
		return replicationPosition, vterrors.Wrap(rerr, "backup failed trying to find replication position")
	}

	return replicationPosition, nil
}

// ExecuteRestore restores from a backup. Any error is returned.
func (be *XtrabackupEngine) ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (mysql.Position, error) {

	cnf := params.Cnf
	mysqld := params.Mysqld
	logger := params.Logger

	zeroPosition := mysql.Position{}
	var bm xtraBackupManifest

	if err := getBackupManifestInto(ctx, bh, &bm); err != nil {
		return zeroPosition, err
	}

	// mark restore as in progress
	if err := createStateFile(cnf); err != nil {
		return zeroPosition, err
	}

	if err := prepareToRestore(ctx, cnf, mysqld, logger); err != nil {
		return zeroPosition, err
	}

	// copy / extract files
	logger.Infof("Restore: Extracting files from %v", bm.FileName)

	if err := be.restoreFromBackup(ctx, cnf, bh, bm, logger); err != nil {
		// don't delete the file here because that is how we detect an interrupted restore
		return zeroPosition, err
	}
	// now find the slave position and return that
	logger.Infof("Restore: returning replication position %v", bm.Position)
	return bm.Position, nil
}

func (be *XtrabackupEngine) restoreFromBackup(ctx context.Context, cnf *Mycnf, bh backupstorage.BackupHandle, bm xtraBackupManifest, logger logutil.Logger) error {
	// first download the file into a tmp dir
	// and extract all the files

	tempDir := fmt.Sprintf("%v/%v", cnf.TmpDir, time.Now().UTC().Format("xtrabackup-2006-01-02.150405"))
	// create tempDir
	if err := os.MkdirAll(tempDir, os.ModePerm); err != nil {
		return err
	}

	if err := be.extractFiles(ctx, logger, bh, bm, tempDir); err != nil {
		logger.Errorf("error extracting backup files: %v", err)
		return err
	}

	// copy / extract files
	logger.Infof("Restore: Preparing the extracted files")
	// prepare the backup
	restoreProgram := path.Join(*xtrabackupEnginePath, xtrabackupBinaryName)
	flagsToExec := []string{"--defaults-file=" + cnf.path,
		"--prepare",
		"--target-dir=" + tempDir,
	}
	prepareCmd := exec.CommandContext(ctx, restoreProgram, flagsToExec...)
	prepareOut, err := prepareCmd.StdoutPipe()
	if err != nil {
		return vterrors.Wrap(err, "cannot create stdout pipe")
	}
	prepareErr, err := prepareCmd.StderrPipe()
	if err != nil {
		return vterrors.Wrap(err, "cannot create stderr pipe")
	}
	if err := prepareCmd.Start(); err != nil {
		return vterrors.Wrap(err, "can't start prepare step")
	}

	// Read stdout/stderr in the background and send each line to the logger.
	prepareWg := &sync.WaitGroup{}
	prepareWg.Add(2)
	go scanLinesToLogger("prepare stdout", prepareOut, logger, prepareWg.Done)
	go scanLinesToLogger("prepare stderr", prepareErr, logger, prepareWg.Done)
	prepareWg.Wait()

	// Get exit status.
	if err := prepareCmd.Wait(); err != nil {
		return vterrors.Wrap(err, "prepare step failed")
	}

	// then move-back
	logger.Infof("Restore: Move extracted and prepared files to final locations")

	flagsToExec = []string{"--defaults-file=" + cnf.path,
		"--move-back",
		"--target-dir=" + tempDir,
	}
	movebackCmd := exec.CommandContext(ctx, restoreProgram, flagsToExec...)
	movebackOut, err := movebackCmd.StdoutPipe()
	if err != nil {
		return vterrors.Wrap(err, "cannot create stdout pipe")
	}
	movebackErr, err := movebackCmd.StderrPipe()
	if err != nil {
		return vterrors.Wrap(err, "cannot create stderr pipe")
	}
	if err := movebackCmd.Start(); err != nil {
		return vterrors.Wrap(err, "can't start move-back step")
	}

	// Read stdout/stderr in the background and send each line to the logger.
	movebackWg := &sync.WaitGroup{}
	movebackWg.Add(2)
	go scanLinesToLogger("move-back stdout", movebackOut, logger, movebackWg.Done)
	go scanLinesToLogger("move-back stderr", movebackErr, logger, movebackWg.Done)
	movebackWg.Wait()

	// Get exit status.
	if err := movebackCmd.Wait(); err != nil {
		return vterrors.Wrap(err, "move-back step failed")
	}

	return nil
}

// restoreFile extracts all the files from the backup archive
func (be *XtrabackupEngine) extractFiles(ctx context.Context, logger logutil.Logger, bh backupstorage.BackupHandle, bm xtraBackupManifest, tempDir string) error {
	// Pull details from the MANIFEST where available, so we can still restore
	// backups taken with different flags. Some fields were not always present,
	// so if necessary we default to the flag values.
	compressed := !bm.SkipCompress
	streamMode := bm.StreamMode
	if streamMode == "" {
		streamMode = *xtrabackupStreamMode
	}
	baseFileName := bm.FileName
	if baseFileName == "" {
		baseFileName = be.backupFileName()
	}

	// Open the source files for reading.
	srcFiles, err := readStripeFiles(ctx, bh, baseFileName, int(bm.NumStripes), logger)
	if err != nil {
		return vterrors.Wrapf(err, "cannot open backup file %v", baseFileName)
	}
	defer func() {
		for _, file := range srcFiles {
			file.Close()
		}
	}()

	srcReaders := []io.Reader{}
	srcDecompressors := []*pgzip.Reader{}
	for _, file := range srcFiles {
		reader := io.Reader(file)

		// Create the decompressor if needed.
		if compressed {
			decompressor, err := pgzip.NewReader(reader)
			if err != nil {
				return vterrors.Wrap(err, "can't create gzip decompressor")
			}
			srcDecompressors = append(srcDecompressors, decompressor)
			reader = decompressor
		}

		srcReaders = append(srcReaders, reader)
	}
	defer func() {
		for _, decompressor := range srcDecompressors {
			if cerr := decompressor.Close(); cerr != nil {
				logger.Errorf("failed to close gzip decompressor: %v", cerr)
			}
		}
	}()

	reader := stripeReader(srcReaders, int64(bm.StripeBlockSize))

	switch streamMode {
	case streamModeTar:
		// now extract the files by running tar
		// error if we can't find tar
		flagsToExec := []string{"-C", tempDir, "-xiv"}
		tarCmd := exec.CommandContext(ctx, "tar", flagsToExec...)
		logger.Infof("Executing tar cmd with flags %v", flagsToExec)
		tarCmd.Stdin = reader
		tarOut, err := tarCmd.StdoutPipe()
		if err != nil {
			return vterrors.Wrap(err, "cannot create stdout pipe")
		}
		tarErr, err := tarCmd.StderrPipe()
		if err != nil {
			return vterrors.Wrap(err, "cannot create stderr pipe")
		}
		if err := tarCmd.Start(); err != nil {
			return vterrors.Wrap(err, "can't start tar")
		}

		// Read stdout/stderr in the background and send each line to the logger.
		tarWg := &sync.WaitGroup{}
		tarWg.Add(2)
		go scanLinesToLogger("tar stdout", tarOut, logger, tarWg.Done)
		go scanLinesToLogger("tar stderr", tarErr, logger, tarWg.Done)
		tarWg.Wait()

		// Get exit status.
		if err := tarCmd.Wait(); err != nil {
			return vterrors.Wrap(err, "tar failed")
		}

	case xbstream:
		// now extract the files by running xbstream
		xbstreamProgram := xbstream
		flagsToExec := []string{}
		if *xbstreamRestoreFlags != "" {
			flagsToExec = append(flagsToExec, strings.Fields(*xbstreamRestoreFlags)...)
		}
		flagsToExec = append(flagsToExec, "-C", tempDir, "-xv")
		xbstreamCmd := exec.CommandContext(ctx, xbstreamProgram, flagsToExec...)
		logger.Infof("Executing xbstream cmd: %v %v", xbstreamProgram, flagsToExec)
		xbstreamCmd.Stdin = reader
		xbstreamOut, err := xbstreamCmd.StdoutPipe()
		if err != nil {
			return vterrors.Wrap(err, "cannot create stdout pipe")
		}
		xbstreamErr, err := xbstreamCmd.StderrPipe()
		if err != nil {
			return vterrors.Wrap(err, "cannot create stderr pipe")
		}
		if err := xbstreamCmd.Start(); err != nil {
			return vterrors.Wrap(err, "can't start xbstream")
		}

		// Read stdout/stderr in the background and send each line to the logger.
		xbstreamWg := &sync.WaitGroup{}
		xbstreamWg.Add(2)
		go scanLinesToLogger("xbstream stdout", xbstreamOut, logger, xbstreamWg.Done)
		go scanLinesToLogger("xbstream stderr", xbstreamErr, logger, xbstreamWg.Done)
		xbstreamWg.Wait()

		// Get exit status.
		if err := xbstreamCmd.Wait(); err != nil {
			return vterrors.Wrap(err, "xbstream failed")
		}
	default:
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%v is not a valid value for xtrabackup_stream_mode, supported modes are tar and xbstream", streamMode)
	}
	return nil
}

var xtrabackupReplicationPositionRegexp = regexp.MustCompile(`GTID of the last change '([^']*)'`)

func findReplicationPosition(input, flavor string, logger logutil.Logger) (mysql.Position, error) {
	match := xtrabackupReplicationPositionRegexp.FindStringSubmatch(input)
	if match == nil || len(match) != 2 {
		return mysql.Position{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "couldn't find replication position in xtrabackup stderr output")
	}
	position := match[1]
	// Remove all spaces, tabs, and newlines.
	position = strings.Replace(position, " ", "", -1)
	position = strings.Replace(position, "\t", "", -1)
	position = strings.Replace(position, "\n", "", -1)
	logger.Infof("Found position: %v", position)
	if position == "" {
		return mysql.Position{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "empty replication position from xtrabackup")
	}

	// flavor is required to parse a string into a mysql.Position
	replicationPosition, err := mysql.ParsePosition(flavor, position)
	if err != nil {
		return mysql.Position{}, vterrors.Wrapf(err, "can't parse replication position from xtrabackup: %v", position)
	}
	return replicationPosition, nil
}

// scanLinesToLogger scans full lines from the given Reader and sends them to
// the given Logger until EOF.
func scanLinesToLogger(prefix string, reader io.Reader, logger logutil.Logger, doneFunc func()) {
	defer doneFunc()

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		logger.Infof("%s: %s", prefix, line)
	}
	if err := scanner.Err(); err != nil {
		// This is usually run in a background goroutine, so there's no point
		// returning an error. Just log it.
		logger.Warningf("error scanning lines from %s: %v", prefix, err)
	}
}

func stripeFileName(baseFileName string, index int) string {
	return fmt.Sprintf("%s-%03d", baseFileName, index)
}

func addStripeFiles(ctx context.Context, backupHandle backupstorage.BackupHandle, baseFileName string, numStripes int, logger logutil.Logger) ([]io.WriteCloser, error) {
	if numStripes <= 1 {
		// No striping.
		file, err := backupHandle.AddFile(ctx, baseFileName, 0)
		return []io.WriteCloser{file}, err
	}

	files := []io.WriteCloser{}
	for i := 0; i < numStripes; i++ {
		filename := stripeFileName(baseFileName, i)
		logger.Infof("Opening backup stripe file %v", filename)
		file, err := backupHandle.AddFile(ctx, filename, 0)
		if err != nil {
			// Close any files we already opened and clear them from the result.
			for _, file := range files {
				if err := file.Close(); err != nil {
					logger.Warningf("error closing backup stripe file: %v", err)
				}
			}
			return nil, err
		}
		files = append(files, file)
	}

	return files, nil
}

func readStripeFiles(ctx context.Context, backupHandle backupstorage.BackupHandle, baseFileName string, numStripes int, logger logutil.Logger) ([]io.ReadCloser, error) {
	if numStripes <= 1 {
		// No striping.
		file, err := backupHandle.ReadFile(ctx, baseFileName)
		return []io.ReadCloser{file}, err
	}

	files := []io.ReadCloser{}
	for i := 0; i < numStripes; i++ {
		file, err := backupHandle.ReadFile(ctx, stripeFileName(baseFileName, i))
		if err != nil {
			// Close any files we already opened and clear them from the result.
			for _, file := range files {
				if err := file.Close(); err != nil {
					logger.Warningf("error closing backup stripe file: %v", err)
				}
			}
			return nil, err
		}
		files = append(files, file)
	}

	return files, nil
}

func copyToStripes(writers []io.Writer, reader io.Reader, blockSize int64) (written int64, err error) {
	if len(writers) == 1 {
		// Not striped.
		return io.Copy(writers[0], reader)
	}

	// Read blocks from source and round-robin them to destination writers.
	// Since we put a buffer in front of the destination file, and pgzip has its
	// own buffer as well, we are writing into a buffer either way (whether a
	// compressor is in the chain or not). That means these writes should not
	// block often, so we shouldn't need separate goroutines here.
	destIndex := 0
	for {
		// Copy blockSize bytes to this writer before rotating to the next one.
		// The only acceptable reason for copying less than blockSize bytes is EOF.
		n, err := io.CopyN(writers[destIndex], reader, blockSize)
		written += n
		if err == io.EOF {
			// We're done.
			return written, nil
		}
		if err != nil {
			// If we failed to copy exactly blockSize bytes for any reason other
			// than EOF, we must abort.
			return written, err
		}

		// Rotate to the next writer.
		destIndex++
		if destIndex == len(writers) {
			destIndex = 0
		}
	}
}

func stripeReader(readers []io.Reader, blockSize int64) io.Reader {
	if len(readers) == 1 {
		// No striping.
		return readers[0]
	}

	// Make a pipe to convert our overall Writer into a Reader.
	// We will launch a goroutine to write to the write half of the pipe,
	// and return the read half to the caller.
	reader, writer := io.Pipe()

	go func() {
		// Read blocks from each source in round-robin and send them to the pipe.
		// When using pgzip, there is already a read-ahead goroutine for every
		// source, so we don't need to launch one for each source.
		// TODO: See if we need to add read-ahead goroutines for the case when
		//   compression is not enabled in order to get any benefit to restore
		//   parallelism from data striping.
		srcIndex := 0
		for {
			// Copy blockSize bytes from this reader before rotating to the next one.
			// The only acceptable reason for copying less than blockSize bytes is EOF.
			n, err := io.CopyN(writer, readers[srcIndex], blockSize)
			if err != nil {
				// If we failed to copy exactly blockSize bytes for any
				// reason other than EOF, we must abort.
				if err != io.EOF {
					writer.CloseWithError(err)
					return
				}

				// If we hit EOF after copying less than the blockSize from
				// this reader, we must be done.
				if n < blockSize {
					// Close the write half so the read half gets EOF.
					writer.Close()
					return
				}
				// If we hit EOF after copying exactly blockSize bytes, then we
				// need to keep checking the rest of the stripes until one of
				// them returns EOF with n < blockSize.
			}

			// Rotate to the next writer.
			srcIndex++
			if srcIndex == len(readers) {
				srcIndex = 0
			}
		}
	}()

	return reader
}

// ShouldDrainForBackup satisfies the BackupEngine interface
// xtrabackup can run while tablet is serving, hence false
func (be *XtrabackupEngine) ShouldDrainForBackup() bool {
	return false
}

func init() {
	BackupRestoreEngineMap[xtrabackupEngineName] = &XtrabackupEngine{}
}
