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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/ioutil"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
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
	xtrabackupEnginePath string
	// flags to pass through to backup phase
	xtrabackupBackupFlags string
	// flags to pass through to prepare phase of restore
	xtrabackupPrepareFlags string
	// flags to pass through to extract phase of restore
	xbstreamRestoreFlags string
	// streaming mode
	xtrabackupStreamMode = "tar"
	xtrabackupUser       string
	// striping mode
	xtrabackupStripes         uint
	xtrabackupStripeBlockSize = uint(102400)
)

const (
	streamModeTar        = "tar"
	writerBufferSize     = 2 * 1024 * 1024 /*2 MiB*/
	xtrabackupBinaryName = "xtrabackup"
	xtrabackupEngineName = "xtrabackup"
	xbstream             = "xbstream"
)

// xtraBackupManifest represents a backup.
// It stores the name of the backup file, the replication position,
// whether the backup is compressed using gzip, and any extra
// command line parameters used while invoking it.
type xtraBackupManifest struct {
	// BackupManifest is an anonymous embedding of the base manifest struct.
	BackupManifest
	// CompressionEngine stores which compression engine was originally provided
	// to compress the files. Please note that if user has provided externalCompressorCmd
	// then it will contain value 'external'. This field is used during restore routine to
	// get a hint about what kind of compression was used.
	CompressionEngine string `json:",omitempty"`
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

	// When CompressionEngine is "external", ExternalDecompressor may be
	// consulted for the external decompressor command.
	//
	// When taking a backup with --compression-engine=external,
	// ExternalDecompressor will be set to the value of
	// --manifest-external-decompressor, if set, or else left as an empty
	// string.
	//
	// When restoring from a backup with CompressionEngine "external",
	// --external-decompressor will be consulted first and, if that is not set,
	// ExternalDecompressor will be used. If neither are set, the restore will
	// abort.
	ExternalDecompressor string
}

func init() {
	for _, cmd := range []string{"vtcombo", "vttablet", "vtbackup", "vttestserver", "vtctldclient"} {
		servenv.OnParseFor(cmd, registerXtraBackupEngineFlags)
	}
}

func registerXtraBackupEngineFlags(fs *pflag.FlagSet) {
	fs.StringVar(&xtrabackupEnginePath, "xtrabackup_root_path", xtrabackupEnginePath, "Directory location of the xtrabackup and xbstream executables, e.g., /usr/bin")
	fs.StringVar(&xtrabackupBackupFlags, "xtrabackup_backup_flags", xtrabackupBackupFlags, "Flags to pass to backup command. These should be space separated and will be added to the end of the command")
	fs.StringVar(&xtrabackupPrepareFlags, "xtrabackup_prepare_flags", xtrabackupPrepareFlags, "Flags to pass to prepare command. These should be space separated and will be added to the end of the command")
	fs.StringVar(&xbstreamRestoreFlags, "xbstream_restore_flags", xbstreamRestoreFlags, "Flags to pass to xbstream command during restore. These should be space separated and will be added to the end of the command. These need to match the ones used for backup e.g. --compress / --decompress, --encrypt / --decrypt")
	fs.StringVar(&xtrabackupStreamMode, "xtrabackup_stream_mode", xtrabackupStreamMode, "Which mode to use if streaming, valid values are tar and xbstream. Please note that tar is not supported in XtraBackup 8.0")
	fs.StringVar(&xtrabackupUser, "xtrabackup_user", xtrabackupUser, "User that xtrabackup will use to connect to the database server. This user must have all necessary privileges. For details, please refer to xtrabackup documentation.")
	fs.UintVar(&xtrabackupStripes, "xtrabackup_stripes", xtrabackupStripes, "If greater than 0, use data striping across this many destination files to parallelize data transfer and decompression")
	fs.UintVar(&xtrabackupStripeBlockSize, "xtrabackup_stripe_block_size", xtrabackupStripeBlockSize, "Size in bytes of each block that gets sent to a given stripe before rotating to the next stripe")
}

func (be *XtrabackupEngine) backupFileName() string {
	fileName := "backup"
	if xtrabackupStreamMode != "" {
		fileName += "."
		fileName += xtrabackupStreamMode
	}
	if backupStorageCompress {
		if ExternalDecompressorCmd != "" {
			fileName += ExternalCompressorExt
		} else {
			if ext, err := getExtensionFromEngine(CompressionEngineName); err != nil {
				// there is a check for this, but just in case that fails, we set a extension to the file
				fileName += ".unknown"
			} else {
				fileName += ext
			}
		}
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

// ExecuteBackup runs a backup based on given params. This could be a full or incremental backup.
// The function returns a BackupResult that indicates the usability of the backup, and an overall error.
func (be *XtrabackupEngine) ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (BackupResult, error) {
	params.Logger.Infof("Executing Backup at %v for keyspace/shard %v/%v on tablet %v, concurrency: %v, compress: %v, incrementalFromPos: %v",
		params.BackupTime, params.Keyspace, params.Shard, params.TabletAlias, params.Concurrency, backupStorageCompress, params.IncrementalFromPos)

	return be.executeFullBackup(ctx, params, bh)
}

// executeFullBackup returns a BackupResult that indicates the usability of the backup,
// and an overall error.
func (be *XtrabackupEngine) executeFullBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (backupResult BackupResult, finalErr error) {
	if params.IncrementalFromPos != "" {
		return BackupUnusable, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "incremental backups not supported in xtrabackup engine.")
	}
	if xtrabackupUser == "" {
		return BackupUnusable, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "xtrabackupUser must be specified.")
	}

	// an extension is required when using an external compressor
	if backupStorageCompress && ExternalCompressorCmd != "" && ExternalCompressorExt == "" {
		return BackupUnusable, vterrors.New(vtrpc.Code_INVALID_ARGUMENT,
			"flag --external-compressor-extension not provided when using an external compressor")
	}

	// use a mysql connection to detect flavor at runtime
	conn, err := params.Mysqld.GetDbaConnection(ctx)
	if conn != nil && err == nil {
		defer conn.Close()
	}

	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "unable to obtain a connection to the database")
	}
	pos, err := conn.PrimaryPosition()
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "unable to obtain primary position")
	}
	serverUUID, err := conn.GetServerUUID()
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get server uuid")
	}

	mysqlVersion, err := params.Mysqld.GetVersionString(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get MySQL version")
	}

	flavor := pos.GTIDSet.Flavor()
	params.Logger.Infof("Detected MySQL flavor: %v", flavor)

	backupFileName := be.backupFileName()
	params.Logger.Infof("backup file name: %s", backupFileName)
	numStripes := int(xtrabackupStripes)

	// Perform backups in a separate function, so deferred calls to Close() are
	// all done before we continue to write the MANIFEST. This ensures that we
	// do not write the MANIFEST unless all files were closed successfully,
	// maintaining the contract that a MANIFEST file should only exist if the
	// backup was created successfully.
	params.Logger.Infof("Starting backup with %v stripe(s)", numStripes)
	replicationPosition, err := be.backupFiles(ctx, params, bh, backupFileName, numStripes, flavor)
	if err != nil {
		return BackupUnusable, err
	}

	// open the MANIFEST
	params.Logger.Infof("Writing backup MANIFEST")
	mwc, err := bh.AddFile(ctx, backupManifestFileName, backupstorage.FileSizeUnknown)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot add %v to backup", backupManifestFileName)
	}
	defer closeFile(mwc, backupManifestFileName, params.Logger, &finalErr)

	// JSON-encode and write the MANIFEST
	bm := &xtraBackupManifest{
		// Common base fields
		BackupManifest: BackupManifest{
			BackupName:     bh.Name(),
			BackupMethod:   xtrabackupEngineName,
			Position:       replicationPosition,
			PurgedPosition: replicationPosition,
			ServerUUID:     serverUUID,
			TabletAlias:    params.TabletAlias,
			Keyspace:       params.Keyspace,
			Shard:          params.Shard,
			BackupTime:     FormatRFC3339(params.BackupTime.UTC()),
			FinishedTime:   FormatRFC3339(time.Now().UTC()),
			MySQLVersion:   mysqlVersion,
			// xtrabackup backups are always created such that they
			// are safe to use for upgrades later on.
			UpgradeSafe: true,
		},

		// XtraBackup-specific fields
		FileName:        backupFileName,
		StreamMode:      xtrabackupStreamMode,
		SkipCompress:    !backupStorageCompress,
		Params:          xtrabackupBackupFlags,
		NumStripes:      int32(numStripes),
		StripeBlockSize: int32(xtrabackupStripeBlockSize),
		// builtin specific field
		CompressionEngine:    CompressionEngineName,
		ExternalDecompressor: ManifestExternalDecompressorCmd,
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

func (be *XtrabackupEngine) backupFiles(
	ctx context.Context,
	params BackupParams,
	bh backupstorage.BackupHandle,
	backupFileName string,
	numStripes int,
	flavor string,
) (replicationPosition replication.Position, finalErr error) {

	backupProgram := path.Join(xtrabackupEnginePath, xtrabackupBinaryName)
	flagsToExec := []string{"--defaults-file=" + params.Cnf.Path,
		"--backup",
		"--socket=" + params.Cnf.SocketFile,
		"--slave-info",
		"--user=" + xtrabackupUser,
		"--target-dir=" + params.Cnf.TmpDir,
	}
	if xtrabackupStreamMode != "" {
		flagsToExec = append(flagsToExec, "--stream="+xtrabackupStreamMode)
	}
	if xtrabackupBackupFlags != "" {
		flagsToExec = append(flagsToExec, strings.Fields(xtrabackupBackupFlags)...)
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
	destFiles, err := addStripeFiles(addFilesCtx, params, bh, backupFileName, numStripes)
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
				params.Logger.Errorf("Timed out waiting for Close() on backup file to complete")
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
			closeFile(file, filename, params.Logger, &finalErr)
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
	destCompressors := []io.Closer{}
	for _, file := range destFiles {
		buffer := bufio.NewWriterSize(file, writerBufferSize)
		destBuffers = append(destBuffers, buffer)
		writer := io.Writer(buffer)

		// Create the gzip compression pipe, if necessary.
		if backupStorageCompress {
			var compressor io.WriteCloser

			if ExternalCompressorCmd != "" {
				compressor, err = newExternalCompressor(ctx, ExternalCompressorCmd, writer, params.Logger)
			} else {
				compressor, err = newBuiltinCompressor(CompressionEngineName, writer, params.Logger)
			}
			if err != nil {
				return replicationPosition, vterrors.Wrap(err, "can't create compressor")
			}

			writer = compressor
			destCompressors = append(destCompressors, ioutil.NewTimeoutCloser(ctx, compressor, closeTimeout))
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
	posBuilder := &strings.Builder{}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)

		scanner := bufio.NewScanner(backupErr)
		capture := false
		for scanner.Scan() {
			line := scanner.Text()
			params.Logger.Infof("xtrabackup stderr: %s", line)

			// Wait until we see the first line of the binlog position.
			// Then capture all subsequent lines. We need multiple lines since
			// the value we're looking for has newlines in it.
			if !capture {
				if !strings.Contains(line, "MySQL binlog position") {
					continue
				}
				capture = true
			}
			fmt.Fprintln(posBuilder, line)
		}
		if err := scanner.Err(); err != nil {
			params.Logger.Errorf("error reading from xtrabackup stderr: %v", err)
		}
	}()

	// Copy from the stream output to destination file (optional gzip)
	blockSize := int64(xtrabackupStripeBlockSize)
	if blockSize < 1024 {
		// Enforce minimum block size.
		blockSize = 1024
	}
	// Add a buffer in front of the raw stdout pipe so io.CopyN() can use the
	// buffered reader's WriteTo() method instead of allocating a new buffer
	// every time.
	backupOutBuf := bufio.NewReaderSize(backupOut, int(blockSize))
	if _, err := copyToStripes(destWriters, backupOutBuf, blockSize); err != nil {
		return replicationPosition, vterrors.Wrap(err, "cannot copy output from xtrabackup command")
	}

	// Close compressor to flush it. After that all data is sent to the buffer.
	for _, compressor := range destCompressors {
		if err := compressor.Close(); err != nil {
			return replicationPosition, vterrors.Wrap(err, "cannot close compressor")
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
		return replicationPosition, vterrors.Wrap(err, fmt.Sprintf("xtrabackup failed with error. Output=%s", sterrOutput))
	}

	posOutput := posBuilder.String()
	replicationPosition, rerr := findReplicationPosition(posOutput, flavor, params.Logger)
	if rerr != nil {
		return replicationPosition, vterrors.Wrap(rerr, "backup failed trying to find replication position")
	}

	return replicationPosition, nil
}

// ExecuteRestore restores from a backup. Any error is returned.
func (be *XtrabackupEngine) ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (*BackupManifest, error) {

	var bm xtraBackupManifest

	if err := getBackupManifestInto(ctx, bh, &bm); err != nil {
		return nil, err
	}

	// mark restore as in progress
	if err := createStateFile(params.Cnf); err != nil {
		return nil, err
	}

	if err := prepareToRestore(ctx, params.Cnf, params.Mysqld, params.Logger, params.MysqlShutdownTimeout); err != nil {
		return nil, err
	}

	// copy / extract files
	params.Logger.Infof("Restore: Extracting files from %v", bm.FileName)

	if err := be.restoreFromBackup(ctx, params.Cnf, bh, bm, params.Logger); err != nil {
		// don't delete the file here because that is how we detect an interrupted restore
		return nil, err
	}
	// now find the replication position and return that
	params.Logger.Infof("Restore: returning replication position %v", bm.Position)
	return &bm.BackupManifest, nil
}

func (be *XtrabackupEngine) restoreFromBackup(ctx context.Context, cnf *Mycnf, bh backupstorage.BackupHandle, bm xtraBackupManifest, logger logutil.Logger) error {
	// first download the file into a tmp dir
	// and extract all the files
	tempDir := fmt.Sprintf("%v/%v", cnf.TmpDir, time.Now().UTC().Format("xtrabackup-2006-01-02.150405"))
	// create tempDir
	if err := os.MkdirAll(tempDir, os.ModePerm); err != nil {
		return err
	}
	// delete tempDir once we are done
	defer func(dir string, l logutil.Logger) {
		err := os.RemoveAll(dir)
		if err != nil {
			l.Errorf("error deleting tempDir(%v): %v", dir, err)
		}
	}(tempDir, logger)

	// For optimization, we are replacing pargzip with pgzip, so newBuiltinDecompressor doesn't have to compare and print warning for every file
	// since newBuiltinDecompressor is helper method and does not hold any state, it was hard to do it in that method itself.
	if bm.CompressionEngine == PargzipCompressor {
		logger.Warningf(`engine "pargzip" doesn't support decompression, using "pgzip" instead`)
		bm.CompressionEngine = PgzipCompressor
		defer func() {
			bm.CompressionEngine = PargzipCompressor
		}()
	}

	if err := be.extractFiles(ctx, logger, bh, bm, tempDir); err != nil {
		logger.Errorf("error extracting backup files: %v", err)
		return err
	}

	// copy / extract files
	logger.Infof("Restore: Preparing the extracted files")
	// prepare the backup
	restoreProgram := path.Join(xtrabackupEnginePath, xtrabackupBinaryName)
	flagsToExec := []string{"--defaults-file=" + cnf.Path,
		"--prepare",
		"--target-dir=" + tempDir,
	}
	if xtrabackupPrepareFlags != "" {
		flagsToExec = append(flagsToExec, strings.Fields(xtrabackupPrepareFlags)...)
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

	flagsToExec = []string{"--defaults-file=" + cnf.Path,
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
		streamMode = xtrabackupStreamMode
	}
	baseFileName := bm.FileName
	if baseFileName == "" {
		baseFileName = be.backupFileName()
	}

	logger.Infof("backup file name: %s", baseFileName)
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
	srcDecompressors := []io.Closer{}
	for _, file := range srcFiles {
		reader := io.Reader(file)

		// Create the decompressor if needed.
		if compressed {
			var decompressor io.ReadCloser
			var deCompressionEngine = bm.CompressionEngine
			if deCompressionEngine == "" {
				// For backward compatibility. Incase if Manifest is from N-1 binary
				// then we assign the default value of compressionEngine.
				deCompressionEngine = PgzipCompressor
			}
			externalDecompressorCmd := ExternalDecompressorCmd
			if externalDecompressorCmd == "" && bm.ExternalDecompressor != "" {
				externalDecompressorCmd = bm.ExternalDecompressor
			}
			if externalDecompressorCmd != "" {
				if deCompressionEngine == ExternalCompressor {
					deCompressionEngine = externalDecompressorCmd
					decompressor, err = newExternalDecompressor(ctx, deCompressionEngine, reader, logger)
				} else {
					decompressor, err = newBuiltinDecompressor(deCompressionEngine, reader, logger)
				}
			} else {
				if deCompressionEngine == ExternalCompressor {
					return fmt.Errorf("%w %q", errUnsupportedCompressionEngine, ExternalCompressor)
				}
				decompressor, err = newBuiltinDecompressor(deCompressionEngine, reader, logger)
			}
			if err != nil {
				return vterrors.Wrap(err, "can't create decompressor")
			}
			srcDecompressors = append(srcDecompressors, ioutil.NewTimeoutCloser(ctx, decompressor, closeTimeout))
			reader = decompressor
		}

		srcReaders = append(srcReaders, reader)
	}
	defer func() {
		for _, decompressor := range srcDecompressors {
			if cerr := decompressor.Close(); cerr != nil {
				logger.Errorf("failed to close decompressor: %v", cerr)
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
		xbstreamProgram := path.Join(xtrabackupEnginePath, xbstream)
		flagsToExec := []string{"-C", tempDir, "-xv"}
		if xbstreamRestoreFlags != "" {
			flagsToExec = append(flagsToExec, strings.Fields(xbstreamRestoreFlags)...)
		}
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

func findReplicationPosition(input, flavor string, logger logutil.Logger) (replication.Position, error) {
	match := xtrabackupReplicationPositionRegexp.FindStringSubmatch(input)
	if match == nil || len(match) != 2 {
		return replication.Position{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "couldn't find replication position in xtrabackup stderr output")
	}
	position := match[1]
	// Remove all spaces, tabs, and newlines.
	position = strings.Replace(position, " ", "", -1)
	position = strings.Replace(position, "\t", "", -1)
	position = strings.Replace(position, "\n", "", -1)
	logger.Infof("Found position: %v", position)
	if position == "" {
		return replication.Position{}, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "empty replication position from xtrabackup")
	}

	// flavor is required to parse a string into a mysql.Position
	replicationPosition, err := replication.ParsePosition(flavor, position)
	if err != nil {
		return replication.Position{}, vterrors.Wrapf(err, "can't parse replication position from xtrabackup: %v", position)
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

func addStripeFiles(ctx context.Context, params BackupParams, backupHandle backupstorage.BackupHandle, baseFileName string, numStripes int) ([]io.WriteCloser, error) {
	// Compute total size of all files we will backup.
	// We delegate the actual backing up to xtrabackup which streams
	// the files as a single archive (tar / xbstream), which might
	// further be compressed using gzip.
	// This approximate total size is passed in to AddFile so that
	// storage plugins can make appropriate choices for parameters
	// like partSize in multi-part uploads
	_, totalSize, err := findFilesToBackup(params.Cnf)
	if err != nil {
		return nil, err
	}

	if numStripes <= 1 {
		// No striping.
		file, err := backupHandle.AddFile(ctx, baseFileName, totalSize)
		return []io.WriteCloser{file}, err
	}

	files := []io.WriteCloser{}
	for i := 0; i < numStripes; i++ {
		filename := stripeFileName(baseFileName, i)
		params.Logger.Infof("Opening backup stripe file %v", filename)
		file, err := backupHandle.AddFile(ctx, filename, totalSize/int64(numStripes))
		if err != nil {
			// Close any files we already opened and clear them from the result.
			for _, file := range files {
				if err := file.Close(); err != nil {
					params.Logger.Warningf("error closing backup stripe file: %v", err)
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
	// Since we put a buffer in front of the destination file, and pargzip has its
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
func (be *XtrabackupEngine) ShouldDrainForBackup(req *tabletmanagerdatapb.BackupRequest) bool {
	return false
}

func init() {
	BackupRestoreEngineMap[xtrabackupEngineName] = &XtrabackupEngine{}
}
