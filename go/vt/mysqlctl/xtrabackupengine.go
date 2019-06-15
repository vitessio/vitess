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
	"io"
	"io/ioutil"
	"os/exec"
	"path"
	"strings"

	"github.com/klauspost/pgzip"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
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
)

const (
	streamModeTar          = "tar"
	xtrabackupBinaryName   = "xtrabackup"
	xtrabackupBackupMethod = "xtrabackup"
	xbstream               = "xbstream"
)

// xtraBackupManifest represents a backup.
// It stores the name of the backup file, the replication position,
// whether the backup is compressed using gzip, and any extra
// command line parameters used while invoking it.
type xtraBackupManifest struct {
	// Name of the backup file
	FileName string
	// BackupMethod, set to xtrabackup
	BackupMethod string
	// Position at which the backup was taken
	Position mysql.Position
	// SkipCompress can be set if the backup files were not run
	// through gzip.
	SkipCompress bool
	// Params are the parameters that backup was run with
	Params string `json:"ExtraCommandLineParams"`
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

// ExecuteBackup returns a boolean that indicates if the backup is usable,
// and an overall error.
func (be *XtrabackupEngine) ExecuteBackup(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, backupConcurrency int, hookExtraEnv map[string]string) (bool, error) {

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

	backupFileName := be.backupFileName()

	wc, err := bh.AddFile(ctx, backupFileName, 0)
	if err != nil {
		return false, vterrors.Wrapf(err, "cannot create backup file %v", backupFileName)
	}
	closeFile := func(wc io.WriteCloser, fileName string) {
		if closeErr := wc.Close(); err == nil {
			err = closeErr
		} else if closeErr != nil {
			// since we already have an error just log this
			logger.Errorf("error closing file %v: %v", fileName, err)
		}
	}
	defer closeFile(wc, backupFileName)

	backupCmd := exec.Command(backupProgram, flagsToExec...)
	backupOut, _ := backupCmd.StdoutPipe()
	backupErr, _ := backupCmd.StderrPipe()
	dst := bufio.NewWriterSize(wc, writerBufferSize)
	writer := io.MultiWriter(dst)

	// Create the gzip compression pipe, if necessary.
	var gzip *pgzip.Writer
	if *backupStorageCompress {
		gzip, err = pgzip.NewWriterLevel(writer, pgzip.BestSpeed)
		if err != nil {
			return false, vterrors.Wrap(err, "cannot create gziper")
		}
		gzip.SetConcurrency(*backupCompressBlockSize, *backupCompressBlocks)
		writer = gzip
	}

	if err = backupCmd.Start(); err != nil {
		return false, vterrors.Wrap(err, "unable to start backup")
	}

	// Copy from the stream output to destination file (optional gzip)
	_, err = io.Copy(writer, backupOut)
	if err != nil {
		return false, vterrors.Wrap(err, "cannot copy output from xtrabackup command")
	}

	// Close gzip to flush it, after that all data is sent to writer.
	if gzip != nil {
		if err = gzip.Close(); err != nil {
			return false, vterrors.Wrap(err, "cannot close gzip")
		}
	}

	// Flush the buffer to finish writing on destination.
	if err = dst.Flush(); err != nil {
		return false, vterrors.Wrapf(err, "cannot flush destination: %v", backupFileName)
	}

	stderrOutput, err := ioutil.ReadAll(backupErr)
	if err != nil {
		return false, vterrors.Wrap(err, "backup failed while reading command output")
	}
	err = backupCmd.Wait()
	output := string(stderrOutput)
	logger.Infof("Xtrabackup backup command output: %v", output)
	if err != nil {
		return false, vterrors.Wrap(err, "xtrabackup failed with error")
	}

	replicationPosition, rerr := findReplicationPosition(output, flavor, logger)
	if rerr != nil {
		return false, vterrors.Wrap(rerr, "backup failed trying to find replication position")
	}
	// open the MANIFEST
	mwc, err := bh.AddFile(ctx, backupManifest, 0)
	if err != nil {
		return false, vterrors.Wrapf(err, "cannot add %v to backup", backupManifest)
	}
	defer closeFile(mwc, backupManifest)

	// JSON-encode and write the MANIFEST
	bm := &xtraBackupManifest{
		FileName:     backupFileName,
		BackupMethod: xtrabackupBackupMethod,
		Position:     replicationPosition,
		SkipCompress: !*backupStorageCompress,
		Params:       *xtrabackupBackupFlags,
	}

	data, err := json.MarshalIndent(bm, "", "  ")
	if err != nil {
		return false, vterrors.Wrapf(err, "cannot JSON encode %v", backupManifest)
	}
	if _, err := mwc.Write([]byte(data)); err != nil {
		return false, vterrors.Wrapf(err, "cannot write %v", backupManifest)
	}

	return true, nil
}

// ExecuteRestore restores from a backup. Any error is returned.
func (be *XtrabackupEngine) ExecuteRestore(
	ctx context.Context,
	cnf *Mycnf,
	mysqld MysqlDaemon,
	logger logutil.Logger,
	dir string,
	bhs []backupstorage.BackupHandle,
	restoreConcurrency int,
	hookExtraEnv map[string]string) (mysql.Position, error) {

	var bh backupstorage.BackupHandle
	var bm xtraBackupManifest
	var index int
	zeroPosition := mysql.Position{}

	for index = len(bhs) - 1; index >= 0; index-- {
		bh = bhs[index]
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

		logger.Infof("Restore: found backup %v %v to restore with %v file", bh.Directory(), bh.Name(), bm.FileName)
		break
	}
	if index < 0 {
		// There is at least one attempted backup, but none could be read.
		// This implies there is data we ought to have, so it's not safe to start
		// up empty.
		return zeroPosition, ErrNoCompleteBackup
	}

	// Starting from here we won't be able to recover if we get stopped by a cancelled
	// context. Thus we use the background context to get through to the finish.

	logger.Infof("Restore: shutdown mysqld")
	err := mysqld.Shutdown(context.Background(), cnf, true)
	if err != nil {
		return zeroPosition, err
	}

	logger.Infof("Restore: deleting existing files")
	if err := removeExistingFiles(cnf); err != nil {
		return zeroPosition, err
	}

	logger.Infof("Restore: reinit config file")
	err = mysqld.ReinitConfig(context.Background(), cnf)
	if err != nil {
		return zeroPosition, err
	}

	// copy / extract files
	logger.Infof("Restore: Extracting all files")

	// first download the file into a tmp dir
	// extract all the files
	if err := be.restoreFile(ctx, cnf, logger, bh, !bm.SkipCompress, be.backupFileName()); err != nil {
		logger.Errorf("error restoring backup file %v:%v", be.backupFileName(), err)
		return zeroPosition, err
	}

	// copy / extract files
	logger.Infof("Restore: Preparing the files")
	// prepare the backup
	restoreProgram := path.Join(*xtrabackupEnginePath, xtrabackupBinaryName)
	flagsToExec := []string{"--defaults-file=" + cnf.path,
		"--prepare",
		"--target-dir=" + cnf.TmpDir,
	}
	prepareCmd := exec.Command(restoreProgram, flagsToExec...)
	prepareOut, _ := prepareCmd.StdoutPipe()
	prepareErr, _ := prepareCmd.StderrPipe()
	if err = prepareCmd.Start(); err != nil {
		return zeroPosition, vterrors.Wrap(err, "unable to start prepare")
	}

	errOutput, _ := ioutil.ReadAll(prepareErr)
	stdOutput, _ := ioutil.ReadAll(prepareOut)
	err = prepareCmd.Wait()
	if string(stdOutput) != "" {
		logger.Infof("Prepare stdout %v", string(stdOutput))
	}
	output := string(errOutput)
	if output != "" {
		logger.Infof("Prepare stderr %v", output)
	}

	if err != nil {
		return zeroPosition, vterrors.Wrap(err, "prepare step failed")
	}

	// then copy-back
	logger.Infof("Restore: Copying the files")

	flagsToExec = []string{"--defaults-file=" + cnf.path,
		"--copy-back",
		"--target-dir=" + cnf.TmpDir,
	}
	copybackCmd := exec.Command(restoreProgram, flagsToExec...)
	copybackErr, _ := copybackCmd.StderrPipe()
	copybackOut, _ := copybackCmd.StdoutPipe()

	if err = copybackCmd.Start(); err != nil {
		return zeroPosition, vterrors.Wrap(err, "unable to start copy-back")
	}

	errOutput, _ = ioutil.ReadAll(copybackErr)
	stdOutput, _ = ioutil.ReadAll(copybackOut)
	err = copybackCmd.Wait()
	output = string(errOutput)
	if output != "" {
		logger.Infof("Copy-back stderr %v", string(output))
	}
	if string(stdOutput) != "" {
		logger.Infof("Copy-back stdout %v", string(stdOutput))
	}

	if err != nil {
		return zeroPosition, vterrors.Wrap(err, "copy-back step failed")
	}

	// now find the slave position and return that
	logger.Infof("Returning replication position %v", bm.Position)
	return bm.Position, nil
}

// restoreFile restores an individual file.
func (be *XtrabackupEngine) restoreFile(
	ctx context.Context,
	cnf *Mycnf,
	logger logutil.Logger,
	bh backupstorage.BackupHandle,
	compress bool,
	name string) (err error) {

	streamMode := *xtrabackupStreamMode
	// Open the source file for reading.
	var source io.ReadCloser
	source, err = bh.ReadFile(ctx, name)
	if err != nil {
		return err
	}
	defer source.Close()

	reader := io.MultiReader(source)

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
					logger.Errorf("failed to close gunziper %v: %v", name, cerr)
				} else {
					err = cerr
				}
			}
		}()
		reader = gz
	}

	switch streamMode {
	case streamModeTar:
		// now extract the files by running tar
		// error if we can't find tar
		flagsToExec := []string{"-C", cnf.TmpDir, "-xi"}
		tarCmd := exec.Command("tar", flagsToExec...)
		logger.Infof("Executing tar cmd with flags %v", flagsToExec)
		tarCmd.Stdin = reader
		tarOut, _ := tarCmd.StdoutPipe()
		tarErr, _ := tarCmd.StderrPipe()
		tarCmd.Start()
		output, _ := ioutil.ReadAll(tarOut)
		errOutput, _ := ioutil.ReadAll(tarErr)
		err := tarCmd.Wait()

		if string(output) != "" {
			logger.Infof("output from tar: %v ", string(output))
		}
		if string(errOutput) != "" {
			logger.Infof("error from tar: %v ", string(errOutput))
		}
		if err != nil {
			return vterrors.Wrap(err, "error from tar")
		}

	case xbstream:
		// now extract the files by running xbstream
		xbstreamProgram := xbstream
		flagsToExec := []string{}
		if *xbstreamRestoreFlags != "" {
			flagsToExec = append(flagsToExec, strings.Fields(*xbstreamRestoreFlags)...)
		}
		flagsToExec = append(flagsToExec, "-C", cnf.TmpDir, "-x")
		xbstreamCmd := exec.Command(xbstreamProgram, flagsToExec...)
		logger.Infof("Executing xbstream cmd: %v %v", xbstreamProgram, flagsToExec)
		xbstreamCmd.Stdin = reader
		xbstreamOut, _ := xbstreamCmd.StdoutPipe()
		xbstreamErr, _ := xbstreamCmd.StderrPipe()
		xbstreamCmd.Start()
		output, _ := ioutil.ReadAll(xbstreamOut)
		errOutput, _ := ioutil.ReadAll(xbstreamErr)
		err := xbstreamCmd.Wait()

		if string(output) != "" {
			logger.Infof("Output from xbstream: %v ", string(output))
		}
		if string(errOutput) != "" {
			logger.Infof("error from xbstream: %v", string(errOutput))
		}
		if err != nil {
			return vterrors.Wrap(err, "error from xbstream")
		}
	default:
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "%v is not a valid value for xtrabackup_stream_mode, supported modes are tar and xbstream", streamMode)
	}
	return nil
}

func findReplicationPosition(input, flavor string, logger logutil.Logger) (mysql.Position, error) {
	substrs := strings.Split(input, "'")
	index := -1
	for i, str := range substrs {
		if strings.Contains(str, "GTID of the last change") {
			index = i + 1
			break
		}
	}
	position := ""
	// asserts that xtrabackup output comes with GTIDs in the format we expect
	if index != -1 && index < len(substrs) {
		// since we are extracting this from the log, it contains newlines
		// replace them with a single space to match the SET GLOBAL gtid_purged command in xtrabackup_slave_info
		position = strings.Replace(substrs[index], "\n", " ", -1)
	}
	logger.Infof("Found position: %v", position)

	// flavor is required to parse a string into a mysql.Position
	replicationPosition, err := mysql.ParsePosition(flavor, position)
	if err != nil {
		return mysql.Position{}, err
	}
	return replicationPosition, nil
}

func init() {
	BackupEngineMap[xtrabackupBackupMethod] = &XtrabackupEngine{}
}
