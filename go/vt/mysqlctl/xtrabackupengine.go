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
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/klauspost/pgzip"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
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
	// TODO assume lives on path and remove this option?
	xtrabackupEnginePath = flag.String("xtrabackup_root_path", "", "directory location of the xtrabackup executable, e.g., /usr/bin")
	// TBD whether to support flags to pass through to backup engine
	xtrabackupBackupFlags = flag.String("xtrabackup_backup_flags", "", "flags to pass to backup command. these will be added to the end of the command")
	// TBD whether to support flags to pass through to restore phase
	xtrabackupRestoreFlags = flag.String("xtrabackup_restore_flags", "", "flags to pass to restore command. these will be added to the end of the command")
	// streaming mode
	xtrabackupStreamMode = flag.String("xtrabackup_stream_mode", "tar", "which mode to use if streaming, valid values are tar and xbstream")
)

const (
	streamModeTar      = "tar"
	xtrabackup         = "xtrabackup"
	xbstream           = "xbstream"
	binlogInfoFileName = "xtrabackup_binlog_info"
	successMsg         = "completed OK!"
)

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

	/* TODO uncomment if we keep this option
	if *xtrabackupEnginePath == "" {
		return false, errors.New("xtrabackup_root_path must be provided")
	}
	*/
	backupProgram := path.Join(*xtrabackupEnginePath, xtrabackup)
	// TODO check that the executable file exists, exit if it doesn't or isn't executable

	// add --slave-info assuming this is a replica tablet
	// TODO what if it is master?

	// TODO need to pass backup username here
	flagsToExec := []string{"--defaults-file=" + cnf.path,
		"--backup",
		"--socket=" + cnf.SocketFile,
		"--slave-info",
		//"--user=vt_dba",
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
		return false, fmt.Errorf("cannot create backup file %v: %v", backupFileName, err)
	}
	defer func() {
		if closeErr := wc.Close(); err == nil {
			err = closeErr
		}
	}()

	backupCmd := exec.Command(backupProgram, flagsToExec...)
	backupOut, _ := backupCmd.StdoutPipe()
	backupErr, _ := backupCmd.StderrPipe()
	dst := bufio.NewWriterSize(wc, 2*1024*1024)
	writer := io.MultiWriter(dst)

	// Create the gzip compression pipe, if necessary.
	var gzip *pgzip.Writer
	if *backupStorageCompress {
		gzip, err = pgzip.NewWriterLevel(writer, pgzip.BestSpeed)
		if err != nil {
			return false, fmt.Errorf("cannot create gziper: %v", err)
		}
		gzip.SetConcurrency(*backupCompressBlockSize, *backupCompressBlocks)
		writer = gzip
	}

	backupCmd.Start()

	// Copy from the stream output to destination file (optional gzip)
	_, err = io.Copy(writer, backupOut)
	if err != nil {
		return false, fmt.Errorf("cannot copy data: %v", err)
	}

	// Close gzip to flush it, after that all data is sent to writer.
	if gzip != nil {
		if err = gzip.Close(); err != nil {
			return false, fmt.Errorf("cannot close gzip: %v", err)
		}
	}

	// Flush the buffer to finish writing on destination.
	if err = dst.Flush(); err != nil {
		return false, fmt.Errorf("cannot flush dst: %v", err)
	}

	errOutput, err := ioutil.ReadAll(backupErr)
	backupCmd.Wait()
	output := string(errOutput)

	logger.Infof("Xtrabackup backup command output: %v", output)
	// check for success message : xtrabackup: completed OK!
	usable := (err == nil && strings.Contains(output, successMsg))

	return usable, nil
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

	/* TODO uncomment if we keep this option
	if *xtrabackupEnginePath == "" {
		return mysql.Position{}, errors.New("xtrabackup_root_path must be provided")
	} */

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

	// copy / extract files
	logger.Infof("Restore: Extracting all files")

	// first download the file into a tmp dir
	// use the latest backup

	bh := bhs[len(bhs)-1]

	// extract all the files
	if err := be.restoreFile(ctx, cnf, logger, bh, *backupStorageHook, *backupStorageCompress, be.backupFileName(), hookExtraEnv); err != nil {
		logger.Errorf("error restoring backup file %v:%v", be.backupFileName(), err)
		return mysql.Position{}, err
	}

	// copy / extract files
	logger.Infof("Restore: Preparing the files")
	// prepare the backup
	program := path.Join(*xtrabackupEnginePath, xtrabackup)
	flagsToExec := []string{"--defaults-file=" + cnf.path,
		"--prepare",
		"--target-dir=" + cnf.TmpDir,
	}
	prepareCmd := exec.Command(program, flagsToExec...)
	prepareOut, _ := prepareCmd.StdoutPipe()
	prepareErr, _ := prepareCmd.StderrPipe()
	prepareCmd.Start()
	errOutput, _ := ioutil.ReadAll(prepareErr)
	stdOutput, _ := ioutil.ReadAll(prepareOut)
	err = prepareCmd.Wait()
	if string(stdOutput) != "" {
		// TODO remove after testing
		logger.Infof("Prepare stdout %v", string(stdOutput))
	}
	output := string(errOutput)
	if output != "" {
		// TODO remove after testing
		logger.Infof("Prepare stderr %v", output)
	}

	if err != nil {
		return mysql.Position{}, err
	}
	if !strings.Contains(output, successMsg) {
		return mysql.Position{}, errors.New("Prepare step failed")
	}

	// then copy-back
	logger.Infof("Restore: Copying the files")

	flagsToExec = []string{"--defaults-file=" + cnf.path,
		"--copy-back",
		"--target-dir=" + cnf.TmpDir,
	}
	copybackCmd := exec.Command(program, flagsToExec...)
	// TODO: check stdout for OK message
	copybackErr, _ := copybackCmd.StderrPipe()
	copybackOut, _ := copybackCmd.StdoutPipe()
	copybackCmd.Start()
	errOutput, _ = ioutil.ReadAll(copybackErr)
	stdOutput, _ = ioutil.ReadAll(copybackOut)
	err = copybackCmd.Wait()
	output = string(errOutput)
	if output != "" {
		// TODO remove after testing
		logger.Infof("Copy-back stderr %v", string(output))
	}
	if string(stdOutput) != "" {
		// TODO remove after testing
		logger.Infof("Copy-back stdout %v", string(stdOutput))
	}

	if err != nil {
		return mysql.Position{}, err
	}
	if !strings.Contains(output, successMsg) {
		return mysql.Position{}, errors.New("Copy-back step failed")
	}

	// now find the slave position and return that
	binlogInfoFile, err := os.Open(path.Join(cnf.TmpDir, binlogInfoFileName))
	if err != nil {
		return mysql.Position{}, err
	}
	defer binlogInfoFile.Close()
	scanner := bufio.NewScanner(binlogInfoFile)
	scanner.Split(bufio.ScanWords)
	counter := 0
	var replicationPosition mysql.Position
	for counter < 3 {
		scanner.Scan()
		if counter == 2 {
			mysqlFlavor := os.Getenv("MYSQL_FLAVOR")
			if replicationPosition, err = mysql.ParsePosition(mysqlFlavor, scanner.Text()); err != nil {
				return mysql.Position{}, err
			}
		}
		counter = counter + 1
	}
	// TODO clean up extracted files from TmpDir
	return replicationPosition, nil
}

// restoreFile restores an individual file.
func (be *XtrabackupEngine) restoreFile(
	ctx context.Context,
	cnf *Mycnf,
	logger logutil.Logger,
	bh backupstorage.BackupHandle,
	transformHook string,
	compress bool,
	name string,
	hookExtraEnv map[string]string) (err error) {

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
			return vterrors.Wrapf(err, "error from tar")
		}

	case xbstream:
		// now extract the files by running xbstream
		xbstreamProgram := path.Join(*xtrabackupEnginePath, xbstream)
		flagsToExec := []string{"-C", cnf.TmpDir, "-x"}
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
			return vterrors.Wrapf(err, "error from xbstream")
		}
	default:
	}
	return nil
}

func init() {
	BackupEngineMap[xtrabackup] = &XtrabackupEngine{}
}
