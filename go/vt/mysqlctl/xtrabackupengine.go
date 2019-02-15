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
	"archive/tar"
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
	"path/filepath"
	"strings"

	"github.com/klauspost/pgzip"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
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
	// TBD whether to support flags to pass through to backup engine
	xtrabackupBackupFlags = flag.String("xtrabackup_backup_flags", "", "flags to pass to backup command. these will be added to the end of the command")
	// TBD whether to support flags to pass through to restore phase
	xtrabackupRestoreFlags = flag.String("xtrabackup_restore_flags", "", "flags to pass to restore command. these will be added to the end of the command")
	// streaming mode
	xtrabackupStreamMode = flag.String("xtrabackup_stream_mode", "", "which mode to use if streaming, valid values are tar and xbstream")
)

const (
	streamModeTar      = "tar"
	xtrabackup         = "xtrabackup"
	xbstream           = "xbstream"
	binlogInfoFileName = "xtrabackup_binlog_info"
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

func (be *XtrabackupEngine) restoreFileName() string {
	fileName := "backup"
	if *xtrabackupStreamMode != "" {
		fileName += "."
		fileName += *xtrabackupStreamMode
	}
	return fileName
}

// ExecuteBackup returns a boolean that indicates if the backup is usable,
// and an overall error.
func (be *XtrabackupEngine) ExecuteBackup(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, backupConcurrency int, hookExtraEnv map[string]string) (bool, error) {

	// TODO support directory mode - xtrabackup --backup /path/to/somewhere
	if *xtrabackupEnginePath == "" {
		return false, errors.New("xtrabackup_root_path must be provided")
	}

	// add --slave-info assuming this is a replica tablet
	// TODO what if it is master?
	flagsToExec := []string{"--defaults-file=" + cnf.path, "--backup", "--socket=" + cnf.SocketFile, "--slave-info"}
	if *xtrabackupStreamMode != "" {
		flagsToExec = append(flagsToExec, "--stream="+*xtrabackupStreamMode)
	}

	backupProgram := path.Join(*xtrabackupEnginePath, xtrabackup)
	if *xtrabackupBackupFlags != "" {
		flagsToExec = append(flagsToExec, strings.Fields(*xtrabackupBackupFlags)...)
	}

	// in streaming mode the last param is the tmp directory
	if *xtrabackupStreamMode != "" {
		// use current tablet's tmp dir
		flagsToExec = append(flagsToExec, cnf.TmpDir)
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

	// Create the external write pipe, if any.
	var pipe io.WriteCloser
	var wait hook.WaitFunc
	transformHook := *backupStorageHook
	if transformHook != "" {
		h := hook.NewHook(transformHook, []string{"-operation", "write"})
		h.ExtraEnv = hookExtraEnv
		pipe, wait, _, err = h.ExecuteAsWritePipe(writer)
		if err != nil {
			return false, fmt.Errorf("'%v' hook returned error: %v", *backupStorageHook, err)
		}
		writer = pipe
	}

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

	// Close the Pipe.
	if wait != nil {
		stderr, err := wait()
		if stderr != "" {
			log.Infof("'%v' hook returned stderr: %v", transformHook, stderr)
		}
		if err != nil {
			return false, fmt.Errorf("'%v' returned error: %v", transformHook, err)
		}
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

	// TODO check for success message : xtrabackup: completed OK!
	errOutput, err := ioutil.ReadAll(backupErr)
	backupCmd.Wait()

	usable := (err == nil && errOutput == nil)

	if errOutput != nil {
		return usable, errors.New(string(errOutput))
	}
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

	if *xtrabackupEnginePath == "" {
		return mysql.Position{}, errors.New("xtrabackup_root_path must be provided")
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

	// copy / extract files
	logger.Infof("Restore: copying all files")

	// first download the file into a tmp dir
	// use the latest backup

	bh := bhs[len(bhs)-1]

	// extract all the files
	if err := be.restoreFile(ctx, cnf, bh, *backupStorageHook, *backupStorageCompress, be.backupFileName(), hookExtraEnv); err != nil {
		return mysql.Position{}, err
	}

	// prepare the backup
	program := path.Join(*xtrabackupEnginePath, xtrabackup)
	flagsToExec := []string{"--defaults-file=" + cnf.path, "--prepare", "--target-dir=" + cnf.TmpDir}
	prepareCmd := exec.Command(program, flagsToExec...)
	// TODO check stdout for OK message
	//prepareOut, _ := prepareCmd.StdoutPipe()
	prepareErr, _ := prepareCmd.StderrPipe()
	errOutput, err := ioutil.ReadAll(prepareErr)
	prepareCmd.Wait()

	if err != nil {
		return mysql.Position{}, err
	}
	if errOutput != nil {
		return mysql.Position{}, errors.New(string(errOutput))
	}

	// then copy-back

	flagsToExec = []string{"--defaults-file=" + cnf.path, "--copy-back", "--target-dir=" + cnf.TmpDir}
	copybackCmd := exec.Command(program, flagsToExec...)
	// TODO: check stdout for OK message
	//copybackOut, _ := copybackCmd.StdoutPipe()
	copybackErr, _ := copybackCmd.StderrPipe()
	errOutput, err = ioutil.ReadAll(copybackErr)
	copybackCmd.Wait()

	if err != nil {
		return mysql.Position{}, err
	}
	if errOutput != nil {
		return mysql.Position{}, errors.New(string(errOutput))
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
	for counter < 2 {
		scanner.Scan()
		if counter == 1 {
			mysqlFlavor := os.Getenv("MYSQL_FLAVOR")
			if replicationPosition, err = mysql.ParsePosition(mysqlFlavor, scanner.Text()); err != nil {
				return mysql.Position{}, err
			}
		}
		counter = counter + 1
	}
	// TODO clean up TmpDir/backupDir
	return replicationPosition, nil
}

// restoreFile restores an individual file.
func (be *XtrabackupEngine) restoreFile(ctx context.Context, cnf *Mycnf, bh backupstorage.BackupHandle, transformHook string, compress bool, name string, hookExtraEnv map[string]string) (err error) {

	// TODO create a directory under TmpDir to hold the extracted backup
	streamMode := *xtrabackupStreamMode
	// Open the source file for reading.
	var source io.ReadCloser
	source, err = bh.ReadFile(ctx, name)
	if err != nil {
		return err
	}
	defer source.Close()

	reader := io.MultiReader(source)

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

	if streamMode == streamModeTar {
		tr := tar.NewReader(reader)
	Extract:
		for {
			header, err := tr.Next()
			switch {
			// if no more files are found return
			case err == io.EOF:
				break Extract
				// return any other error
			case err != nil:
				return err
				// if the header is nil, just skip it (not sure how this happens)
			case header == nil:
				continue
			}

			// the target location where the dir/file should be created
			target := filepath.Join(cnf.TmpDir, header.Name)

			// the following switch could also be done using fi.Mode(), not sure if there
			// a benefit of using one vs. the other.
			// fi := header.FileInfo()

			// check the file type
			switch header.Typeflag {
			// if its a dir and it doesn't exist create it
			case tar.TypeDir:
				if _, err := os.Stat(target); err != nil {
					if err := os.MkdirAll(target, 0755); err != nil {
						return err
					}
				}
				// if it's a file create it
			case tar.TypeReg:
				f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
				if err != nil {
					return err
				}
				// copy over contents
				if _, err := io.Copy(f, tr); err != nil {
					return err
				}
				// manually close here after each file operation; defering would cause each file close
				// to wait until all operations have completed.
				f.Close()
			}
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

	} else if streamMode == xbstream {
		dstFileName := path.Join(cnf.TmpDir, be.restoreFileName())

		// Open the destination file for writing.
		dstFile, err := os.Create(dstFileName)
		if err != nil {
			return fmt.Errorf("cannot create destination file %v: %v", dstFileName, err)
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
		if err := dst.Flush(); err != nil {
			return err
		}

		// now extract the files by running xbstream
		xbstreamProgram := path.Join(*xtrabackupEnginePath, xbstream)
		flagsToExec := []string{"-x", "-C", cnf.TmpDir, "<", be.restoreFileName()}
		xbstreamCmd := exec.Command(xbstreamProgram, flagsToExec...)
		// TODO check
		//xbstreamOut, _ := xbstreamCmd.StdoutPipe()
		xbstreamErr, _ := xbstreamCmd.StderrPipe()
		xbstreamCmd.Start()
		errOutput, err := ioutil.ReadAll(xbstreamErr)
		xbstreamCmd.Wait()

		if err != nil {
			return err
		}
		if errOutput != nil {
			return errors.New(string(errOutput))
		}
	}
	return nil
}

func init() {
	BackupEngineMap[xtrabackup] = &XtrabackupEngine{}
}
