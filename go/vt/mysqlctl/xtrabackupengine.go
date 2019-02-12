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
	"os/exec"
	"path"
	"strings"

	"github.com/klauspost/pgzip"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

// XtrabackupEngine encapsulates the logic of the xtrabackup engine
// it implements the BackupEngine interface and contains all the logic
// required to implement a backup/restore by invoking xtrabackup with
// the appropriate parameters
type XtrabackupEngine struct {
	ProgramFileName string
}

var (
	// path where backup engine program is located
	backupEnginePath = flag.String("backup_engine_root_path", "", "directory location of the backup engine executable, e.g., /usr/local/bin")
	// flags to pass through to backup engine
	backupEngineFlags = flag.String("backup_engine_flags", "", "flags to pass to backup engine. these will be added to the end of the command. Used only by non-builtin engines")
	// backup file name if using a program that produces a single file
	backupFileName = flag.String("backup_filename", "", "filename where backup will be saved, used only if we are using an external engine (not builtin) to create the backup")
)

// ExecuteBackup returns a boolean that indicates if the backup is usable,
// and an overall error.
func (be *XtrabackupEngine) ExecuteBackup(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, backupConcurrency int, hookExtraEnv map[string]string) (bool, error) {

	flagsToExec := []string{"--defaults-file=" + cnf.path, "--stream=tar", "--backup", "--socket=" + cnf.SocketFile}

	backupProgram := path.Join(*backupEnginePath, be.ProgramFileName)
	if *backupEngineFlags != "" {
		flagsToExec = append(flagsToExec, strings.Fields(*backupEngineFlags)...)
	}

	wc, err := bh.AddFile(ctx, *backupFileName, 0)
	if err != nil {
		return false, fmt.Errorf("cannot create backup file %v: %v", *backupFileName, err)
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

	usable := (err == nil && errOutput == nil)

	if errOutput != nil {
		return usable, errors.New(string(errOutput))
	}
	return usable, nil
}

// ExecuteRestore restores from a backup. If there is no
// appropriate backup on the BackupStorage, Restore logs an error
// and returns ErrNoBackup. Any other error is returned.
func (be *XtrabackupEngine) ExecuteRestore(
	ctx context.Context,
	cnf *Mycnf,
	mysqld MysqlDaemon,
	dir string,
	bhs []backupstorage.BackupHandle,
	restoreConcurrency int,
	hookExtraEnv map[string]string,
	localMetadata map[string]string,
	logger logutil.Logger,
	deleteBeforeRestore bool,
	dbName string) (mysql.Position, error) {

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

	return mysql.Position{}, errors.New("not yet implemented")
}

func init() {
	BackupEngineMap["xtrabackup"] = &XtrabackupEngine{ProgramFileName: "xtrabackup"}
}
