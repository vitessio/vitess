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

package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

// MysqlctldProcess is a generic handle for a running mysqlctld command .
// It can be spawned manually
type MysqlctldProcess struct {
	Name         string
	Binary       string
	LogDirectory string
	TabletUID    int
	MySQLPort    int
	InitDBFile   string
	ExtraArgs    []string
	InitMysql    bool
	proc         *exec.Cmd
	exit         chan error
}

// InitDb executes mysqlctld command to add cell info
func (mysqlctld *MysqlctldProcess) InitDb() (err error) {
	tmpProcess := exec.Command(
		mysqlctld.Binary,
		"-log_dir", mysqlctld.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctld.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctld.MySQLPort),
		"-init_db_sql_file", mysqlctld.InitDBFile,
	)
	return tmpProcess.Run()
}

// Start starts the mysqlctld and returns the error.
func (mysqlctld *MysqlctldProcess) Start() error {
	_ = createDirectory(mysqlctld.LogDirectory, 0700)
	mysqlctld.proc = exec.Command(
		mysqlctld.Binary,
		"-log_dir", mysqlctld.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctld.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctld.MySQLPort),
	)

	mysqlctld.proc.Args = append(mysqlctld.proc.Args, mysqlctld.ExtraArgs...)

	if mysqlctld.InitMysql {
		mysqlctld.proc.Args = append(mysqlctld.proc.Args,
			"-init_db_sql_file", mysqlctld.InitDBFile)
	}

	errFile, _ := os.Create(path.Join(mysqlctld.LogDirectory, "mysqlctld-stderr.txt"))
	mysqlctld.proc.Stderr = errFile

	mysqlctld.proc.Env = append(mysqlctld.proc.Env, os.Environ()...)
	mysqlctld.proc.Stdout = os.Stdout
	mysqlctld.proc.Stderr = os.Stderr

	log.Infof("%v %v", strings.Join(mysqlctld.proc.Args, " "))

	err := mysqlctld.proc.Start()
	if err != nil {
		return err
	}

	mysqlctld.exit = make(chan error)
	go func() {
		mysqlctld.exit <- mysqlctld.proc.Wait()
		mysqlctld.proc = nil
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if err := healthCheck(context.Background(), mysqlctld.TabletUID); err == nil {
			return nil
		}
		select {
		case err := <-mysqlctld.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", mysqlctld.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", mysqlctld.Name, <-mysqlctld.exit)

}

// Stop executes mysqlctld command to stop mysql instance
func (mysqlctld *MysqlctldProcess) Stop() (err error) {
	if mysqlctld.proc == nil || mysqlctld.exit == nil {
		return nil
	}

	// Attempt graceful shutdown with SIGTERM first
	mysqlctld.proc.Process.Signal(syscall.SIGTERM)
	// mysqlctld.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-mysqlctld.exit:
		mysqlctld.proc = nil
		return err

	case <-time.After(15 * time.Second):
		mysqlctld.proc.Process.Kill()
		return <-mysqlctld.exit
	}
}

// CleanupFiles clean the mysql files to make sure we can start the same process again
func (mysqlctld *MysqlctldProcess) CleanupFiles(tabletUID int) {
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tabletUID)))
}

// MysqlctldProcessInstance returns a Mysqlctld handle for mysqlctld process
// configured with the given Config.
func MysqlctldProcessInstance(tabletUID int, mySQLPort int, tmpDirectory string) *MysqlctldProcess {
	mysqlctld := &MysqlctldProcess{
		Name:         "mysqlctld",
		Binary:       "mysqlctld",
		LogDirectory: tmpDirectory,
		InitDBFile:   path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"),
	}
	mysqlctld.MySQLPort = mySQLPort
	mysqlctld.TabletUID = tabletUID
	mysqlctld.InitMysql = true
	return mysqlctld
}

// StartMySQLctld starts mysqlctld process
func StartMySQLctld(ctx context.Context, tablet *Vttablet, username string, tmpDirectory string) error {
	tablet.MysqlctldProcess = *MysqlctldProcessInstance(tablet.TabletUID, tablet.MySQLPort, tmpDirectory)
	return tablet.MysqlctldProcess.Start()
}

// StartMySQLctldAndGetConnection create a connection to tablet mysql
func StartMySQLctldAndGetConnection(ctx context.Context, tablet *Vttablet, username string, tmpDirectory string) (*mysql.Conn, error) {
	tablet.MysqlctldProcess = *MysqlctldProcessInstance(tablet.TabletUID, tablet.MySQLPort, tmpDirectory)
	err := tablet.MysqlctldProcess.Start()
	if err != nil {
		return nil, err
	}

	params := mysql.ConnParams{
		Uname:      username,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tablet.TabletUID), "/mysql.sock"),
	}

	return mysql.Connect(ctx, &params)
}

// ExecuteCommandWithOutput executes any mysqlctld command and returns output
func (mysqlctld *MysqlctldProcess) ExecuteCommandWithOutput(args ...string) (result string, err error) {
	tmpProcess := exec.Command(
		mysqlctld.Binary,
		args...,
	)
	log.Info(fmt.Sprintf("Executing mysqlctld with arguments %v", strings.Join(tmpProcess.Args, " ")))
	resultByte, err := tmpProcess.CombinedOutput()
	return string(resultByte), err
}

func healthCheck(ctx context.Context, tabletUID int) error {
	params := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tabletUID), "/mysql.sock"),
	}

	_, err := mysql.Connect(ctx, &params)
	return err
}
