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
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

// MysqlctldProcess is a generic handle for a running mysqlctld command .
// It can be spawned manually
type MysqlctldProcess struct {
	Name               string
	Binary             string
	LogDirectory       string
	Password           string
	TabletUID          int
	MySQLPort          int
	InitDBFile         string
	ExtraArgs          []string
	process            *exec.Cmd
	exit               chan error
	InitMysql          bool
	exitSignalReceived bool
}

// InitDb executes mysqlctld command to add cell info
func (mysqlctld *MysqlctldProcess) InitDb() (err error) {
	tmpProcess := exec.Command(
		mysqlctld.Binary,
		"--log_dir", mysqlctld.LogDirectory,
		"--tablet_uid", fmt.Sprintf("%d", mysqlctld.TabletUID),
		"--mysql_port", fmt.Sprintf("%d", mysqlctld.MySQLPort),
		"--init_db_sql_file", mysqlctld.InitDBFile,
	)
	return tmpProcess.Run()
}

// Start starts the mysqlctld and returns the error.
func (mysqlctld *MysqlctldProcess) Start() error {
	if mysqlctld.process != nil {
		return fmt.Errorf("process is already running")
	}
	_ = createDirectory(mysqlctld.LogDirectory, 0700)
	tempProcess := exec.Command(
		mysqlctld.Binary,
		"--log_dir", mysqlctld.LogDirectory,
		"--tablet_uid", fmt.Sprintf("%d", mysqlctld.TabletUID),
		"--mysql_port", fmt.Sprintf("%d", mysqlctld.MySQLPort),
	)

	tempProcess.Args = append(tempProcess.Args, mysqlctld.ExtraArgs...)

	if mysqlctld.InitMysql {
		tempProcess.Args = append(tempProcess.Args,
			"--init_db_sql_file", mysqlctld.InitDBFile)
	}

	errFile, _ := os.Create(path.Join(mysqlctld.LogDirectory, "mysqlctld-stderr.txt"))
	tempProcess.Stderr = errFile

	tempProcess.Env = append(tempProcess.Env, os.Environ()...)
	tempProcess.Stdout = os.Stdout
	tempProcess.Stderr = os.Stderr

	log.Infof("%v", strings.Join(tempProcess.Args, " "))

	err := tempProcess.Start()
	if err != nil {
		return err
	}

	mysqlctld.process = tempProcess

	mysqlctld.exit = make(chan error)
	go func(mysqlctld *MysqlctldProcess) {
		err := mysqlctld.process.Wait()
		if !mysqlctld.exitSignalReceived {
			fmt.Printf("mysqlctld stopped unexpectedly, tabletUID %v, mysql port %v, PID %v\n", mysqlctld.TabletUID, mysqlctld.MySQLPort, mysqlctld.process.Process.Pid)
		}
		mysqlctld.process = nil
		mysqlctld.exitSignalReceived = false
		mysqlctld.exit <- err
	}(mysqlctld)

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if mysqlctld.IsHealthy() {
			return nil
		}
		select {
		case err := <-mysqlctld.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", mysqlctld.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", mysqlctld.Name, mysqlctld.Stop())

}

// Stop executes mysqlctld command to stop mysql instance
func (mysqlctld *MysqlctldProcess) Stop() error {
	// if mysqlctld.process == nil || mysqlctld.exit == nil {
	// 	return nil
	// }
	mysqlctld.exitSignalReceived = true
	tmpProcess := exec.Command(
		"mysqlctl",
		"--tablet_uid", fmt.Sprintf("%d", mysqlctld.TabletUID),
	)
	tmpProcess.Args = append(tmpProcess.Args, mysqlctld.ExtraArgs...)
	tmpProcess.Args = append(tmpProcess.Args, "shutdown")
	return tmpProcess.Run()
}

// CleanupFiles clean the mysql files to make sure we can start the same process again
func (mysqlctld *MysqlctldProcess) CleanupFiles(tabletUID int) {
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tabletUID)))
}

// MysqlCtldProcessInstance returns a Mysqlctld handle for mysqlctld process
// configured with the given Config.
func MysqlCtldProcessInstance(tabletUID int, mySQLPort int, tmpDirectory string) *MysqlctldProcess {
	var initFile = path.Join(os.Getenv("VTROOT"), "/config/init_db.sql") //default value
	if isSQL, err := isSQLFlavor(); err == nil {
		if !isSQL {
			// execute init_db without `super_read_only`
			initFile = path.Join(os.Getenv("VTROOT"), "config/init_testserver_db.sql")
		}
	}
	mysqlctld := &MysqlctldProcess{
		Name:         "mysqlctld",
		Binary:       "mysqlctld",
		LogDirectory: tmpDirectory,
		InitDBFile:   initFile,
	}
	mysqlctld.MySQLPort = mySQLPort
	mysqlctld.TabletUID = tabletUID
	mysqlctld.InitMysql = true
	return mysqlctld
}

// IsHealthy gives the health status of mysql.
func (mysqlctld *MysqlctldProcess) IsHealthy() bool {
	socketFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", mysqlctld.TabletUID), "/mysql.sock")
	params := NewConnParams(0, mysqlctld.Password, socketFile, "")
	_, err := mysql.Connect(context.Background(), &params)
	return err == nil
}
