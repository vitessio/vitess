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
	"html/template"
	"os"
	"os/exec"
	"path"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/tlstest"
)

// MysqlctlProcess is a generic handle for a running mysqlctl command .
// It can be spawned manually
type MysqlctlProcess struct {
	Name            string
	Binary          string
	LogDirectory    string
	TabletUID       int
	MySQLPort       int
	InitDBFile      string
	ExtraArgs       []string
	InitMysql       bool
	SecureTransport bool
}

// InitDb executes mysqlctl command to add cell info
func (mysqlctl *MysqlctlProcess) InitDb() (err error) {
	args := []string{"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
		"init",
		"-init_db_sql_file", mysqlctl.InitDBFile}
	if *isCoverage {
		args = append([]string{"-test.coverprofile=" + getCoveragePath("mysql-initdb.out"), "-test.v"}, args...)
	}
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		args...)
	return tmpProcess.Run()
}

// Start executes mysqlctl command to start mysql instance
func (mysqlctl *MysqlctlProcess) Start() (err error) {
	tmpProcess, err := mysqlctl.StartProcess()
	if err != nil {
		return err
	}
	return tmpProcess.Wait()
}

// StartProcess starts the mysqlctl and returns the process reference
func (mysqlctl *MysqlctlProcess) StartProcess() (*exec.Cmd, error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
	)
	if *isCoverage {
		tmpProcess.Args = append(tmpProcess.Args, []string{"-test.coverprofile=" + getCoveragePath("mysql-start.out")}...)
	}

	if len(mysqlctl.ExtraArgs) > 0 {
		tmpProcess.Args = append(tmpProcess.Args, mysqlctl.ExtraArgs...)
	}
	if mysqlctl.InitMysql {
		if mysqlctl.SecureTransport {
			// Set up EXTRA_MY_CNF for ssl
			sslPath := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/ssl_%010d", mysqlctl.TabletUID))
			sslPathData := struct {
				Dir string
			}{
				Dir: sslPath,
			}

			os.MkdirAll(sslPath, 0755)
			extraMyCNF := path.Join(sslPath, "ssl.cnf")
			fout, err := os.Create(extraMyCNF)
			if err != nil {
				log.Error(err)
				return nil, err
			}

			template.Must(template.New(fmt.Sprintf("%010d", mysqlctl.TabletUID)).Parse(`
ssl_ca={{.Dir}}/ca-cert.pem
ssl_cert={{.Dir}}/server-001-cert.pem
ssl_key={{.Dir}}/server-001-key.pem
`)).Execute(fout, sslPathData)
			if err := fout.Close(); err != nil {
				return nil, err
			}

			tlstest.CreateClientServerCertPairs(sslPath)

			tmpProcess.Env = append(tmpProcess.Env, "EXTRA_MY_CNF="+extraMyCNF)
			tmpProcess.Env = append(tmpProcess.Env, "VTDATAROOT="+os.Getenv("VTDATAROOT"))
		}

		tmpProcess.Args = append(tmpProcess.Args, "init",
			"-init_db_sql_file", mysqlctl.InitDBFile)
	}
	tmpProcess.Args = append(tmpProcess.Args, "start")
	log.Infof("Starting mysqlctl with command: %v", tmpProcess.Args)
	return tmpProcess, tmpProcess.Start()
}

// Stop executes mysqlctl command to stop mysql instance
func (mysqlctl *MysqlctlProcess) Stop() (err error) {
	log.Infof("Shutting down MySQL: %d", mysqlctl.TabletUID)
	defer log.Infof("MySQL shutdown complete: %d", mysqlctl.TabletUID)
	tmpProcess, err := mysqlctl.StopProcess()
	if err != nil {
		return err
	}
	return tmpProcess.Wait()
}

// StopProcess executes mysqlctl command to stop mysql instance and returns process reference
func (mysqlctl *MysqlctlProcess) StopProcess() (*exec.Cmd, error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
	)
	if *isCoverage {
		tmpProcess.Args = append(tmpProcess.Args, []string{"-test.coverprofile=" + getCoveragePath("mysql-stop.out")}...)
	}
	if len(mysqlctl.ExtraArgs) > 0 {
		tmpProcess.Args = append(tmpProcess.Args, mysqlctl.ExtraArgs...)
	}
	tmpProcess.Args = append(tmpProcess.Args, "shutdown")
	return tmpProcess, tmpProcess.Start()
}

// CleanupFiles clean the mysql files to make sure we can start the same process again
func (mysqlctl *MysqlctlProcess) CleanupFiles(tabletUID int) {
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/data", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/relay-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/tmp", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/bin-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/innodb", tabletUID)))
}

// MysqlCtlProcessInstanceOptionalInit returns a Mysqlctl handle for mysqlctl process
// configured with the given Config.
func MysqlCtlProcessInstanceOptionalInit(tabletUID int, mySQLPort int, tmpDirectory string, initMySQL bool) *MysqlctlProcess {
	mysqlctl := &MysqlctlProcess{
		Name:         "mysqlctl",
		Binary:       "mysqlctl",
		LogDirectory: tmpDirectory,
		InitDBFile:   path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"),
	}
	mysqlctl.MySQLPort = mySQLPort
	mysqlctl.TabletUID = tabletUID
	mysqlctl.InitMysql = initMySQL
	mysqlctl.SecureTransport = false
	return mysqlctl
}

// MysqlCtlProcessInstance returns a Mysqlctl handle for mysqlctl process
// configured with the given Config.
func MysqlCtlProcessInstance(tabletUID int, mySQLPort int, tmpDirectory string) *MysqlctlProcess {
	return MysqlCtlProcessInstanceOptionalInit(tabletUID, mySQLPort, tmpDirectory, true)
}

// StartMySQL starts mysqlctl process
func StartMySQL(ctx context.Context, tablet *Vttablet, username string, tmpDirectory string) error {
	tablet.MysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, tmpDirectory)
	return tablet.MysqlctlProcess.Start()
}

// StartMySQLAndGetConnection create a connection to tablet mysql
func StartMySQLAndGetConnection(ctx context.Context, tablet *Vttablet, username string, tmpDirectory string) (*mysql.Conn, error) {
	tablet.MysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, tmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	if err != nil {
		return nil, err
	}
	params := mysql.ConnParams{
		Uname:      username,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tablet.TabletUID), "/mysql.sock"),
	}

	return mysql.Connect(ctx, &params)
}

// ExecuteCommandWithOutput executes any mysqlctl command and returns output
func (mysqlctl *MysqlctlProcess) ExecuteCommandWithOutput(args ...string) (result string, err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		args...,
	)
	log.Info(fmt.Sprintf("Executing mysqlctl with arguments %v", strings.Join(tmpProcess.Args, " ")))
	resultByte, err := tmpProcess.CombinedOutput()
	return string(resultByte), err
}
