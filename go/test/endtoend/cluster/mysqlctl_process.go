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
	"strconv"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
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
	args := []string{"--log_dir", mysqlctl.LogDirectory,
		"--tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"--mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
		"init", "--",
		"--init_db_sql_file", mysqlctl.InitDBFile}
	if *isCoverage {
		args = append([]string{"--test.coverprofile=" + getCoveragePath("mysql-initdb.out"), "--test.v"}, args...)
	}
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		args...)
	return tmpProcess.Run()
}

// Start executes mysqlctl command to start mysql instance
func (mysqlctl *MysqlctlProcess) Start() (err error) {
	tmpProcess, err := mysqlctl.startProcess(true)
	if err != nil {
		return err
	}
	return tmpProcess.Wait()
}

// StartProvideInit executes mysqlctl command to start mysql instance
func (mysqlctl *MysqlctlProcess) StartProvideInit(init bool) (err error) {
	tmpProcess, err := mysqlctl.startProcess(init)
	if err != nil {
		return err
	}
	return tmpProcess.Wait()
}

// StartProcess starts the mysqlctl and returns the process reference
func (mysqlctl *MysqlctlProcess) StartProcess() (*exec.Cmd, error) {
	return mysqlctl.startProcess(true)
}

func (mysqlctl *MysqlctlProcess) startProcess(init bool) (*exec.Cmd, error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"--log_dir", mysqlctl.LogDirectory,
		"--tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"--mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
	)
	if *isCoverage {
		tmpProcess.Args = append(tmpProcess.Args, []string{"--test.coverprofile=" + getCoveragePath("mysql-start.out")}...)
	}

	if len(mysqlctl.ExtraArgs) > 0 {
		tmpProcess.Args = append(tmpProcess.Args, mysqlctl.ExtraArgs...)
	}
	if mysqlctl.InitMysql {
		if mysqlctl.SecureTransport {
			// Set up EXTRA_MY_CNF for ssl
			sslPath := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/ssl_%010d", mysqlctl.TabletUID))
			os.MkdirAll(sslPath, 0755)

			// create certificates
			clientServerKeyPair := tlstest.CreateClientServerCertPairs(sslPath)

			// use the certificate values in template to create cnf file
			sslPathData := struct {
				Dir        string
				ServerCert string
				ServerKey  string
			}{
				Dir:        sslPath,
				ServerCert: clientServerKeyPair.ServerCert,
				ServerKey:  clientServerKeyPair.ServerKey,
			}

			extraMyCNF := path.Join(sslPath, "ssl.cnf")
			fout, err := os.Create(extraMyCNF)
			if err != nil {
				log.Error(err)
				return nil, err
			}

			template.Must(template.New(fmt.Sprintf("%010d", mysqlctl.TabletUID)).Parse(`
ssl_ca={{.Dir}}/ca-cert.pem
ssl_cert={{.ServerCert}}
ssl_key={{.ServerKey}}
`)).Execute(fout, sslPathData)
			if err := fout.Close(); err != nil {
				return nil, err
			}

			tmpProcess.Env = append(tmpProcess.Env, "EXTRA_MY_CNF="+extraMyCNF)
			tmpProcess.Env = append(tmpProcess.Env, "VTDATAROOT="+os.Getenv("VTDATAROOT"))
		}

		if init {
			tmpProcess.Args = append(tmpProcess.Args, "init", "--",
				"--init_db_sql_file", mysqlctl.InitDBFile)
		}
	}
	tmpProcess.Args = append(tmpProcess.Args, "start")
	log.Infof("Starting mysqlctl with command: %v", tmpProcess.Args)
	return tmpProcess, tmpProcess.Start()
}

// Stop executes mysqlctl command to stop mysql instance and kills the mysql instance if it doesn't shutdown in 30 seconds.
func (mysqlctl *MysqlctlProcess) Stop() (err error) {
	log.Infof("Shutting down MySQL: %d", mysqlctl.TabletUID)
	defer log.Infof("MySQL shutdown complete: %d", mysqlctl.TabletUID)
	tmpProcess, err := mysqlctl.StopProcess()
	if err != nil {
		return err
	}
	// On the CI it was noticed that MySQL shutdown hangs sometimes and
	// on local investigation it was waiting on SEMI_SYNC acks for an internal command
	// of Vitess even after closing the socket file.
	// To prevent this process for hanging for 5 minutes, we will add a 30-second timeout.
	exit := make(chan error)
	go func() {
		exit <- tmpProcess.Wait()
	}()
	select {
	case <-time.After(30 * time.Second):
		break
	case err := <-exit:
		if err == nil {
			return nil
		}
		break
	}
	pidFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.pid", mysqlctl.TabletUID))
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		// We can't read the file which means the PID file does not exist
		// The server must have stopped
		return nil
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		return err
	}
	return syscall.Kill(pid, syscall.SIGKILL)
}

// StopProcess executes mysqlctl command to stop mysql instance and returns process reference
func (mysqlctl *MysqlctlProcess) StopProcess() (*exec.Cmd, error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"--log_dir", mysqlctl.LogDirectory,
		"--tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
	)
	if *isCoverage {
		tmpProcess.Args = append(tmpProcess.Args, []string{"--test.coverprofile=" + getCoveragePath("mysql-stop.out")}...)
	}
	if len(mysqlctl.ExtraArgs) > 0 {
		tmpProcess.Args = append(tmpProcess.Args, mysqlctl.ExtraArgs...)
	}
	tmpProcess.Args = append(tmpProcess.Args, "shutdown")
	return tmpProcess, tmpProcess.Start()
}

func (mysqlctl *MysqlctlProcess) BasePath() string {
	return path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", mysqlctl.TabletUID))
}

func (mysqlctl *MysqlctlProcess) BinaryLogsPath() string {
	return path.Join(mysqlctl.BasePath(), "bin-logs")
}

// CleanupFiles clean the mysql files to make sure we can start the same process again
func (mysqlctl *MysqlctlProcess) CleanupFiles(tabletUID int) {
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/data", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/relay-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/tmp", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/bin-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/innodb", tabletUID)))
}

// Connect returns a new connection to the underlying MySQL server
func (mysqlctl *MysqlctlProcess) Connect(ctx context.Context, username string) (*mysql.Conn, error) {
	params := mysql.ConnParams{
		Uname:      username,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", mysqlctl.TabletUID), "/mysql.sock"),
	}

	return mysql.Connect(ctx, &params)
}

// MysqlCtlProcessInstanceOptionalInit returns a Mysqlctl handle for mysqlctl process
// configured with the given Config.
func MysqlCtlProcessInstanceOptionalInit(tabletUID int, mySQLPort int, tmpDirectory string, initMySQL bool) *MysqlctlProcess {
	initFile, err := getInitDBFileUsed()
	if err != nil {
		log.Errorf("Couldn't find init db file - %v", err)
	}
	mysqlctl := &MysqlctlProcess{
		Name:         "mysqlctl",
		Binary:       "mysqlctl",
		LogDirectory: tmpDirectory,
		InitDBFile:   initFile,
	}
	mysqlctl.MySQLPort = mySQLPort
	mysqlctl.TabletUID = tabletUID
	mysqlctl.InitMysql = initMySQL
	mysqlctl.SecureTransport = false
	return mysqlctl
}

func getInitDBFileUsed() (string, error) {
	versionStr, err := mysqlctl.GetVersionString()
	if err != nil {
		return "", err
	}
	flavor, _, err := mysqlctl.ParseVersionString(versionStr)
	if err != nil {
		return "", err
	}
	if flavor == mysqlctl.FlavorMySQL || flavor == mysqlctl.FlavorPercona {
		return path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"), nil
	}
	// Non-MySQL instances for example MariaDB, will use init_testserver_db.sql which does not contain super_read_only global variable.
	// Even though MariaDB support is deprecated (https://github.com/vitessio/vitess/issues/9518) but we still support migration scenario.
	return path.Join(os.Getenv("VTROOT"), "go/test/endtoend/vreplication/testdata/config/init_testserver_db.sql"), nil
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
