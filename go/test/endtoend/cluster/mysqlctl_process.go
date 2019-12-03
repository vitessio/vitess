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

	"vitess.io/vitess/go/mysql"
)

// MysqlctlProcess is a generic handle for a running mysqlctl command .
// It can be spawned manually
type MysqlctlProcess struct {
	Name         string
	Binary       string
	LogDirectory string
	TabletUID    int
	MySQLPort    int
	InitDBFile   string
}

// InitDb executes mysqlctl command to add cell info
func (mysqlctl *MysqlctlProcess) InitDb() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
		"init",
		"-init_db_sql_file", mysqlctl.InitDBFile,
	)
	return tmpProcess.Run()
}

func (mysqlctl *MysqlctlProcess) Start() error {
	return mysqlctl.StartWithArgs("")
}

// StartWithArgs executes mysqlctl command to start mysql instance with arguments
func (mysqlctl *MysqlctlProcess) StartWithArgs(extraCnf string, extraArgs ...string) (err error) {
	if extraCnf != "" {
		os.Setenv("EXTRA_MY_CNF", extraCnf)
	}
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
	)
	if len(extraArgs) > 0 {
		tmpProcess.Args = append(tmpProcess.Args, extraArgs...)
	}
	tmpProcess.Args = append(tmpProcess.Args, "init",
		"-init_db_sql_file", mysqlctl.InitDBFile)
	return tmpProcess.Run()
}

// Stop executes mysqlctl command to stop mysql instance
func (mysqlctl *MysqlctlProcess) Stop() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"shutdown",
	)
	return tmpProcess.Run()
}

// CleanupFiles clean the mysql files to make sure we can start the same process again
func (mysqlctl *MysqlctlProcess) CleanupFiles(tabletUID int) {
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/data", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/relay-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/tmp", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/bin-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/innodb", tabletUID)))
}

// MysqlCtlProcessInstance returns a Mysqlctl handle for mysqlctl process
// configured with the given Config.
func MysqlCtlProcessInstance(tabletUID int, mySQLPort int, tmpDirectory string) *MysqlctlProcess {
	mysqlctl := &MysqlctlProcess{
		Name:         "mysqlctl",
		Binary:       "mysqlctl",
		LogDirectory: tmpDirectory,
		InitDBFile:   path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"),
	}
	mysqlctl.MySQLPort = mySQLPort
	mysqlctl.TabletUID = tabletUID
	return mysqlctl
}

// StartMySQL create a connection to tablet mysql
func StartMySQL(ctx context.Context, tablet *Vttablet, username string, tmpDirectory string) (*mysql.Conn, error) {
	tablet.MysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, tmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	if err != nil {
		return nil, err
	}
	params := mysql.ConnParams{
		Uname:      username,
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tablet.TabletUID), "/mysql.sock"),
	}

	conn, err := mysql.Connect(ctx, &params)
	return conn, err
}
