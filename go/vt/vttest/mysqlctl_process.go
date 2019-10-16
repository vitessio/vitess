/*
Copyright 2017 GitHub Inc.

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

package vttest

import (
	"fmt"
	"os"
	"os/exec"
	"path"
)

// MysqlctlProcess is a generic handle for a running mysqlctl command .
// It can be spawned manually
type MysqlctlProcess struct {
	Name         string
	Binary       string
	LogDirectory string
	TabletUID    int
	MySqlPort    int
	InitDBFile   string
}

// InitDb executes mysqlctl command to add cell info
func (mysqlctl *MysqlctlProcess) InitDb() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySqlPort),
		"init",
		"-init_db_sql_file", mysqlctl.InitDBFile,
	)
	return tmpProcess.Run()
}

// InitDb executes mysqlctl command to add cell info
func (mysqlctl *MysqlctlProcess) Start() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySqlPort),
		"init",
		"-init_db_sql_file", mysqlctl.InitDBFile,
	)
	return tmpProcess.Run()
}

// InitDb executes mysqlctl command to add cell info
func (mysqlctl *MysqlctlProcess) Stop() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"shutdown",
	)
	return tmpProcess.Run()
}

// MysqlCtlProcessInstance returns a Mysqlctl handle for mysqlctl process
// configured with the given Config.
func MysqlCtlProcessInstance(TabletUID int, MySqlPort int) *MysqlctlProcess {
	mysqlctl := &MysqlctlProcess{
		Name:         "mysqlctl",
		Binary:       "mysqlctl",
		LogDirectory: path.Join(os.Getenv("VTDATAROOT"), "/tmp"),
		InitDBFile:   path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"),
	}
	mysqlctl.MySqlPort = MySqlPort
	mysqlctl.TabletUID = TabletUID
	return mysqlctl
}
