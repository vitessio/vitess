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

package vttest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
)

// MySQLManager is an interface to a mysqld process manager, capable
// of starting/shutting down mysqld services and initializing them.
type MySQLManager interface {
	Setup() error
	Start() error
	TearDown() error
	Auth() (string, string)
	Address() (string, int)
	UnixSocket() string
	TabletDir() string
	Params(dbname string) mysql.ConnParams
}

// Mysqlctl implements MySQLManager through Vitess' mysqlctld tool
type Mysqlctl struct {
	Binary    string
	InitFile  string
	Directory string
	Port      int
	MyCnf     []string
	Env       []string
	UID       uint32
}

// Setup spawns a new mysqld service and initializes it with the defaults.
// The service is kept running in the background until TearDown() is called.
func (ctl *Mysqlctl) Setup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		ctl.Binary,
		"-alsologtostderr",
		"-tablet_uid", fmt.Sprintf("%d", ctl.UID),
		"-mysql_port", fmt.Sprintf("%d", ctl.Port),
		"init",
		"-init_db_sql_file", ctl.InitFile,
	)

	myCnf := strings.Join(ctl.MyCnf, ":")

	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, ctl.Env...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("EXTRA_MY_CNF=%s", myCnf))

	_, err := cmd.Output()
	return err
}

// Start spawns a mysqld service for an existing data directory
// The service is kept running in the background until TearDown() is called.
func (ctl *Mysqlctl) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		ctl.Binary,
		"-alsologtostderr",
		"-tablet_uid", fmt.Sprintf("%d", ctl.UID),
		"-mysql_port", fmt.Sprintf("%d", ctl.Port),
		"start",
	)

	myCnf := strings.Join(ctl.MyCnf, ":")

	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, ctl.Env...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("EXTRA_MY_CNF=%s", myCnf))

	_, err := cmd.Output()
	return err
}

// TearDown shutdowns the running mysqld service
func (ctl *Mysqlctl) TearDown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		ctl.Binary,
		"-alsologtostderr",
		"-tablet_uid", fmt.Sprintf("%d", ctl.UID),
		"-mysql_port", fmt.Sprintf("%d", ctl.Port),
		"shutdown",
	)

	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, ctl.Env...)

	_, err := cmd.Output()
	return err
}

// Auth returns the username/password tuple required to log in to mysqld
func (ctl *Mysqlctl) Auth() (string, string) {
	return "vt_dba", ""
}

// Address returns the hostname/tcp port pair required to connect to mysqld
func (ctl *Mysqlctl) Address() (string, int) {
	return "", ctl.Port
}

// UnixSocket returns the path to the local Unix socket required to connect to mysqld
func (ctl *Mysqlctl) UnixSocket() string {
	return path.Join(ctl.TabletDir(), "mysql.sock")
}

// TabletDir returns the path where data for this Tablet would be stored
func (ctl *Mysqlctl) TabletDir() string {
	return mysqlctl.DefaultTabletDirAtRoot(ctl.Directory, ctl.UID)
}

// Params returns the mysql.ConnParams required to connect directly to mysqld
// using Vitess' mysql client.
func (ctl *Mysqlctl) Params(dbname string) mysql.ConnParams {
	return mysql.ConnParams{
		Charset:    DefaultCharset,
		DbName:     dbname,
		Uname:      "vt_dba",
		UnixSocket: ctl.UnixSocket(),
	}
}
