// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Commands for controlling an external mysql process. 

Some commands are issued as exec'd tools, some are handled by connecting via
the mysql protocol.
*/

package mysqlctl

import (
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/relog"
)

const (
	MysqlWaitTime = 20 // number of seconds to wait
)

type CreateConnection func() (*mysql.Connection, error)

type Mysqld struct {
	config           *Mycnf
	dbaParams        mysql.ConnectionParams
	replParams       mysql.ConnectionParams
	createConnection CreateConnection
}

func NewMysqld(config *Mycnf, dba, repl mysql.ConnectionParams) *Mysqld {
	createSuperConnection := func() (*mysql.Connection, error) {
		return mysql.Connect(dba)
	}

	return &Mysqld{config, dba, repl, createSuperConnection}
}

func Start(mt *Mysqld) error {
	relog.Info("mysqlctl.Start")
	dir := os.ExpandEnv("$VTROOT/dist/vt-mysql")
	name := dir + "/bin/mysqld_safe"
	arg := []string{
		"--defaults-file=" + mt.config.MycnfFile}
	env := []string{
		os.ExpandEnv("LD_LIBRARY_PATH=$VTROOT/dist/vt-mysql/lib/mysql"),
	}

	cmd := exec.Command(name, arg...)
	cmd.Dir = dir
	cmd.Env = env
	relog.Info("Start %v", cmd)
	_, err := cmd.StderrPipe()
	if err != nil {
		return nil
	}
	err = cmd.Start()
	if err != nil {
		return nil
	}

	// wait so we don't get a bunch of defunct processes
	go cmd.Wait()

	// give it some time to succeed - usually by the time the socket emerges
	// we are in good shape
	for i := 0; i < MysqlWaitTime; i++ {
		time.Sleep(1e9)
		_, statErr := os.Stat(mt.config.SocketFile)
		if statErr == nil {
			return nil
		} else if statErr.(*os.PathError).Err != syscall.ENOENT {
			return statErr
		}
	}
	return errors.New(name + ": deadline exceeded waiting for " + mt.config.SocketFile)
}

/* waitForMysqld: should the function block until mysqld has stopped?
This can actually take a *long* time if the buffer cache needs to be fully
flushed - on the order of 20-30 minutes.
*/
func Shutdown(mt *Mysqld, waitForMysqld bool) error {
	relog.Info("mysqlctl.Shutdown")
	// possibly mysql is already shutdown, check for a few files first
	_, socketPathErr := os.Stat(mt.config.SocketFile)
	_, pidPathErr := os.Stat(mt.config.PidFile())
	if socketPathErr != nil && pidPathErr != nil {
		relog.Warning("assuming shutdown - no socket, no pid file")
		return nil
	}

	dir := os.ExpandEnv("$VTROOT/dist/vt-mysql")
	name := dir + "/bin/mysqladmin"
	arg := []string{
		"-u", "vt_dba", "-S", mt.config.SocketFile,
		"shutdown"}
	env := []string{
		os.ExpandEnv("LD_LIBRARY_PATH=$VTROOT/dist/vt-mysql/lib/mysql"),
	}
	_, err := execCmd(name, arg, env, dir)
	if err != nil {
		return err
	}

	// wait for mysqld to really stop. use the sock file as a proxy for that since
	// we can't call wait() in a process we didn't start.
	if waitForMysqld {
		for i := 0; i < MysqlWaitTime; i++ {
			_, statErr := os.Stat(mt.config.SocketFile)
			// NOTE: dreaded PathError :(
			if statErr != nil && statErr.(*os.PathError).Err == syscall.ENOENT {
				return nil
			}
			time.Sleep(1e9)
		}
		return errors.New("gave up waiting for mysqld to stop")
	}
	return nil
}

/* exec and wait for a return code. look for name in $PATH. */
func execCmd(name string, args, env []string, dir string) (cmd *exec.Cmd, err error) {
	cmdPath, _ := exec.LookPath(name)
	relog.Info("execCmd: %v %v %v", name, cmdPath, args)

	cmd = exec.Command(cmdPath, args...)
	cmd.Env = env
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		err = errors.New(name + ": " + string(output))
	}
	return cmd, err
}

func Init(mt *Mysqld) error {
	relog.Info("mysqlctl.Init")
	for _, path := range mt.config.DirectoryList() {
		if err := os.MkdirAll(path, 0775); err != nil {
			relog.Error("%s", err.Error())
			return err
		}
		// FIXME(msolomon) validate permissions?
	}

	cnfTemplatePath := os.ExpandEnv("$VTROOT/src/code.google.com/p/vitess/config/mycnf")
	configData, err := MakeMycnfForMysqld(mt, cnfTemplatePath, "tablet uid?")
	if err == nil {
		err = ioutil.WriteFile(mt.config.MycnfFile, []byte(configData), 0664)
	}
	if err != nil {
		relog.Error("failed creating %v: %v", mt.config.MycnfFile, err)
		return err
	}

	dbTbzPath := os.ExpandEnv("$VTROOT/src/code.google.com/p/vitess/data/bootstrap/mysql-db-dir.tbz")
	relog.Info("decompress bootstrap db %v", dbTbzPath)
	args := []string{"-xj", "-C", mt.config.DataDir, "-f", dbTbzPath}
	_, tarErr := execCmd("tar", args, []string{}, "")
	if tarErr != nil {
		relog.Error("failed unpacking %v: %v", dbTbzPath, tarErr)
		return tarErr
	}
	if err = Start(mt); err != nil {
		relog.Error("failed starting, check %v", mt.config.ErrorLogPath())
		return err
	}
	schemaPath := os.ExpandEnv("$VTROOT/src/code.google.com/p/vitess/data/bootstrap/_vt_schema.sql")
	schema, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	sqlCmds := make([]string, 0, 10)
	relog.Info("initial schema: %v", string(schema))
	for _, cmd := range strings.Split(string(schema), ";") {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		sqlCmds = append(sqlCmds, cmd)
	}

	return mt.executeSuperQueryList(sqlCmds)
}

func Teardown(mt *Mysqld, force bool) error {
	relog.Info("mysqlctl.Teardown")
	if err := Shutdown(mt, true); err != nil {
		if !force {
			relog.Error("failed mysqld shutdown: %v", err.Error())
			return err
		} else {
			relog.Warning("failed mysqld shutdown: %v", err.Error())
		}
	}
	var removalErr error
	for _, dir := range mt.config.DirectoryList() {
		relog.Info("remove data dir %v", dir)
		if err := os.RemoveAll(dir); err != nil {
			relog.Error("failed removing %v: %v", dir, err.Error())
			removalErr = err
		}
	}
	return removalErr
}

func Reinit(mt *Mysqld) error {
	if err := Teardown(mt, false); err != nil {
		return err
	}
	return Init(mt)
}

func (mysqld *Mysqld) Addr() string {
	return mysqld.config.MysqlAddr()
}
