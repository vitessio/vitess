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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/relog"
	vtenv "code.google.com/p/vitess/go/vt/env"
	"code.google.com/p/vitess/go/vt/hook"
)

const (
	MysqlWaitTime = 120 * time.Second // default number of seconds to wait
)

type CreateConnection func() (*mysql.Connection, error)

var DefaultDbaParams = mysql.ConnectionParams{
	Uname:   "vt_dba",
	Charset: "utf8",
}

var DefaultReplParams = mysql.ConnectionParams{
	Uname:   "vt_repl",
	Charset: "utf8",
}

type Mysqld struct {
	config           *Mycnf
	dbaParams        mysql.ConnectionParams
	replParams       mysql.ConnectionParams
	createConnection CreateConnection
	TabletDir        string
	SnapshotDir      string
}

func NewMysqld(config *Mycnf, dba, repl mysql.ConnectionParams) *Mysqld {
	if dba == DefaultDbaParams {
		dba.UnixSocket = config.SocketFile
	}

	// the super connection is not linked to a specific database
	// (allows us to create them)
	superParams := dba
	superParams.Dbname = ""
	createSuperConnection := func() (*mysql.Connection, error) {
		return mysql.Connect(superParams)
	}
	return &Mysqld{config,
		dba,
		repl,
		createSuperConnection,
		TabletDir(config.ServerId),
		SnapshotDir(config.ServerId),
	}
}

func Start(mt *Mysqld, mysqlWaitTime time.Duration) error {
	var name string

	// try the mysqld start hook, if any
	h := hook.NewSimpleHook("mysqld_start")
	hr := h.Execute()
	switch hr.ExitStatus {
	case hook.HOOK_SUCCESS:
		// hook exists and worked, we can keep going
		name = "mysqld_start hook"
	case hook.HOOK_DOES_NOT_EXIST:
		// hook doesn't exist, run mysqld_safe ourselves
		relog.Info("No mysqld_start hook, running mysqld_safe directly")
		dir, err := vtenv.VtMysqlRoot()
		if err != nil {
			return err
		}
		name = path.Join(dir, "bin/mysqld_safe")
		arg := []string{
			"--defaults-file=" + mt.config.path}
		env := []string{os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql")}

		cmd := exec.Command(name, arg...)
		cmd.Dir = dir
		cmd.Env = env
		relog.Info("mysqlctl.Start mysqlWaitTime:%v %#v", mysqlWaitTime, cmd)
		_, err = cmd.StderrPipe()
		if err != nil {
			return nil
		}
		err = cmd.Start()
		if err != nil {
			return nil
		}

		// wait so we don't get a bunch of defunct processes
		go cmd.Wait()
	default:
		// hook failed, we report error
		return fmt.Errorf("mysqld_start hook failed: %v", hr.String())
	}

	// give it some time to succeed - usually by the time the socket emerges
	// we are in good shape
	for i := mysqlWaitTime; i >= 0; i -= time.Second {
		_, statErr := os.Stat(mt.config.SocketFile)
		if statErr == nil {
			// Make sure the socket file isn't stale.
			conn, connErr := mt.createConnection()
			if connErr == nil {
				conn.Close()
				return nil
			}
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
		time.Sleep(time.Second)
	}
	return errors.New(name + ": deadline exceeded waiting for " + mt.config.SocketFile)
}

/* waitForMysqld: should the function block until mysqld has stopped?
This can actually take a *long* time if the buffer cache needs to be fully
flushed - on the order of 20-30 minutes.
*/
func Shutdown(mt *Mysqld, waitForMysqld bool, mysqlWaitTime time.Duration) error {
	relog.Info("mysqlctl.Shutdown")
	// possibly mysql is already shutdown, check for a few files first
	_, socketPathErr := os.Stat(mt.config.SocketFile)
	_, pidPathErr := os.Stat(mt.config.PidFile)
	if socketPathErr != nil && pidPathErr != nil {
		relog.Warning("assuming shutdown - no socket, no pid file")
		return nil
	}

	// try the mysqld shutdown hook, if any
	h := hook.NewSimpleHook("mysqld_shutdown")
	hr := h.Execute()
	switch hr.ExitStatus {
	case hook.HOOK_SUCCESS:
		// hook exists and worked, we can keep going
	case hook.HOOK_DOES_NOT_EXIST:
		// hook doesn't exist, try mysqladmin
		relog.Info("No mysqld_shutdown hook, running mysqladmin directly")
		dir, err := vtenv.VtMysqlRoot()
		if err != nil {
			return err
		}
		name := path.Join(dir, "bin/mysqladmin")
		arg := []string{
			"-u", "vt_dba", "-S", mt.config.SocketFile,
			"shutdown"}
		env := []string{
			os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql"),
		}
		_, err = execCmd(name, arg, env, dir)
		if err != nil {
			return err
		}
	default:
		// hook failed, we report error
		return fmt.Errorf("mysqld_shutdown hook failed: %v", hr.String())
	}

	// wait for mysqld to really stop. use the sock file as a proxy for that since
	// we can't call wait() in a process we didn't start.
	if waitForMysqld {
		for i := mysqlWaitTime; i >= 0; i -= time.Second {
			_, statErr := os.Stat(mt.config.SocketFile)
			if statErr != nil && os.IsNotExist(statErr) {
				return nil
			}
			time.Sleep(time.Second)
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

func Init(mt *Mysqld, mysqlWaitTime time.Duration) error {
	relog.Info("mysqlctl.Init")
	err := mt.createDirs()
	if err != nil {
		relog.Error("%s", err.Error())
		return err
	}
	root, err := vtenv.VtRoot()
	if err != nil {
		relog.Error("%s", err.Error())
		return err
	}

	hr := hook.NewSimpleHook("make_mycnf").Execute()

	configData := ""
	if hr.ExitStatus == hook.HOOK_DOES_NOT_EXIST {
		relog.Info("make_mycnf hook doesn't exist")
		cnfTemplatePaths := []string{
			path.Join(root, "config/mycnf/default.cnf"),
			path.Join(root, "config/mycnf/master.cnf"),
			path.Join(root, "config/mycnf/replica.cnf"),
		}

		if extraCnf := os.Getenv("EXTRA_MY_CNF"); extraCnf != "" {
			parts := strings.Split(extraCnf, ":")
			cnfTemplatePaths = append(cnfTemplatePaths, parts...)
		}

		configData, err = MakeMycnf(mt.config, cnfTemplatePaths)
	} else if hr.ExitStatus == hook.HOOK_SUCCESS {
		configData, err = fillMycnfTemplate(mt.config, hr.Stdout)
	} else {
		err = fmt.Errorf("make_mycnf hook failed(%v): %v", hr.ExitStatus, hr.Stderr)
	}

	if err == nil {
		err = ioutil.WriteFile(mt.config.path, []byte(configData), 0664)
	}
	if err != nil {
		relog.Error("failed creating %v: %v", mt.config.path, err)
		return err
	}

	dbTbzPath := path.Join(root, "data/bootstrap/mysql-db-dir.tbz")
	relog.Info("decompress bootstrap db %v", dbTbzPath)
	args := []string{"-xj", "-C", mt.config.DataDir, "-f", dbTbzPath}
	_, tarErr := execCmd("tar", args, []string{}, "")
	if tarErr != nil {
		relog.Error("failed unpacking %v: %v", dbTbzPath, tarErr)
		return tarErr
	}
	if err = Start(mt, mysqlWaitTime); err != nil {
		relog.Error("failed starting, check %v", mt.config.ErrorLogPath)
		return err
	}
	schemaPath := path.Join(root, "data/bootstrap/_vt_schema.sql")
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

func (mt *Mysqld) createDirs() error {
	relog.Info("creating directory %s", mt.TabletDir)
	if err := os.MkdirAll(mt.TabletDir, 0775); err != nil {
		return err
	}
	for _, dir := range TopLevelDirs() {
		if err := mt.createTopDir(dir); err != nil {
			return err
		}
	}
	for _, dir := range DirectoryList(mt.config) {
		relog.Info("creating directory %s", dir)
		if err := os.MkdirAll(dir, 0775); err != nil {
			return err
		}
		// FIXME(msolomon) validate permissions?
	}
	return nil
}

// createTopDir creates a top level directory under TabletDir.
// However, if a directory of the same name already exists under
// vtenv.VtDataRoot(), it creates a directory named after the tablet
// id under that directory, and then creates a symlink under TabletDir
// that points to the newly created directory.  For example, if
// /vt/data is present, it will create the following structure:
// /vt/data/vt_xxxx /vt/vt_xxxx/data -> /vt/data/vt_xxxx
func (mt *Mysqld) createTopDir(dir string) error {
	vtname := path.Base(mt.TabletDir)
	target := path.Join(vtenv.VtDataRoot(), dir)
	_, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			topdir := path.Join(mt.TabletDir, dir)
			relog.Info("creating directory %s", topdir)
			return os.MkdirAll(topdir, 0775)
		}
		return err
	}
	linkto := path.Join(target, vtname)
	source := path.Join(mt.TabletDir, dir)
	relog.Info("creating directory %s", linkto)
	err = os.MkdirAll(linkto, 0775)
	if err != nil {
		return err
	}
	relog.Info("creating symlink %s -> %s", source, linkto)
	return os.Symlink(linkto, source)
}

func Teardown(mt *Mysqld, force bool) error {
	relog.Info("mysqlctl.Teardown")
	if err := Shutdown(mt, true, MysqlWaitTime); err != nil {
		relog.Warning("failed mysqld shutdown: %v", err.Error())
		if !force {
			return err
		}
	}
	var removalErr error
	for _, dir := range TopLevelDirs() {
		qdir := path.Join(mt.TabletDir, dir)
		if err := deleteTopDir(qdir); err != nil {
			removalErr = err
		}
	}
	return removalErr
}

func deleteTopDir(dir string) (removalErr error) {
	fi, err := os.Lstat(dir)
	if err != nil {
		relog.Error("error deleting dir %v: %v", dir, err.Error())
		removalErr = err
	} else if fi.Mode()&os.ModeSymlink != 0 {
		target, err := filepath.EvalSymlinks(dir)
		if err != nil {
			relog.Error("could not resolve symlink %v: %v", dir, err.Error())
			removalErr = err
		}
		relog.Info("remove data dir (symlinked) %v", target)
		if err = os.RemoveAll(target); err != nil {
			relog.Error("failed removing %v: %v", target, err.Error())
			removalErr = err
		}
	}
	relog.Info("remove data dir %v", dir)
	if err = os.RemoveAll(dir); err != nil {
		relog.Error("failed removing %v: %v", dir, err.Error())
		removalErr = err
	}
	return
}

func (mysqld *Mysqld) Addr() string {
	hostname := netutil.FullyQualifiedHostnameOrPanic()
	return fmt.Sprintf("%v:%v", hostname, mysqld.config.MysqlPort)
}

func (mysqld *Mysqld) IpAddr() string {
	addr, err := netutil.ResolveIpAddr(mysqld.Addr())
	if err != nil {
		panic(err) // should never happen
	}
	return addr
}

// executes some SQL commands using a mysql command line interface process
func (mysqld *Mysqld) ExecuteMysqlCommand(sql string) error {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return err
	}
	name := path.Join(dir, "bin/mysql")
	arg := []string{
		"-u", "vt_dba", "-S", mysqld.config.SocketFile,
		"-e", sql}
	env := []string{
		"LD_LIBRARY_PATH=" + path.Join(dir, "lib/mysql"),
	}
	_, err = execCmd(name, arg, env, dir)
	if err != nil {
		return err
	}
	return nil
}
