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
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	vtenv "github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl/mysqlctlclient"
)

const (
	// MysqlWaitTime is the default number of seconds to wait for mysql
	MysqlWaitTime = 120 * time.Second
)

var (
	dbaPoolSize    = flag.Int("dba_pool_size", 10, "Size of the connection pool for dba connections")
	dbaIdleTimeout = flag.Duration("dba_idle_timeout", time.Minute, "Idle timeout for dba connections")
	appPoolSize    = flag.Int("app_pool_size", 40, "Size of the connection pool for app connections")
	appIdleTimeout = flag.Duration("app_idle_timeout", time.Minute, "Idle timeout for app connections")

	socketFile = flag.String("mysqlctl_socket", "", "socket file to use for remote mysqlctl actions (empty for local actions)")
)

// Mysqld is the object that represents a mysqld daemon running on this server.
type Mysqld struct {
	config      *Mycnf
	dba         *mysql.ConnectionParams
	dbApp       *mysql.ConnectionParams
	dbaPool     *dbconnpool.ConnectionPool
	appPool     *dbconnpool.ConnectionPool
	replParams  *mysql.ConnectionParams
	TabletDir   string
	SnapshotDir string

	// mutex protects the fields below.
	mutex         sync.Mutex
	mysqlFlavor   MysqlFlavor
	onTermFuncs   []func()
	cancelWaitCmd chan struct{}
}

// NewMysqld creates a Mysqld object based on the provided configuration
// and connection parameters.
// dbaName and appName are the base for stats exports, use 'Dba' and 'App', except in tests
func NewMysqld(dbaName, appName string, config *Mycnf, dba, app, repl *mysql.ConnectionParams) *Mysqld {
	if *dba == dbconfigs.DefaultDBConfigs.Dba {
		dba.UnixSocket = config.SocketFile
	}

	// create and open the connection pool for dba access
	dbaMysqlStats := stats.NewTimings("Mysql" + dbaName)
	dbaPool := dbconnpool.NewConnectionPool(dbaName+"ConnPool", *dbaPoolSize, *dbaIdleTimeout)
	dbaPool.Open(dbconnpool.DBConnectionCreator(dba, dbaMysqlStats))

	// create and open the connection pool for app access
	appMysqlStats := stats.NewTimings("Mysql" + appName)
	appPool := dbconnpool.NewConnectionPool(appName+"ConnPool", *dbaPoolSize, *dbaIdleTimeout)
	appPool.Open(dbconnpool.DBConnectionCreator(app, appMysqlStats))

	return &Mysqld{
		config:      config,
		dba:         dba,
		dbApp:       app,
		dbaPool:     dbaPool,
		appPool:     appPool,
		replParams:  repl,
		TabletDir:   TabletDir(config.ServerId),
		SnapshotDir: SnapshotDir(config.ServerId),
	}
}

// Cnf returns the mysql config for the daemon
func (mysqld *Mysqld) Cnf() *Mycnf {
	return mysqld.config
}

// Start will start the mysql daemon, either by running the 'mysqld_start'
// hook, or by running mysqld_safe in the background.
// If a mysqlctld address is provided in a flag, Start will run remotely.
func (mysqld *Mysqld) Start(mysqlWaitTime time.Duration) error {
	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.Start() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile, mysqlWaitTime)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.Start(mysqlWaitTime)
	}

	var name string
	ts := fmt.Sprintf("Mysqld.Start(%v)", time.Now().Unix())

	// try the mysqld start hook, if any
	switch hr := hook.NewSimpleHook("mysqld_start").Execute(); hr.ExitStatus {
	case hook.HOOK_SUCCESS:
		// hook exists and worked, we can keep going
		name = "mysqld_start hook"
	case hook.HOOK_DOES_NOT_EXIST:
		// hook doesn't exist, run mysqld_safe ourselves
		log.Infof("%v: No mysqld_start hook, running mysqld_safe directly", ts)
		dir, err := vtenv.VtMysqlRoot()
		if err != nil {
			return err
		}
		name = path.Join(dir, "bin/mysqld_safe")
		arg := []string{
			"--defaults-file=" + mysqld.config.path}
		env := []string{os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql")}

		cmd := exec.Command(name, arg...)
		cmd.Dir = dir
		cmd.Env = env
		log.Infof("%v mysqlWaitTime:%v %#v", ts, mysqlWaitTime, cmd)
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return nil
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil
		}
		go func() {
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				log.Infof("%v stderr: %v", ts, scanner.Text())
			}
		}()
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				log.Infof("%v stdout: %v", ts, scanner.Text())
			}
		}()
		err = cmd.Start()
		if err != nil {
			return nil
		}

		mysqld.mutex.Lock()
		mysqld.cancelWaitCmd = make(chan struct{})
		go func(cancel <-chan struct{}) {
			// Wait regardless of cancel, so we don't generate defunct processes.
			err := cmd.Wait()
			log.Infof("%v exit: %v", ts, err)

			// The process exited. Trigger OnTerm callbacks, unless we were cancelled.
			select {
			case <-cancel:
			default:
				mysqld.mutex.Lock()
				for _, callback := range mysqld.onTermFuncs {
					go callback()
				}
				mysqld.mutex.Unlock()
			}
		}(mysqld.cancelWaitCmd)
		mysqld.mutex.Unlock()
	default:
		// hook failed, we report error
		return fmt.Errorf("mysqld_start hook failed: %v", hr.String())
	}

	// give it some time to succeed - usually by the time the socket emerges
	// we are in good shape
	for i := mysqlWaitTime; i >= 0; i -= time.Second {
		_, statErr := os.Stat(mysqld.config.SocketFile)
		if statErr == nil {
			// Make sure the socket file isn't stale.
			conn, connErr := mysqld.dbaPool.Get(0)
			if connErr == nil {
				conn.Recycle()
				return nil
			}
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
		log.Infof("%v: sleeping for 1s waiting for socket file %v", ts, mysqld.config.SocketFile)
		time.Sleep(time.Second)
	}
	return errors.New(name + ": deadline exceeded waiting for " + mysqld.config.SocketFile)
}

// Shutdown will stop the mysqld daemon that is running in the background.
//
// waitForMysqld: should the function block until mysqld has stopped?
// This can actually take a *long* time if the buffer cache needs to be fully
// flushed - on the order of 20-30 minutes.
//
// If a mysqlctld address is provided in a flag, Shutdown will run remotely.
func (mysqld *Mysqld) Shutdown(waitForMysqld bool, mysqlWaitTime time.Duration) error {
	log.Infof("Mysqld.Shutdown")

	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.Shutdown() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile, mysqlWaitTime)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.Shutdown(waitForMysqld, mysqlWaitTime)
	}

	// We're shutting down on purpose. We no longer want to be notified when
	// mysqld terminates.
	mysqld.mutex.Lock()
	if mysqld.cancelWaitCmd != nil {
		close(mysqld.cancelWaitCmd)
		mysqld.cancelWaitCmd = nil
	}
	mysqld.mutex.Unlock()

	// possibly mysql is already shutdown, check for a few files first
	_, socketPathErr := os.Stat(mysqld.config.SocketFile)
	_, pidPathErr := os.Stat(mysqld.config.PidFile)
	if socketPathErr != nil && pidPathErr != nil {
		log.Warningf("assuming mysqld already shut down - no socket, no pid file found")
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
		log.Infof("No mysqld_shutdown hook, running mysqladmin directly")
		dir, err := vtenv.VtMysqlRoot()
		if err != nil {
			return err
		}
		name := path.Join(dir, "bin/mysqladmin")
		arg := []string{
			"-u", "vt_dba", "-S", mysqld.config.SocketFile,
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
			_, statErr := os.Stat(mysqld.config.SocketFile)
			if statErr != nil && os.IsNotExist(statErr) {
				return nil
			}
			log.Infof("Mysqld.Shutdown: sleeping for 1s waiting for socket file %v", mysqld.config.SocketFile)
			time.Sleep(time.Second)
		}
		return errors.New("gave up waiting for mysqld to stop")
	}
	return nil
}

/* exec and wait for a return code. look for name in $PATH. */
func execCmd(name string, args, env []string, dir string) (cmd *exec.Cmd, err error) {
	cmdPath, _ := exec.LookPath(name)
	log.Infof("execCmd: %v %v %v", name, cmdPath, args)

	cmd = exec.Command(cmdPath, args...)
	cmd.Env = env
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		err = errors.New(name + ": " + string(output))
	}
	log.Infof("execCmd: command returned: %v", string(output))
	return cmd, err
}

// Init will create the default directory structure for the mysqld process,
// generate / configure a my.cnf file, unpack a skeleton database,
// and create some management tables.
func (mysqld *Mysqld) Init(mysqlWaitTime time.Duration, bootstrapArchive string, skipSchema bool) error {
	log.Infof("mysqlctl.Init")
	err := mysqld.createDirs()
	if err != nil {
		log.Errorf("%s", err.Error())
		return err
	}
	root, err := vtenv.VtRoot()
	if err != nil {
		log.Errorf("%s", err.Error())
		return err
	}

	// Set up config files.
	if err = mysqld.initConfig(root); err != nil {
		log.Errorf("failed creating %v: %v", mysqld.config.path, err)
		return err
	}

	// Unpack bootstrap DB files.
	dbTbzPath := path.Join(root, "data/bootstrap/"+bootstrapArchive)
	log.Infof("decompress bootstrap db %v", dbTbzPath)
	args := []string{"-xj", "-C", mysqld.TabletDir, "-f", dbTbzPath}
	if _, err = execCmd("tar", args, []string{}, ""); err != nil {
		log.Errorf("failed unpacking %v: %v", dbTbzPath, err)
		return err
	}

	// Start mysqld.
	if err = mysqld.Start(mysqlWaitTime); err != nil {
		log.Errorf("failed starting, check %v", mysqld.config.ErrorLogPath)
		return err
	}

	// Load initial schema.
	if skipSchema {
		return nil
	}
	schemaPath := path.Join(root, "data/bootstrap/_vt_schema.sql")
	schema, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	sqlCmds := make([]string, 0, 10)
	log.Infof("initial schema: %v", string(schema))
	for _, cmd := range strings.Split(string(schema), ";") {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		sqlCmds = append(sqlCmds, cmd)
	}

	return mysqld.ExecuteSuperQueryList(sqlCmds)
}

func (mysqld *Mysqld) initConfig(root string) error {
	var err error
	var configData string

	switch hr := hook.NewSimpleHook("make_mycnf").Execute(); hr.ExitStatus {
	case hook.HOOK_DOES_NOT_EXIST:
		log.Infof("make_mycnf hook doesn't exist, reading default template files")
		cnfTemplatePaths := []string{
			path.Join(root, "config/mycnf/default.cnf"),
			path.Join(root, "config/mycnf/master.cnf"),
			path.Join(root, "config/mycnf/replica.cnf"),
		}

		if extraCnf := os.Getenv("EXTRA_MY_CNF"); extraCnf != "" {
			parts := strings.Split(extraCnf, ":")
			cnfTemplatePaths = append(cnfTemplatePaths, parts...)
		}

		configData, err = mysqld.config.makeMycnf(cnfTemplatePaths)
	case hook.HOOK_SUCCESS:
		configData, err = mysqld.config.fillMycnfTemplate(hr.Stdout)
	default:
		return fmt.Errorf("make_mycnf hook failed(%v): %v", hr.ExitStatus, hr.Stderr)
	}
	if err != nil {
		return err
	}

	return ioutil.WriteFile(mysqld.config.path, []byte(configData), 0664)
}

func (mysqld *Mysqld) createDirs() error {
	log.Infof("creating directory %s", mysqld.TabletDir)
	if err := os.MkdirAll(mysqld.TabletDir, 0775); err != nil {
		return err
	}
	for _, dir := range TopLevelDirs() {
		if err := mysqld.createTopDir(dir); err != nil {
			return err
		}
	}
	for _, dir := range mysqld.config.directoryList() {
		log.Infof("creating directory %s", dir)
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
func (mysqld *Mysqld) createTopDir(dir string) error {
	vtname := path.Base(mysqld.TabletDir)
	target := path.Join(vtenv.VtDataRoot(), dir)
	_, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			topdir := path.Join(mysqld.TabletDir, dir)
			log.Infof("creating directory %s", topdir)
			return os.MkdirAll(topdir, 0775)
		}
		return err
	}
	linkto := path.Join(target, vtname)
	source := path.Join(mysqld.TabletDir, dir)
	log.Infof("creating directory %s", linkto)
	err = os.MkdirAll(linkto, 0775)
	if err != nil {
		return err
	}
	log.Infof("creating symlink %s -> %s", source, linkto)
	return os.Symlink(linkto, source)
}

// Teardown will shutdown the running daemon, and delete the root directory.
func (mysqld *Mysqld) Teardown(force bool) error {
	log.Infof("mysqlctl.Teardown")
	if err := mysqld.Shutdown(true, MysqlWaitTime); err != nil {
		log.Warningf("failed mysqld shutdown: %v", err.Error())
		if !force {
			return err
		}
	}
	var removalErr error
	for _, dir := range TopLevelDirs() {
		qdir := path.Join(mysqld.TabletDir, dir)
		if err := deleteTopDir(qdir); err != nil {
			removalErr = err
		}
	}
	return removalErr
}

func deleteTopDir(dir string) (removalErr error) {
	fi, err := os.Lstat(dir)
	if err != nil {
		log.Errorf("error deleting dir %v: %v", dir, err.Error())
		removalErr = err
	} else if fi.Mode()&os.ModeSymlink != 0 {
		target, err := filepath.EvalSymlinks(dir)
		if err != nil {
			log.Errorf("could not resolve symlink %v: %v", dir, err.Error())
			removalErr = err
		}
		log.Infof("remove data dir (symlinked) %v", target)
		if err = os.RemoveAll(target); err != nil {
			log.Errorf("failed removing %v: %v", target, err.Error())
			removalErr = err
		}
	}
	log.Infof("remove data dir %v", dir)
	if err = os.RemoveAll(dir); err != nil {
		log.Errorf("failed removing %v: %v", dir, err.Error())
		removalErr = err
	}
	return
}

// Addr returns the fully qualified host name + port for this instance.
func (mysqld *Mysqld) Addr() string {
	hostname := netutil.FullyQualifiedHostnameOrPanic()
	return netutil.JoinHostPort(hostname, mysqld.config.MysqlPort)
}

// IPAddr returns the IP address for this instance
func (mysqld *Mysqld) IPAddr() string {
	addr, err := netutil.ResolveIpAddr(mysqld.Addr())
	if err != nil {
		panic(err) // should never happen
	}
	return addr
}

// ExecuteMysqlCommand executes some SQL commands using a mysql command line interface process
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

// GetDbaConnection returns a connection from the dba pool.
// Recycle needs to be called on the result.
func (mysqld *Mysqld) GetDbaConnection() (dbconnpool.PoolConnection, error) {
	return mysqld.dbaPool.Get(0)
}

// GetAppConnection returns a connection from the app pool.
// Recycle needs to be called on the result.
func (mysqld *Mysqld) GetAppConnection() (dbconnpool.PoolConnection, error) {
	return mysqld.appPool.Get(0)
}

// Close will close this instance of Mysqld. It will wait for all dba
// queries to be finished.
func (mysqld *Mysqld) Close() {
	mysqld.dbaPool.Close()
	mysqld.appPool.Close()
}

// OnTerm registers a function to be called if mysqld terminates for any
// reason other than a call to Mysqld.Shutdown(). This only works if mysqld
// was actually started by calling Start() on this Mysqld instance.
func (mysqld *Mysqld) OnTerm(f func()) {
	mysqld.mutex.Lock()
	defer mysqld.mutex.Unlock()
	mysqld.onTermFuncs = append(mysqld.onTermFuncs, f)
}
