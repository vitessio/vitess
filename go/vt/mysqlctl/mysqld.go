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
	"io"
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
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	vtenv "github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl/mysqlctlclient"
	"golang.org/x/net/context"
)

var (
	dbaPoolSize    = flag.Int("dba_pool_size", 20, "Size of the connection pool for dba connections")
	dbaIdleTimeout = flag.Duration("dba_idle_timeout", time.Minute, "Idle timeout for dba connections")
	appPoolSize    = flag.Int("app_pool_size", 40, "Size of the connection pool for app connections")
	appIdleTimeout = flag.Duration("app_idle_timeout", time.Minute, "Idle timeout for app connections")

	socketFile = flag.String("mysqlctl_socket", "", "socket file to use for remote mysqlctl actions (empty for local actions)")

	// masterConnectRetry is used in 'SET MASTER' commands
	masterConnectRetry = flag.Duration("master_connect_retry", 10*time.Second, "how long to wait in between slave -> connection attempts. Only precise to the second.")
)

// Mysqld is the object that represents a mysqld daemon running on this server.
type Mysqld struct {
	config        *Mycnf
	dba           *sqldb.ConnParams
	dbApp         *sqldb.ConnParams
	dbaPool       *dbconnpool.ConnectionPool
	appPool       *dbconnpool.ConnectionPool
	replParams    *sqldb.ConnParams
	dbaMysqlStats *stats.Timings
	tabletDir     string

	// mutex protects the fields below.
	mutex         sync.Mutex
	mysqlFlavor   MysqlFlavor
	onTermFuncs   []func()
	cancelWaitCmd chan struct{}
}

// NewMysqld creates a Mysqld object based on the provided configuration
// and connection parameters.
// dbaName and appName are the base for stats exports, use 'Dba' and 'App', except in tests
func NewMysqld(dbaName, appName string, config *Mycnf, dba, app, repl *sqldb.ConnParams) *Mysqld {
	if *dba == dbconfigs.DefaultDBConfigs.Dba {
		dba.UnixSocket = config.SocketFile
	}

	// create and open the connection pool for dba access
	dbaMysqlStatsName := ""
	dbaPoolName := ""
	if dbaName != "" {
		dbaMysqlStatsName = "Mysql" + dbaName
		dbaPoolName = dbaName + "ConnPool"
	}
	dbaMysqlStats := stats.NewTimings(dbaMysqlStatsName)
	dbaPool := dbconnpool.NewConnectionPool(dbaPoolName, *dbaPoolSize, *dbaIdleTimeout)
	dbaPool.Open(dbconnpool.DBConnectionCreator(dba, dbaMysqlStats))

	// create and open the connection pool for app access
	appMysqlStatsName := ""
	appPoolName := ""
	if appName != "" {
		appMysqlStatsName = "Mysql" + appName
		appPoolName = appName + "ConnPool"
	}
	appMysqlStats := stats.NewTimings(appMysqlStatsName)
	appPool := dbconnpool.NewConnectionPool(appPoolName, *appPoolSize, *appIdleTimeout)
	appPool.Open(dbconnpool.DBConnectionCreator(app, appMysqlStats))

	return &Mysqld{
		config:        config,
		dba:           dba,
		dbApp:         app,
		dbaPool:       dbaPool,
		appPool:       appPool,
		replParams:    repl,
		dbaMysqlStats: dbaMysqlStats,
		tabletDir:     path.Dir(config.DataDir),
	}
}

// Cnf returns the mysql config for the daemon
func (mysqld *Mysqld) Cnf() *Mycnf {
	return mysqld.config
}

// TabletDir returns the main tablet directory.
// It's a method so it can be accessed through the MysqlDaemon interface.
func (mysqld *Mysqld) TabletDir() string {
	return mysqld.tabletDir
}

// RunMysqlUpgrade will run the mysql_upgrade program on the current install.
// Will not be called when mysqld is running.
func (mysqld *Mysqld) RunMysqlUpgrade() error {
	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.RunMysqlUpgrade() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.RunMysqlUpgrade(context.TODO())
	}

	// find mysql_upgrade. If not there, we do nothing.
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		log.Warningf("VT_MYSQL_ROOT not set, skipping mysql_upgrade step: %v", err)
		return nil
	}
	name := path.Join(dir, "bin/mysql_upgrade")
	if _, err := os.Stat(name); err != nil {
		log.Warningf("mysql_upgrade binary not present, skipping it: %v", err)
		return nil
	}

	// run the program, if it fails, we fail
	args := []string{
		// --defaults-file=* must be the first arg.
		"--defaults-file=" + mysqld.config.path,
		"--socket", mysqld.config.SocketFile,
		"--user", mysqld.dba.Uname,
		"--force", // Don't complain if it's already been upgraded.
	}
	if mysqld.dba.Pass != "" {
		// --password must be omitted entirely if empty, or else it will prompt.
		args = append(args, "--password", mysqld.dba.Pass)
	}
	cmd := exec.Command(name, args...)
	cmd.Env = []string{os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql")}
	out, err := cmd.CombinedOutput()
	log.Infof("mysql_upgrade output: %s", out)
	return err
}

// Start will start the mysql daemon, either by running the 'mysqld_start'
// hook, or by running mysqld_safe in the background.
// If a mysqlctld address is provided in a flag, Start will run remotely.
func (mysqld *Mysqld) Start(ctx context.Context) error {
	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.Start() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.Start(ctx)
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
		log.Infof("%v %#v", ts, cmd)
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

	return mysqld.Wait(ctx)
}

// Wait returns nil when mysqld is up and accepting connections.
func (mysqld *Mysqld) Wait(ctx context.Context) error {
	log.Infof("Waiting for mysqld socket file (%v) to be ready...", mysqld.config.SocketFile)
	for {
		select {
		case <-ctx.Done():
			return errors.New("deadline exceeded waiting for mysqld socket file to appear: " + mysqld.config.SocketFile)
		default:
		}

		_, statErr := os.Stat(mysqld.config.SocketFile)
		if statErr == nil {
			// Make sure the socket file isn't stale.
			// Use a user that exists even before we apply the init_db_sql_file.
			conn, connErr := mysql.Connect(sqldb.ConnParams{
				Uname:      "root",
				Charset:    "utf8",
				UnixSocket: mysqld.config.SocketFile,
			})
			if connErr == nil {
				conn.Close()
				return nil
			}
			log.Infof("mysqld socket file exists, but can't connect: %v", connErr)
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("can't stat mysqld socket file: %v", statErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Shutdown will stop the mysqld daemon that is running in the background.
//
// waitForMysqld: should the function block until mysqld has stopped?
// This can actually take a *long* time if the buffer cache needs to be fully
// flushed - on the order of 20-30 minutes.
//
// If a mysqlctld address is provided in a flag, Shutdown will run remotely.
func (mysqld *Mysqld) Shutdown(ctx context.Context, waitForMysqld bool) error {
	log.Infof("Mysqld.Shutdown")

	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.Shutdown() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.Shutdown(ctx, waitForMysqld)
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
	if os.IsNotExist(socketPathErr) && os.IsNotExist(pidPathErr) {
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
			"-u", mysqld.dba.Uname, "-S", mysqld.config.SocketFile,
			"shutdown"}
		env := []string{
			os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql"),
		}
		_, _, err = execCmd(name, arg, env, dir, nil)
		if err != nil {
			return err
		}
	default:
		// hook failed, we report error
		return fmt.Errorf("mysqld_shutdown hook failed: %v", hr.String())
	}

	// Wait for mysqld to really stop. Use the socket and pid files as a
	// proxy for that since we can't call wait() in a process we
	// didn't start.
	if waitForMysqld {
		log.Infof("Mysqld.Shutdown: waiting for socket file (%v) and pid file (%v) to disappear",
			mysqld.config.SocketFile, mysqld.config.PidFile)

		for {
			select {
			case <-ctx.Done():
				return errors.New("gave up waiting for mysqld to stop")
			default:
			}

			_, socketPathErr = os.Stat(mysqld.config.SocketFile)
			_, pidPathErr = os.Stat(mysqld.config.PidFile)
			if os.IsNotExist(socketPathErr) && os.IsNotExist(pidPathErr) {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

// execCmd searches the PATH for a command and runs it, logging the output.
// If input is not nil, pipe it to the command's stdin.
func execCmd(name string, args, env []string, dir string, input io.Reader) (cmd *exec.Cmd, output string, err error) {
	cmdPath, _ := exec.LookPath(name)
	log.Infof("execCmd: %v %v %v", name, cmdPath, args)

	cmd = exec.Command(cmdPath, args...)
	cmd.Env = env
	cmd.Dir = dir
	if input != nil {
		cmd.Stdin = input
	}
	out, err := cmd.CombinedOutput()
	output = string(out)
	if err != nil {
		err = errors.New(name + ": " + output)
	}
	log.Infof("execCmd: command returned: %v", output)
	return cmd, output, err
}

// Init will create the default directory structure for the mysqld process,
// generate / configure a my.cnf file, install a skeleton database,
// and apply the provided initial SQL file.
func (mysqld *Mysqld) Init(ctx context.Context, initDBSQLFile string) error {
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

	// Install data dir.
	if err = mysqld.installDataDir(); err != nil {
		return err
	}

	// Start mysqld.
	if err = mysqld.Start(ctx); err != nil {
		log.Errorf("failed starting mysqld (check %v for more info): %v", mysqld.config.ErrorLogPath, err)
		return err
	}

	// Run initial SQL file.
	sqlFile, err := os.Open(initDBSQLFile)
	if err != nil {
		return fmt.Errorf("can't open init_db_sql_file (%v): %v", initDBSQLFile, err)
	}
	defer sqlFile.Close()
	if err := mysqld.executeMysqlScript("root", sqlFile); err != nil {
		return fmt.Errorf("can't run init_db_sql_file (%v): %v", initDBSQLFile, err)
	}

	return nil
}

func (mysqld *Mysqld) installDataDir() error {
	mysqlRoot, err := vtenv.VtMysqlRoot()
	if err != nil {
		log.Errorf("%v", err)
		return err
	}

	// Check mysqld version.
	_, version, err := execCmd(path.Join(mysqlRoot, "sbin/mysqld"),
		[]string{"--version"}, nil, mysqlRoot, nil)
	if err != nil {
		return err
	}

	if strings.Contains(version, "Ver 5.7.") {
		// MySQL 5.7 GA and up have deprecated mysql_install_db.
		// Instead, initialization is built into mysqld.
		log.Infof("Installing data dir with mysqld --initialize-insecure")

		args := []string{
			"--defaults-file=" + mysqld.config.path,
			"--basedir=" + mysqlRoot,
			"--initialize-insecure", // Use empty 'root'@'localhost' password.
		}
		if _, _, err = execCmd(path.Join(mysqlRoot, "sbin/mysqld"), args, nil, mysqlRoot, nil); err != nil {
			log.Errorf("mysqld --initialize-insecure failed: %v", err)
			return err
		}
		return nil
	}

	log.Infof("Installing data dir with mysql_install_db")
	args := []string{
		"--defaults-file=" + mysqld.config.path,
		"--basedir=" + mysqlRoot,
	}
	if _, _, err = execCmd(path.Join(mysqlRoot, "bin/mysql_install_db"), args, nil, mysqlRoot, nil); err != nil {
		log.Errorf("mysql_install_db failed: %v", err)
		return err
	}
	return nil
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

// ReinitConfig updates the config file as if Mysqld is initializing. At the
// moment it only randomizes ServerID because it's not safe to restore a replica
// from a backup and then give it the same ServerID as before, MySQL can then
// skip transactions in the replication stream with the same server_id.
func (mysqld *Mysqld) ReinitConfig(ctx context.Context) error {
	log.Infof("Mysqld.ReinitConfig")

	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.ReinitConfig() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.ReinitConfig(ctx)
	}

	if err := mysqld.config.RandomizeMysqlServerID(); err != nil {
		return err
	}
	root, err := vtenv.VtRoot()
	if err != nil {
		return err
	}
	return mysqld.initConfig(root)
}

func (mysqld *Mysqld) createDirs() error {
	log.Infof("creating directory %s", mysqld.tabletDir)
	if err := os.MkdirAll(mysqld.tabletDir, os.ModePerm); err != nil {
		return err
	}
	for _, dir := range TopLevelDirs() {
		if err := mysqld.createTopDir(dir); err != nil {
			return err
		}
	}
	for _, dir := range mysqld.config.directoryList() {
		log.Infof("creating directory %s", dir)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
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
	vtname := path.Base(mysqld.tabletDir)
	target := path.Join(vtenv.VtDataRoot(), dir)
	_, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			topdir := path.Join(mysqld.tabletDir, dir)
			log.Infof("creating directory %s", topdir)
			return os.MkdirAll(topdir, os.ModePerm)
		}
		return err
	}
	linkto := path.Join(target, vtname)
	source := path.Join(mysqld.tabletDir, dir)
	log.Infof("creating directory %s", linkto)
	err = os.MkdirAll(linkto, os.ModePerm)
	if err != nil {
		return err
	}
	log.Infof("creating symlink %s -> %s", source, linkto)
	return os.Symlink(linkto, source)
}

// Teardown will shutdown the running daemon, and delete the root directory.
func (mysqld *Mysqld) Teardown(ctx context.Context, force bool) error {
	log.Infof("mysqlctl.Teardown")
	if err := mysqld.Shutdown(ctx, true); err != nil {
		log.Warningf("failed mysqld shutdown: %v", err.Error())
		if !force {
			return err
		}
	}
	var removalErr error
	for _, dir := range TopLevelDirs() {
		qdir := path.Join(mysqld.tabletDir, dir)
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

// executeMysqlCommands executes some SQL commands,
// using the mysql command line tool.
func (mysqld *Mysqld) executeMysqlCommands(user, sql string) error {
	return mysqld.executeMysqlScript(user, strings.NewReader(sql))
}

// executeMysqlScript executes a .sql script file with the mysql command line tool.
func (mysqld *Mysqld) executeMysqlScript(user string, sql io.Reader) error {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return err
	}
	name := path.Join(dir, "bin/mysql")
	arg := []string{"--batch", "-u", user, "-S", mysqld.config.SocketFile}
	env := []string{
		"LD_LIBRARY_PATH=" + path.Join(dir, "lib/mysql"),
	}
	_, _, err = execCmd(name, arg, env, dir, sql)
	if err != nil {
		return err
	}
	return nil
}

// GetAppConnection returns a connection from the app pool.
// Recycle needs to be called on the result.
func (mysqld *Mysqld) GetAppConnection(ctx context.Context) (dbconnpool.PoolConnection, error) {
	return mysqld.appPool.Get(ctx)
}

// GetDbaConnection creates a new DBConnection.
func (mysqld *Mysqld) GetDbaConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(mysqld.dba, mysqld.dbaMysqlStats)
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
