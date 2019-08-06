/*
Copyright 2017 Google Inc.

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

/*
Commands for controlling an external mysql process.

Some commands are issued as exec'd tools, some are handled by connecting via
the mysql protocol.
*/

package mysqlctl

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	vtenv "vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/mysqlctlclient"
)

var (
	// DisableActiveReparents is a flag to disable active
	// reparents for safety reasons. It is used in three places:
	// 1. in this file to skip registering the commands.
	// 2. in vtctld so it can be exported to the UI (different
	// package, that's why it's exported). That way we can disable
	// menu items there, using features.
	// 3. prevents the vtworker from updating replication topology
	// after restarting replication after a split clone/diff.
	DisableActiveReparents = flag.Bool("disable_active_reparents", false, "if set, do not allow active reparents. Use this to protect a cluster using external reparents.")

	dbaPoolSize    = flag.Int("dba_pool_size", 20, "Size of the connection pool for dba connections")
	dbaIdleTimeout = flag.Duration("dba_idle_timeout", time.Minute, "Idle timeout for dba connections")
	appPoolSize    = flag.Int("app_pool_size", 40, "Size of the connection pool for app connections")
	appIdleTimeout = flag.Duration("app_idle_timeout", time.Minute, "Idle timeout for app connections")

	poolDynamicHostnameResolution = flag.Duration("pool_hostname_resolve_interval", 0, "if set force an update to all hostnames and reconnect if changed, defaults to 0 (disabled)")

	socketFile        = flag.String("mysqlctl_socket", "", "socket file to use for remote mysqlctl actions (empty for local actions)")
	mycnfTemplateFile = flag.String("mysqlctl_mycnf_template", "", "template file to use for generating the my.cnf file during server init")

	// masterConnectRetry is used in 'SET MASTER' commands
	masterConnectRetry = flag.Duration("master_connect_retry", 10*time.Second, "how long to wait in between slave -> connection attempts. Only precise to the second.")

	dbaMysqlStats      = stats.NewTimings("MysqlDba", "MySQL DBA stats", "operation")
	allprivsMysqlStats = stats.NewTimings("MysqlAllPrivs", "MySQl Stats for all privs", "operation")
	appMysqlStats      = stats.NewTimings("MysqlApp", "MySQL app stats", "operation")

	versionRegex = regexp.MustCompile(`Ver ([0-9]+)\.([0-9]+)\.([0-9]+)`)
)

// Mysqld is the object that represents a mysqld daemon running on this server.
type Mysqld struct {
	dbcfgs  *dbconfigs.DBConfigs
	dbaPool *dbconnpool.ConnectionPool
	appPool *dbconnpool.ConnectionPool

	capabilities CapabilitySet

	// mutex protects the fields below.
	mutex         sync.Mutex
	onTermFuncs   []func()
	cancelWaitCmd chan struct{}
}

// NewMysqld creates a Mysqld object based on the provided configuration
// and connection parameters.
func NewMysqld(dbcfgs *dbconfigs.DBConfigs) *Mysqld {
	result := &Mysqld{
		dbcfgs: dbcfgs,
	}

	// Create and open the connection pool for dba access.
	result.dbaPool = dbconnpool.NewConnectionPool("DbaConnPool", *dbaPoolSize, *dbaIdleTimeout, *poolDynamicHostnameResolution)
	result.dbaPool.Open(dbcfgs.Dba(), dbaMysqlStats)

	// Create and open the connection pool for app access.
	result.appPool = dbconnpool.NewConnectionPool("AppConnPool", *appPoolSize, *appIdleTimeout, *poolDynamicHostnameResolution)
	result.appPool.Open(dbcfgs.AppWithDB(), appMysqlStats)

	version, _ := getVersionString()
	f, v, err := parseVersionString(version)

	// Fallback if required
	if err != nil {
		f, v, err = getVersionFromEnv()
		if err != nil {
			panic("Could not detect version from mysqld --version or MYSQL_FLAVOR")
		}

	}

	log.Infof("Using flavor: %v, version: %v", f, v)
	result.capabilities = NewCapabilitySet(f, v)
	return result
}

func getVersionFromEnv() (flavor mysqlFlavor, ver serverVersion, err error) {

	env := os.Getenv("MYSQL_FLAVOR")

	if env == "MariaDB" {
		return flavorMariaDB, serverVersion{10, 0, 10}, nil
	}
	if env == "MariaDB103" {
		return flavorMariaDB, serverVersion{10, 3, 7}, nil
	}
	if env == "MySQL80" {
		return flavorMySQL, serverVersion{8, 0, 11}, nil
	}
	if env == "MySQL56" {
		return flavorMySQL, serverVersion{5, 7, 10}, nil
	}

	return flavor, ver, fmt.Errorf("Could not determine version from MYSQL_FLAVOR: %s", env)

}

func getVersionString() (string, error) {
	mysqlRoot, err := vtenv.VtMysqlRoot()
	if err != nil {
		return "", err
	}
	mysqldPath, err := binaryPath(mysqlRoot, "mysqld")
	if err != nil {
		return "", err
	}
	_, version, err := execCmd(mysqldPath, []string{"--version"}, nil, mysqlRoot, nil)
	if err != nil {
		return "", err
	}
	return version, nil
}

// parse the output of mysqld --version into a flavor and version
func parseVersionString(version string) (flavor mysqlFlavor, ver serverVersion, err error) {

	if strings.Contains(version, "Percona") {
		flavor = flavorPercona
	} else if strings.Contains(version, "MariaDB") {
		flavor = flavorMariaDB
	} else {
		// OS distributed MySQL releases have a version string like:
		// mysqld  Ver 5.7.27-0ubuntu0.19.04.1 for Linux on x86_64 ((Ubuntu))
		flavor = flavorMySQL
	}
	v := versionRegex.FindStringSubmatch(version)
	if len(v) != 4 {
		return flavor, ver, fmt.Errorf("Could not parse server version from: %s", version)
	}
	ver.Major, err = strconv.Atoi(string(v[1]))
	if err != nil {
		return flavor, ver, fmt.Errorf("Could not parse server version from: %s", version)
	}
	ver.Minor, err = strconv.Atoi(string(v[2]))
	if err != nil {
		return flavor, ver, fmt.Errorf("Could not parse server version from: %s", version)
	}
	ver.Patch, err = strconv.Atoi(string(v[3]))
	if err != nil {
		return flavor, ver, fmt.Errorf("Could not parse server version from: %s", version)
	}

	return
}

// RunMysqlUpgrade will run the mysql_upgrade program on the current
// install.  Will be called only when mysqld is running with no
// network and no grant tables.
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

	if mysqld.capabilities.HasMySQLUpgradeInServer() {
		log.Warningf("MySQL version has built-in upgrade, skipping RunMySQLUpgrade")
		return nil
	}

	// Find mysql_upgrade. If not there, we do nothing.
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		log.Warningf("VT_MYSQL_ROOT not set, skipping mysql_upgrade step: %v", err)
		return nil
	}
	name, err := binaryPath(dir, "mysql_upgrade")
	if err != nil {
		log.Warningf("mysql_upgrade binary not present, skipping it: %v", err)
		return nil
	}

	// Since we started mysql with --skip-grant-tables, we should
	// be able to run mysql_upgrade without any valid user or
	// password. However, mysql_upgrade executes a 'flush
	// privileges' right in the middle, and then subsequent
	// commands fail if we don't use valid credentials. So let's
	// use dba credentials.
	params, err := dbconfigs.WithCredentials(mysqld.dbcfgs.Dba())
	if err != nil {
		return err
	}
	defaultsFile, err := mysqld.defaultsExtraFile(params)
	if err != nil {
		return err
	}
	defer os.Remove(defaultsFile)

	// Run the program, if it fails, we fail.  Note in this
	// moment, mysqld is running with no grant tables on the local
	// socket only, so this doesn't need any user or password.
	args := []string{
		// --defaults-file=* must be the first arg.
		"--defaults-file=" + defaultsFile,
		"--force", // Don't complain if it's already been upgraded.
	}
	cmd := exec.Command(name, args...)
	cmd.Env = []string{os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql")}
	out, err := cmd.CombinedOutput()
	log.Infof("mysql_upgrade output: %s", out)
	return err
}

// Start will start the mysql daemon, either by running the
// 'mysqld_start' hook, or by running mysqld_safe in the background.
// If a mysqlctld address is provided in a flag, Start will run
// remotely.  When waiting for mysqld to start, we will use
// the dba user.
func (mysqld *Mysqld) Start(ctx context.Context, cnf *Mycnf, mysqldArgs ...string) error {
	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.Start() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.Start(ctx, mysqldArgs...)
	}

	if err := mysqld.startNoWait(ctx, cnf, mysqldArgs...); err != nil {
		return err
	}

	return mysqld.Wait(ctx, cnf)
}

// startNoWait is the internal version of Start, and it doesn't wait.
func (mysqld *Mysqld) startNoWait(ctx context.Context, cnf *Mycnf, mysqldArgs ...string) error {
	var name string
	ts := fmt.Sprintf("Mysqld.Start(%v)", time.Now().Unix())

	// try the mysqld start hook, if any
	switch hr := hook.NewHook("mysqld_start", mysqldArgs).Execute(); hr.ExitStatus {
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
		name, err = binaryPath(dir, "mysqld_safe")
		if err != nil {
			// The movement to use systemd means that mysqld_safe is not always provided.
			// This should not be considered an issue do not generate a warning.
			log.Infof("%v: trying to launch mysqld instead", err)
			name, err = binaryPath(dir, "mysqld")
			// If this also fails, return an error.
			if err != nil {
				return err
			}
		}
		mysqlBaseDir, err := vtenv.VtMysqlBaseDir()
		if err != nil {
			return err
		}
		arg := []string{
			"--defaults-file=" + cnf.path,
			"--basedir" + mysqlBaseDir,
		}
		arg = append(arg, mysqldArgs...)
		env := []string{os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql")}

		cmd := exec.Command(name, arg...)
		cmd.Dir = dir
		cmd.Env = env
		log.Infof("%v %#v", ts, cmd)
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
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
			return err
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

	return nil
}

// Wait returns nil when mysqld is up and accepting connections. It
// will use the dba credentials to try to connect. Use wait() with
// different credentials if needed.
func (mysqld *Mysqld) Wait(ctx context.Context, cnf *Mycnf) error {
	params, err := dbconfigs.WithCredentials(mysqld.dbcfgs.Dba())
	if err != nil {
		return err
	}

	return mysqld.wait(ctx, cnf, params)
}

// wait is the internal version of Wait, that takes credentials.
func (mysqld *Mysqld) wait(ctx context.Context, cnf *Mycnf, params *mysql.ConnParams) error {
	log.Infof("Waiting for mysqld socket file (%v) to be ready...", cnf.SocketFile)

	for {
		select {
		case <-ctx.Done():
			return errors.New("deadline exceeded waiting for mysqld socket file to appear: " + cnf.SocketFile)
		default:
		}

		_, statErr := os.Stat(cnf.SocketFile)
		if statErr == nil {
			// Make sure the socket file isn't stale.
			conn, connErr := mysql.Connect(ctx, params)
			if connErr == nil {
				conn.Close()
				return nil
			}
			log.Infof("mysqld socket file exists, but can't connect: %v", connErr)
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("can't stat mysqld socket file: %v", statErr)
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

// Shutdown will stop the mysqld daemon that is running in the background.
//
// waitForMysqld: should the function block until mysqld has stopped?
// This can actually take a *long* time if the buffer cache needs to be fully
// flushed - on the order of 20-30 minutes.
//
// If a mysqlctld address is provided in a flag, Shutdown will run remotely.
func (mysqld *Mysqld) Shutdown(ctx context.Context, cnf *Mycnf, waitForMysqld bool) error {
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
	_, socketPathErr := os.Stat(cnf.SocketFile)
	_, pidPathErr := os.Stat(cnf.PidFile)
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
		name, err := binaryPath(dir, "mysqladmin")
		if err != nil {
			return err
		}
		params, err := dbconfigs.WithCredentials(mysqld.dbcfgs.Dba())
		if err != nil {
			return err
		}
		cnf, err := mysqld.defaultsExtraFile(params)
		if err != nil {
			return err
		}
		defer os.Remove(cnf)
		args := []string{
			"--defaults-extra-file=" + cnf,
			"--shutdown-timeout=300",
			"--connect-timeout=30",
			"--wait=10",
			"shutdown",
		}
		env := []string{
			os.ExpandEnv("LD_LIBRARY_PATH=$VT_MYSQL_ROOT/lib/mysql"),
		}
		_, _, err = execCmd(name, args, env, dir, nil)
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
			cnf.SocketFile, cnf.PidFile)

		for {
			select {
			case <-ctx.Done():
				return errors.New("gave up waiting for mysqld to stop")
			default:
			}

			_, socketPathErr = os.Stat(cnf.SocketFile)
			_, pidPathErr = os.Stat(cnf.PidFile)
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
		log.Infof("execCmd: %v failed: %v", name, err)
		err = fmt.Errorf("%v: %v, output: %v", name, err, output)
	}
	log.Infof("execCmd: %v output: %v", name, output)
	return cmd, output, err
}

// binaryPath does a limited path lookup for a command,
// searching only within sbin and bin in the given root.
func binaryPath(root, binary string) (string, error) {
	subdirs := []string{"sbin", "bin", "libexec", "scripts"}
	for _, subdir := range subdirs {
		binPath := path.Join(root, subdir, binary)
		if _, err := os.Stat(binPath); err == nil {
			return binPath, nil
		}
	}
	return "", fmt.Errorf("%s not found in any of %s/{%s}",
		binary, root, strings.Join(subdirs, ","))
}

// InitConfig will create the default directory structure for the mysqld process,
// generate / configure a my.cnf file.
func (mysqld *Mysqld) InitConfig(cnf *Mycnf) error {
	log.Infof("mysqlctl.InitConfig")
	err := mysqld.createDirs(cnf)
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
	if err = mysqld.initConfig(root, cnf, cnf.path); err != nil {
		log.Errorf("failed creating %v: %v", cnf.path, err)
		return err
	}
	return nil
}

// Init will create the default directory structure for the mysqld process,
// generate / configure a my.cnf file install a skeleton database,
// and apply the provided initial SQL file.
func (mysqld *Mysqld) Init(ctx context.Context, cnf *Mycnf, initDBSQLFile string) error {
	log.Infof("mysqlctl.Init")
	err := mysqld.InitConfig(cnf)
	if err != nil {
		log.Errorf("%s", err.Error())
		return err
	}
	// Install data dir.
	if err = mysqld.installDataDir(cnf); err != nil {
		return err
	}

	// Start mysqld. We do not use Start, as we have to wait using
	// the root user.
	if err = mysqld.startNoWait(ctx, cnf); err != nil {
		log.Errorf("failed starting mysqld (check mysql error log %v for more info): %v", cnf.ErrorLogPath, err)
		return err
	}

	// Wait for mysqld to be ready, using root credentials, as no
	// user is created yet.
	params := &mysql.ConnParams{
		Uname:      "root",
		Charset:    "utf8",
		UnixSocket: cnf.SocketFile,
	}
	if err = mysqld.wait(ctx, cnf, params); err != nil {
		log.Errorf("failed starting mysqld in time (check mysyql error log %v for more info): %v", cnf.ErrorLogPath, err)
		return err
	}

	// Run initial SQL file.
	sqlFile, err := os.Open(initDBSQLFile)
	if err != nil {
		return fmt.Errorf("can't open init_db_sql_file (%v): %v", initDBSQLFile, err)
	}
	defer sqlFile.Close()
	if err := mysqld.executeMysqlScript(params, sqlFile); err != nil {
		return fmt.Errorf("can't run init_db_sql_file (%v): %v", initDBSQLFile, err)
	}

	return nil
}

func (mysqld *Mysqld) installDataDir(cnf *Mycnf) error {
	mysqlRoot, err := vtenv.VtMysqlRoot()
	if err != nil {
		return err
	}
	mysqldPath, err := binaryPath(mysqlRoot, "mysqld")
	if err != nil {
		return err
	}

	mysqlBaseDir, err := vtenv.VtMysqlBaseDir()
	if err != nil {
		return err
	}
	if mysqld.capabilities.HasInitializeInServer() {
		log.Infof("Installing data dir with mysqld --initialize-insecure")
		args := []string{
			"--defaults-file=" + cnf.path,
			"--basedir=" + mysqlBaseDir,
			"--initialize-insecure", // Use empty 'root'@'localhost' password.
		}
		if _, _, err = execCmd(mysqldPath, args, nil, mysqlRoot, nil); err != nil {
			log.Errorf("mysqld --initialize-insecure failed: %v", err)
			return err
		}
		return nil
	}

	log.Infof("Installing data dir with mysql_install_db")
	args := []string{
		"--defaults-file=" + cnf.path,
		"--basedir=" + mysqlBaseDir,
	}
	cmdPath, err := binaryPath(mysqlRoot, "mysql_install_db")
	if err != nil {
		return err
	}
	if _, _, err = execCmd(cmdPath, args, nil, mysqlRoot, nil); err != nil {
		log.Errorf("mysql_install_db failed: %v", err)
		return err
	}
	return nil
}

func (mysqld *Mysqld) initConfig(root string, cnf *Mycnf, outFile string) error {
	var err error
	var configData string

	env := make(map[string]string)
	envVars := []string{"KEYSPACE", "SHARD", "TABLET_TYPE", "TABLET_ID", "TABLET_DIR", "MYSQL_PORT"}
	for _, v := range envVars {
		env[v] = os.Getenv(v)
	}

	switch hr := hook.NewHookWithEnv("make_mycnf", nil, env).Execute(); hr.ExitStatus {
	case hook.HOOK_DOES_NOT_EXIST:
		log.Infof("make_mycnf hook doesn't exist, reading template files")
		configData, err = cnf.makeMycnf(mysqld.getMycnfTemplates(root))
	case hook.HOOK_SUCCESS:
		configData, err = cnf.fillMycnfTemplate(hr.Stdout)
	default:
		return fmt.Errorf("make_mycnf hook failed(%v): %v", hr.ExitStatus, hr.Stderr)
	}
	if err != nil {
		return err
	}

	return ioutil.WriteFile(outFile, []byte(configData), 0664)
}

func contains(haystack []string, needle string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

func (mysqld *Mysqld) getMycnfTemplates(root string) []string {
	if *mycnfTemplateFile != "" {
		return []string{*mycnfTemplateFile}
	}

	cnfTemplatePaths := []string{
		path.Join(root, "config/mycnf/default.cnf"),
		path.Join(root, "config/mycnf/master.cnf"),
		path.Join(root, "config/mycnf/replica.cnf"),
	}

	if extraCnf := os.Getenv("EXTRA_MY_CNF"); extraCnf != "" {
		parts := strings.Split(extraCnf, ":")
		cnfTemplatePaths = append(cnfTemplatePaths, parts...)
	}

	// Only include these files if they exist.
	// master_{flavor}.cnf
	// Percona Server == MySQL in this context

	f := flavorMariaDB
	if mysqld.capabilities.IsMySQLLike() {
		f = flavorMySQL
	}

	p := path.Join(root, fmt.Sprintf("config/mycnf/master_%s.cnf", f))
	_, err := os.Stat(p)
	if err == nil && !contains(cnfTemplatePaths, p) {
		cnfTemplatePaths = append(cnfTemplatePaths, p)
	}

	// master_{flavor}{major}{minor}.cnf
	p = path.Join(root, fmt.Sprintf("config/mycnf/master_%s%d%d.cnf", f, mysqld.capabilities.version.Major, mysqld.capabilities.version.Minor))
	_, err = os.Stat(p)
	if err == nil && !contains(cnfTemplatePaths, p) {
		cnfTemplatePaths = append(cnfTemplatePaths, p)
	}

	return cnfTemplatePaths
}

// RefreshConfig attempts to recreate the my.cnf from templates, and log and
// swap in to place if it's updated. It keeps a copy of the last version in case fallback is required.
// Should be called from a stable replica, server_id is not regenerated.
func (mysqld *Mysqld) RefreshConfig(ctx context.Context, cnf *Mycnf) error {
	// Execute as remote action on mysqlctld if requested.
	if *socketFile != "" {
		log.Infof("executing Mysqld.RefreshConfig() remotely via mysqlctld server: %v", *socketFile)
		client, err := mysqlctlclient.New("unix", *socketFile)
		if err != nil {
			return fmt.Errorf("can't dial mysqlctld: %v", err)
		}
		defer client.Close()
		return client.RefreshConfig(ctx)
	}

	log.Info("Checking for updates to my.cnf")
	root, err := vtenv.VtRoot()
	if err != nil {
		return err
	}
	f, err := ioutil.TempFile(path.Dir(cnf.path), "my.cnf")
	if err != nil {
		return fmt.Errorf("could not create temp file: %v", err)
	}

	defer os.Remove(f.Name())
	err = mysqld.initConfig(root, cnf, f.Name())
	if err != nil {
		return fmt.Errorf("could not initConfig in %v: %v", f.Name(), err)
	}

	existing, err := ioutil.ReadFile(cnf.path)
	if err != nil {
		return fmt.Errorf("could not read existing file %v: %v", cnf.path, err)
	}
	updated, err := ioutil.ReadFile(f.Name())
	if err != nil {
		return fmt.Errorf("could not read updated file %v: %v", f.Name(), err)
	}

	if bytes.Equal(existing, updated) {
		log.Infof("No changes to my.cnf. Continuing.")
		return nil
	}

	backupPath := cnf.path + ".previous"
	err = os.Rename(cnf.path, backupPath)
	if err != nil {
		return fmt.Errorf("could not back up existing %v: %v", cnf.path, err)
	}
	err = os.Rename(f.Name(), cnf.path)
	if err != nil {
		return fmt.Errorf("could not move %v to %v: %v", f.Name(), cnf.path, err)
	}
	log.Infof("Updated my.cnf. Backup of previous version available in %v", backupPath)

	return nil
}

// ReinitConfig updates the config file as if Mysqld is initializing. At the
// moment it only randomizes ServerID because it's not safe to restore a replica
// from a backup and then give it the same ServerID as before, MySQL can then
// skip transactions in the replication stream with the same server_id.
func (mysqld *Mysqld) ReinitConfig(ctx context.Context, cnf *Mycnf) error {
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

	if err := cnf.RandomizeMysqlServerID(); err != nil {
		return err
	}
	root, err := vtenv.VtRoot()
	if err != nil {
		return err
	}
	return mysqld.initConfig(root, cnf, cnf.path)
}

func (mysqld *Mysqld) createDirs(cnf *Mycnf) error {
	tabletDir := cnf.TabletDir()
	log.Infof("creating directory %s", tabletDir)
	if err := os.MkdirAll(tabletDir, os.ModePerm); err != nil {
		return err
	}
	for _, dir := range TopLevelDirs() {
		if err := mysqld.createTopDir(cnf, dir); err != nil {
			return err
		}
	}
	for _, dir := range cnf.directoryList() {
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
func (mysqld *Mysqld) createTopDir(cnf *Mycnf, dir string) error {
	tabletDir := cnf.TabletDir()
	vtname := path.Base(tabletDir)
	target := path.Join(vtenv.VtDataRoot(), dir)
	_, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			topdir := path.Join(tabletDir, dir)
			log.Infof("creating directory %s", topdir)
			return os.MkdirAll(topdir, os.ModePerm)
		}
		return err
	}
	linkto := path.Join(target, vtname)
	source := path.Join(tabletDir, dir)
	log.Infof("creating directory %s", linkto)
	err = os.MkdirAll(linkto, os.ModePerm)
	if err != nil {
		return err
	}
	log.Infof("creating symlink %s -> %s", source, linkto)
	return os.Symlink(linkto, source)
}

// Teardown will shutdown the running daemon, and delete the root directory.
func (mysqld *Mysqld) Teardown(ctx context.Context, cnf *Mycnf, force bool) error {
	log.Infof("mysqlctl.Teardown")
	if err := mysqld.Shutdown(ctx, cnf, true); err != nil {
		log.Warningf("failed mysqld shutdown: %v", err.Error())
		if !force {
			return err
		}
	}
	var removalErr error
	for _, dir := range TopLevelDirs() {
		qdir := path.Join(cnf.TabletDir(), dir)
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

// executeMysqlScript executes a .sql script from an io.Reader with the mysql
// command line tool. It uses the connParams as is, not adding credentials.
func (mysqld *Mysqld) executeMysqlScript(connParams *mysql.ConnParams, sql io.Reader) error {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return err
	}
	name, err := binaryPath(dir, "mysql")
	if err != nil {
		return err
	}
	cnf, err := mysqld.defaultsExtraFile(connParams)
	if err != nil {
		return err
	}
	defer os.Remove(cnf)
	args := []string{
		"--defaults-extra-file=" + cnf,
		"--batch",
	}
	env := []string{
		"LD_LIBRARY_PATH=" + path.Join(dir, "lib/mysql"),
	}
	_, _, err = execCmd(name, args, env, dir, sql)
	if err != nil {
		return err
	}
	return nil
}

// defaultsExtraFile returns the filename for a temporary config file
// that contains the user, password and socket file to connect to
// mysqld.  We write a temporary config file so the password is never
// passed as a command line parameter.  Note ioutil.TempFile uses 0600
// as permissions, so only the local user can read the file.  The
// returned temporary file should be removed after use, typically in a
// 'defer os.Remove()' statement.
func (mysqld *Mysqld) defaultsExtraFile(connParams *mysql.ConnParams) (string, error) {
	var contents string
	if connParams.UnixSocket == "" {
		contents = fmt.Sprintf(`
[client]
user=%v
password=%v
host=%v
port=%v
`, connParams.Uname, connParams.Pass, connParams.Host, connParams.Port)
	} else {
		contents = fmt.Sprintf(`
[client]
user=%v
password=%v
socket=%v
`, connParams.Uname, connParams.Pass, connParams.UnixSocket)
	}

	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		return "", err
	}
	name := tmpfile.Name()
	if _, err := tmpfile.Write([]byte(contents)); err != nil {
		tmpfile.Close()
		os.Remove(name)
		return "", err
	}
	if err := tmpfile.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

// GetAppConnection returns a connection from the app pool.
// Recycle needs to be called on the result.
func (mysqld *Mysqld) GetAppConnection(ctx context.Context) (*dbconnpool.PooledDBConnection, error) {
	return mysqld.appPool.Get(ctx)
}

// GetDbaConnection creates a new DBConnection.
func (mysqld *Mysqld) GetDbaConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(mysqld.dbcfgs.Dba(), dbaMysqlStats)
}

// GetAllPrivsConnection creates a new DBConnection.
func (mysqld *Mysqld) GetAllPrivsConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(mysqld.dbcfgs.AllPrivsWithDB(), allprivsMysqlStats)
}

// Close will close this instance of Mysqld. It will wait for all dba
// queries to be finished.
func (mysqld *Mysqld) Close() {
	if mysqld.dbaPool != nil {
		mysqld.dbaPool.Close()
	}
	if mysqld.appPool != nil {
		mysqld.appPool.Close()
	}
}

// OnTerm registers a function to be called if mysqld terminates for any
// reason other than a call to Mysqld.Shutdown(). This only works if mysqld
// was actually started by calling Start() on this Mysqld instance.
func (mysqld *Mysqld) OnTerm(f func()) {
	mysqld.mutex.Lock()
	defer mysqld.mutex.Unlock()
	mysqld.onTermFuncs = append(mysqld.onTermFuncs, f)
}
