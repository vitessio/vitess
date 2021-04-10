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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"

	// we use gRPC everywhere, so import the vtgate client.
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

// Environment is the interface that customizes the global settings for
// the test cluster. Usually the same environment settings are shared by
// all the LocalCluster instances in a given test suite, with each instance
// receiving a different Config for specific tests.
// For Environments that create temporary data on-disk and clean it up on
// termination, a brand new instance of Environment should be passed to
// each LocalCluster.
type Environment interface {
	// BinaryPath returns the full path to the given executable
	BinaryPath(bin string) string

	// MySQLManager is the constructor for the MySQL manager that will
	// be used by the cluster. The manager must take care of initializing
	// and destructing the MySQL instance(s) that will be used by the cluster.
	// See: vttest.MySQLManager for the interface the manager must implement
	MySQLManager(mycnf []string, snapshot string) (MySQLManager, error)

	// Directory is the path where the local cluster will store all its
	// data and metadata. For local testing, this should probably be an
	// unique temporary directory.
	Directory() string

	// LogDirectory is the directory where logs for all services in the
	// cluster will be stored.
	LogDirectory() string

	// VtcomoboArguments are the extra commandline arguments that will be
	// passed to `vtcombo`
	VtcomboArguments() []string

	// ProcessHealthCheck returns a HealthChecker for the given service.
	// The HealthChecker takes an address and attempts to check whether
	// the service is up and healthy.
	// If a given service does not require any custom health checks,
	// nil can be returned.
	ProcessHealthCheck(name string) HealthChecker

	// DefaultProtocol is the protocol used to communicate with the
	// Vitess cluster. This is usually "grpc".
	DefaultProtocol() string

	// PortForProtocol returns the listening port for a given service
	// on the given protocol. If protocol is empty, the default protocol
	// for each service is assumed.
	PortForProtocol(name, protocol string) int

	// EnvVars returns the environment variables that will be passed
	// to all Vitess processes spawned by the local cluster. These variables
	// always take precedence over the variables inherited from the current
	// process.
	EnvVars() []string

	// TearDown is called during LocalCluster.TearDown() to cleanup
	// any temporary data in the environment. Environments that can
	// last through several test runs do not need to implement it.
	TearDown() error
}

// LocalTestEnv is an Environment implementation for local testing
// See: NewLocalTestEnv()
type LocalTestEnv struct {
	BasePort     int
	TmpPath      string
	DefaultMyCnf []string
	Env          []string
}

// DefaultMySQLFlavor is the MySQL flavor used by vttest when MYSQL_FLAVOR is not
// set in the environment
const DefaultMySQLFlavor = "MySQL56"

// GetMySQLOptions returns the default option set for the given MySQL
// flavor. If flavor is not set, the value from the `MYSQL_FLAVOR` env
// variable is used, and if this is not set, DefaultMySQLFlavor will
// be used.
// Returns the name of the MySQL flavor being used, the set of MySQL CNF
// files specific to this flavor, and any errors.
func GetMySQLOptions(flavor string) (string, []string, error) {
	if flavor == "" {
		flavor = os.Getenv("MYSQL_FLAVOR")
	}

	if flavor == "" {
		flavor = DefaultMySQLFlavor
	}

	mycnf := []string{}
	mycnf = append(mycnf, "config/mycnf/default-fast.cnf")

	for i, cnf := range mycnf {
		mycnf[i] = path.Join(os.Getenv("VTROOT"), cnf)
	}

	return flavor, mycnf, nil
}

// EnvVars implements EnvVars for LocalTestEnv
func (env *LocalTestEnv) EnvVars() []string {
	return env.Env
}

// BinaryPath implements BinaryPath for LocalTestEnv
func (env *LocalTestEnv) BinaryPath(binary string) string {
	return path.Join(os.Getenv("VTROOT"), "bin", binary)
}

// MySQLManager implements MySQLManager for LocalTestEnv
func (env *LocalTestEnv) MySQLManager(mycnf []string, snapshot string) (MySQLManager, error) {
	return &Mysqlctl{
		Binary:    env.BinaryPath("mysqlctl"),
		InitFile:  path.Join(os.Getenv("VTROOT"), "config/init_db.sql"),
		Directory: env.TmpPath,
		Port:      env.PortForProtocol("mysql", ""),
		MyCnf:     append(env.DefaultMyCnf, mycnf...),
		Env:       env.EnvVars(),
		UID:       1,
	}, nil
}

// DefaultProtocol implements DefaultProtocol for LocalTestEnv.
// It is always GRPC.
func (env *LocalTestEnv) DefaultProtocol() string {
	return "grpc"
}

// PortForProtocol implements PortForProtocol for LocalTestEnv.
func (env *LocalTestEnv) PortForProtocol(name, proto string) int {
	switch name {
	case "vtcombo":
		if proto == "grpc" {
			return env.BasePort + 1
		}
		return env.BasePort

	case "mysql":
		return env.BasePort + 2

	case "vtcombo_mysql_port":
		return env.BasePort + 3

	default:
		panic("unknown service name: " + name)
	}
}

// ProcessHealthCheck implements ProcessHealthCheck for LocalTestEnv.
// By default, it performs no service-specific health checks
func (env *LocalTestEnv) ProcessHealthCheck(name string) HealthChecker {
	return nil
}

// VtcomboArguments implements VtcomboArguments for LocalTestEnv.
func (env *LocalTestEnv) VtcomboArguments() []string {
	return []string{
		"-service_map", strings.Join(
			[]string{"grpc-vtgateservice", "grpc-vtctl"}, ",",
		),
		"-enable_queries",
	}
}

// LogDirectory implements LogDirectory for LocalTestEnv.
func (env *LocalTestEnv) LogDirectory() string {
	return path.Join(env.TmpPath, "logs")
}

// Directory implements Directory for LocalTestEnv.
func (env *LocalTestEnv) Directory() string {
	return env.TmpPath
}

// TearDown implements TearDown for LocalTestEnv
func (env *LocalTestEnv) TearDown() error {
	return os.RemoveAll(env.TmpPath)
}

func tmpdir(dataroot string) (dir string, err error) {
	dir, err = ioutil.TempDir(dataroot, "vttest")
	return
}

func randomPort() int {
	v := rand.Int31n(20000)
	return int(v + 10000)
}

// NewLocalTestEnv returns an instance of the default test environment used
// for local testing Vitess. The defaults are as follows:
// - Directory() is a random temporary directory in VTDATAROOT, which is cleaned
// up when closing the Environment.
// - LogDirectory() is the `logs` subdir inside Directory()
// - The MySQL flavor is set to `flavor`. If the argument is not set, it will
// default to the value of MYSQL_FLAVOR, and if this variable is not set, to
// DefaultMySQLFlavor
// - PortForProtocol() will return ports based off the given basePort. If basePort
// is zero, a random port between 10000 and 20000 will be chosen.
// - DefaultProtocol() is always "grpc"
// - ProcessHealthCheck() performs no service-specific health checks
// - BinaryPath() will look up the default Vitess binaries in VTROOT
// - MySQLManager() will return a vttest.Mysqlctl instance, configured with the
// given MySQL flavor. This will use the `mysqlctl` command to initialize and
// teardown a single mysqld instance.
func NewLocalTestEnv(flavor string, basePort int) (*LocalTestEnv, error) {
	directory, err := tmpdir(os.Getenv("VTDATAROOT"))
	if err != nil {
		return nil, err
	}
	return NewLocalTestEnvWithDirectory(flavor, basePort, directory)
}

// NewLocalTestEnvWithDirectory returns a new instance of the default test
// environment with a directory explicitly specified.
func NewLocalTestEnvWithDirectory(flavor string, basePort int, directory string) (*LocalTestEnv, error) {
	if _, err := os.Stat(path.Join(directory, "logs")); os.IsNotExist(err) {
		err := os.Mkdir(path.Join(directory, "logs"), 0700)
		if err != nil {
			return nil, err
		}
	}

	flavor, mycnf, err := GetMySQLOptions(flavor)
	if err != nil {
		return nil, err
	}

	if basePort == 0 {
		basePort = randomPort()
	}

	return &LocalTestEnv{
		BasePort:     basePort,
		TmpPath:      directory,
		DefaultMyCnf: mycnf,
		Env: []string{
			fmt.Sprintf("VTDATAROOT=%s", directory),
			fmt.Sprintf("MYSQL_FLAVOR=%s", flavor),
		},
	}, nil
}

func defaultEnvFactory() (Environment, error) {
	return NewLocalTestEnv("", 0)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewDefaultEnv is an user-configurable callback that returns a new Environment
// instance with the default settings.
// This callback is only used in cases where the user hasn't explicitly set
// the Env variable when initializing a LocalCluster
var NewDefaultEnv = defaultEnvFactory
