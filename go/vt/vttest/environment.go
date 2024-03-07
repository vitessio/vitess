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
	"math/rand"
	"os"
	"path"
	"strings"

	"vitess.io/vitess/go/vt/proto/vttest"

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

	// TopoManager is the constructor for the Topology manager that will
	// be used by the cluster. It's only used when we run the local cluster with
	// a remote topo server instead of in-memory topo server within vtcombo process
	// See: vttest.TopoManager for the interface of topo manager
	TopoManager(topoImplementation, topoServerAddress, topoRoot string, topology *vttest.VTTestTopology) TopoManager

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
	BasePort        int
	TmpPath         string
	DefaultMyCnf    []string
	InitDBFile      string
	Env             []string
	EnableToxiproxy bool
}

// DefaultMySQLFlavor is the MySQL flavor used by vttest when no explicit
// flavor is given.
const DefaultMySQLFlavor = "MySQL56"

// GetMySQLOptions returns the set of MySQL CNF files and any errors.
func GetMySQLOptions() ([]string, error) {
	mycnf := []string{}
	mycnf = append(mycnf, "config/mycnf/test-suite.cnf")

	for i, cnf := range mycnf {
		mycnf[i] = path.Join(os.Getenv("VTROOT"), cnf)
	}

	return mycnf, nil
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
	mysqlctl := &Mysqlctl{
		Binary:    env.BinaryPath("mysqlctl"),
		InitFile:  env.InitDBFile,
		Directory: env.TmpPath,
		Port:      env.PortForProtocol("mysql", ""),
		MyCnf:     append(env.DefaultMyCnf, mycnf...),
		Env:       env.EnvVars(),
		UID:       1,
	}
	if !env.EnableToxiproxy {
		return mysqlctl, nil
	}

	return NewToxiproxyctl(
		env.BinaryPath("toxiproxy-server"),
		env.PortForProtocol("toxiproxy", ""),
		env.PortForProtocol("mysql_behind_toxiproxy", ""),
		mysqlctl,
		path.Join(env.LogDirectory(), "toxiproxy.log"),
	)
}

// TopoManager implements TopoManager for LocalTestEnv
func (env *LocalTestEnv) TopoManager(topoImplementation, topoServerAddress, topoRoot string, topology *vttest.VTTestTopology) TopoManager {
	return &Topoctl{
		TopoImplementation:      topoImplementation,
		TopoGlobalServerAddress: topoServerAddress,
		TopoGlobalRoot:          topoRoot,
		Topology:                topology,
	}
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

	case "toxiproxy":
		return env.BasePort + 4

	case "mysql_behind_toxiproxy":
		return env.BasePort + 5

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
		"--service_map", strings.Join(
			[]string{"grpc-vtgateservice", "grpc-vtctl", "grpc-vtctld"}, ",",
		),
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
	dir, err = os.MkdirTemp(dataroot, "vttest")
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
// default DefaultMySQLFlavor
// - PortForProtocol() will return ports based off the given basePort. If basePort
// is zero, a random port between 10000 and 20000 will be chosen.
// - DefaultProtocol() is always "grpc"
// - ProcessHealthCheck() performs no service-specific health checks
// - BinaryPath() will look up the default Vitess binaries in VTROOT
// - MySQLManager() will return a vttest.Mysqlctl instance, configured with the
// given MySQL flavor. This will use the `mysqlctl` command to initialize and
// teardown a single mysqld instance.
func NewLocalTestEnv(basePort int) (*LocalTestEnv, error) {
	directory, err := tmpdir(os.Getenv("VTDATAROOT"))
	if err != nil {
		return nil, err
	}
	return NewLocalTestEnvWithDirectory(basePort, directory)
}

// NewLocalTestEnvWithDirectory returns a new instance of the default test
// environment with a directory explicitly specified.
func NewLocalTestEnvWithDirectory(basePort int, directory string) (*LocalTestEnv, error) {
	if _, err := os.Stat(path.Join(directory, "logs")); os.IsNotExist(err) {
		err := os.Mkdir(path.Join(directory, "logs"), 0700)
		if err != nil {
			return nil, err
		}
	}

	mycnf, err := GetMySQLOptions()
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
		InitDBFile:   path.Join(os.Getenv("VTROOT"), "config/init_db.sql"),
		Env: []string{
			fmt.Sprintf("VTDATAROOT=%s", directory),
			"VTTEST=endtoend",
		},
	}, nil
}

func defaultEnvFactory() (Environment, error) {
	return NewLocalTestEnv(0)
}

// NewDefaultEnv is an user-configurable callback that returns a new Environment
// instance with the default settings.
// This callback is only used in cases where the user hasn't explicitly set
// the Env variable when initializing a LocalCluster
var NewDefaultEnv = defaultEnvFactory
