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

// Package vttest contains helpers to set up Vitess for testing.
package vttest

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	vtutils "vitess.io/vitess/go/vt/utils"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var versionRegex = regexp.MustCompile(`Version: ([0-9]+)\.([0-9]+)\.([0-9]+)`)

// getMajorVersion extracts the major version from a binary by running binaryName --version
func getMajorVersion(binaryName string) (int, error) {
	version, err := exec.Command(binaryName, "--version").Output()
	if err != nil {
		return 0, err
	}
	v := versionRegex.FindStringSubmatch(string(version))
	if len(v) != 4 {
		return 0, fmt.Errorf("could not parse server version from: %s", version)
	}

	return strconv.Atoi(v[1])
}

// HealthChecker is a callback that impements a service-specific health check
// It must return true if the service at the given `addr` is reachable, false
// otherwerise.
type HealthChecker func(addr string) bool

// VtProcess is a generic handle for a running Vitess process.
// It can be spawned manually or through one of the available
// helper methods.
type VtProcess struct {
	Name            string
	Directory       string
	LogDirectory    string
	Binary          string
	ExtraArgs       []string
	Env             []string
	BindAddress     string
	BindAddressGprc string
	Port            int
	PortGrpc        int
	HealthCheck     HealthChecker

	proc *exec.Cmd
	exit chan error
}

// getVars returns the JSON contents of the `/debug/vars` endpoint
// of this Vitess-based process. If an error is returned, it probably
// means that the Vitess service has not started successfully.
func getVars(addr string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/debug/vars", addr)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// defaultHealthCheck checks the health of the Vitess process using getVars.
// It is used when VtProcess.HealthCheck is nil.
func defaultHealthCheck(addr string) bool {
	_, err := getVars(addr)
	return err == nil
}

// IsHealthy returns whether the monitored Vitess process has started
// successfully.
func (vtp *VtProcess) IsHealthy() bool {
	healthCheck := vtp.HealthCheck
	if healthCheck == nil {
		healthCheck = defaultHealthCheck
	}
	return healthCheck(vtp.Address())
}

// Address returns the main address for this Vitess process.
// This is usually the main HTTP endpoint for the service.
func (vtp *VtProcess) Address() string {
	return fmt.Sprintf("%s:%d", vtp.BindAddress, vtp.Port)
}

// WaitTerminate attempts to gracefully shutdown the Vitess process by sending
// a SIGTERM, then wait for up to 10s for it to exit. If the process hasn't
// exited cleanly after 10s, a SIGKILL is forced and the corresponding exit
// error is returned to the user
func (vtp *VtProcess) WaitTerminate() error {
	if vtp.proc == nil || vtp.exit == nil {
		return nil
	}

	// Attempt graceful shutdown with SIGTERM first
	vtp.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-vtp.exit:
		vtp.proc = nil
		return err

	case <-time.After(10 * time.Second):
		vtp.proc.Process.Kill()
		vtp.proc = nil
		return <-vtp.exit
	}
}

// WaitStart spawns this Vitess process and waits for it to be up
// and running. The process is considered "up" when it starts serving
// its debug HTTP endpoint -- this means the process was successfully
// started.
// If the process is not healthy after 60s, this method will timeout and
// return an error.
func (vtp *VtProcess) WaitStart() (err error) {
	vtp.proc = exec.Command(
		vtp.Binary,
		"--port", strconv.Itoa(vtp.Port),
		"--bind-address", vtp.BindAddress,
		"--log_dir", vtp.LogDirectory,
		"--alsologtostderr",
	)

	if vtp.PortGrpc != 0 {
		vtp.proc.Args = append(vtp.proc.Args, "--grpc-port")
		vtp.proc.Args = append(vtp.proc.Args, strconv.Itoa(vtp.PortGrpc))
	}

	if vtp.BindAddressGprc != "" {
		vtp.proc.Args = append(vtp.proc.Args, "--grpc-bind-address")
		vtp.proc.Args = append(vtp.proc.Args, vtp.BindAddressGprc)
	}

	vtp.proc.Args = append(vtp.proc.Args, vtp.ExtraArgs...)
	vtp.proc.Env = append(vtp.proc.Env, os.Environ()...)
	vtp.proc.Env = append(vtp.proc.Env, vtp.Env...)
	if !testing.Testing() || testing.Verbose() {
		vtp.proc.Stderr = os.Stderr
		vtp.proc.Stdout = os.Stdout
	}

	log.Infof("%v %v", strings.Join(vtp.proc.Args, " "))
	err = vtp.proc.Start()
	if err != nil {
		return
	}

	vtp.exit = make(chan error)
	go func() {
		vtp.exit <- vtp.proc.Wait()
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtp.IsHealthy() {
			return nil
		}

		select {
		case err := <-vtp.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vtp.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	vtp.proc.Process.Kill()
	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vtp.Name, <-vtp.exit)
}

const (
	// DefaultCharset is the default charset used by MySQL instances
	DefaultCharset = "utf8mb4"
)

// QueryServerArgs are the default arguments passed to all Vitess query servers
var QueryServerArgs = []string{
	"--queryserver-config-pool-size", "4",
	"--queryserver-config-query-timeout", "300s",
	"--queryserver-config-schema-reload-time", "60s",
	"--queryserver-config-stream-pool-size", "4",
	"--queryserver-config-transaction-cap", "4",
	"--queryserver-config-txpool-timeout", "300s",
}

// VtcomboProcess returns a VtProcess handle for a local `vtcombo` service,
// configured with the given Config.
// The process must be manually started by calling WaitStart()
func VtcomboProcess(environment Environment, args *Config, mysql MySQLManager) (*VtProcess, error) {
	vtcomboBindAddress := "127.0.0.1"
	if args.VtComboBindAddress != "" {
		vtcomboBindAddress = args.VtComboBindAddress
	}
	grpcBindAddress := ""
	if servenv.GRPCBindAddress() != "" {
		grpcBindAddress = servenv.GRPCBindAddress()
	}

	vt := &VtProcess{
		Name:            "vtcombo",
		Directory:       environment.Directory(),
		LogDirectory:    environment.LogDirectory(),
		Binary:          environment.BinaryPath("vtcombo"),
		BindAddress:     vtcomboBindAddress,
		BindAddressGprc: grpcBindAddress,
		Port:            environment.PortForProtocol("vtcombo", ""),
		PortGrpc:        environment.PortForProtocol("vtcombo", "grpc"),
		HealthCheck:     environment.ProcessHealthCheck("vtcombo"),
		Env:             environment.EnvVars(),
	}

	user, pass := mysql.Auth()
	socket := mysql.UnixSocket()
	charset := args.Charset
	if charset == "" {
		charset = DefaultCharset
	}
	protoTopo, _ := prototext.Marshal(args.Topology)
	vt.ExtraArgs = append(vt.ExtraArgs, []string{
		//TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
		"--db-charset", charset,
		"--db-app-user", user,
		"--db-app-password", pass,
		"--db-dba-user", user,
		"--db-dba-password", pass,
		"--proto-topo", string(protoTopo),
		"--mycnf-server-id", "1",
		"--mycnf-socket-file", socket,
		"--normalize-queries",
		"--dbddl-plugin", "vttest",
		"--foreign-key-mode", args.ForeignKeyMode,
		"--planner-version", args.PlannerVersion,
		fmt.Sprintf("--enable-online-ddl=%t", args.EnableOnlineDDL),
		fmt.Sprintf("--enable-direct-ddl=%t", args.EnableDirectDDL),
		fmt.Sprintf("--enable-system-settings=%t", args.EnableSystemSettings),
		fmt.Sprintf("--no-scatter=%t", args.NoScatter),
	}...)

	// If topo tablet refresh interval is not defined then we will give it value of 10s. Please note
	// that the default value is 1 minute, but we are keeping it low to make vttestserver perform faster.
	// Less value might result in high pressure on topo but for testing purpose that should not be a concern.
	if args.VtgateTabletRefreshInterval <= 0 {
		vt.ExtraArgs = append(vt.ExtraArgs, fmt.Sprintf("--tablet-refresh-interval=%v", 10*time.Second))
	} else {
		vt.ExtraArgs = append(vt.ExtraArgs, fmt.Sprintf("--tablet-refresh-interval=%v", args.VtgateTabletRefreshInterval))
	}

	// If gateway initial tablet timeout is not defined then we will give it value of 30s (vtcombo default).
	// Setting it to a lower value will reduce the time VTGate waits for tablets at startup.
	if args.VtgateGatewayInitialTabletTimeout <= 0 {
		vt.ExtraArgs = append(vt.ExtraArgs, fmt.Sprintf("--gateway-initial-tablet-timeout=%v", 30*time.Second))
	} else {
		vt.ExtraArgs = append(vt.ExtraArgs, fmt.Sprintf("--gateway-initial-tablet-timeout=%v", args.VtgateGatewayInitialTabletTimeout))
	}

	vt.ExtraArgs = append(vt.ExtraArgs, QueryServerArgs...)
	vt.ExtraArgs = append(vt.ExtraArgs, environment.VtcomboArguments()...)

	//TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
	if args.SchemaDir != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"--schema-dir", args.SchemaDir}...)
	}
	if args.PersistentMode && args.DataDir != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"--vschema-persistence-dir", path.Join(args.DataDir, "vschema_data")}...)
	}
	if args.TransactionMode != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"--transaction-mode", args.TransactionMode}...)
	}
	if args.TransactionTimeout != 0 {
		vt.ExtraArgs = append(vt.ExtraArgs, "--queryserver-config-transaction-timeout", fmt.Sprintf("%v", args.TransactionTimeout))
	}
	if args.TabletHostName != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"--tablet-hostname", args.TabletHostName}...)
	}
	if servenv.GRPCAuth() == "mtls" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"--grpc-auth-mode", servenv.GRPCAuth(), "--grpc-key", servenv.GRPCKey(), "--grpc-cert", servenv.GRPCCert(), "--grpc-ca", servenv.GRPCCertificateAuthority(), "--grpc-auth-mtls-allowed-substrings", servenv.ClientCertSubstrings()}...)
	}
	vtVer, err := getMajorVersion(vt.Binary)
	if err != nil {
		return nil, err
	}
	if args.VSchemaDDLAuthorizedUsers != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{vtutils.GetFlagVariantForTestsByVersion("--vschema-ddl-authorized-users", vtVer), args.VSchemaDDLAuthorizedUsers}...)
	}
	vt.ExtraArgs = append(vt.ExtraArgs, "--mysql-server-version", servenv.MySQLServerVersion())
	if socket != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"--db-socket", socket,
		}...)
	} else {
		hostname, p := mysql.Address()
		port := strconv.Itoa(p)

		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"--db-host", hostname,
			"--db-port", port,
		}...)
	}

	vtcomboMysqlPort := environment.PortForProtocol("vtcombo_mysql_port", "")
	vtcomboMysqlBindAddress := "localhost"
	if args.MySQLBindHost != "" {
		vtcomboMysqlBindAddress = args.MySQLBindHost
	}

	vt.ExtraArgs = append(vt.ExtraArgs, []string{
		"--mysql-auth-server-impl", "none",
		"--mysql-server-port", strconv.Itoa(vtcomboMysqlPort),
		"--mysql-server-bind-address", vtcomboMysqlBindAddress,
	}...)

	if args.ExternalTopoImplementation != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"--external-topo-server",
			"--topo-implementation", args.ExternalTopoImplementation,
			"--topo-global-server-address", args.ExternalTopoGlobalServerAddress,
			"--topo-global-root", args.ExternalTopoGlobalRoot,
		}...)
	}

	return vt, nil
}
