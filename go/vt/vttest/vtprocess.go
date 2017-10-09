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
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
)

// HealthChecker is a callback that impements a service-specific health check
// It must return true if the service at the given `addr` is reachable, false
// otherwerise.
type HealthChecker func(addr string) bool

// VtProcess is a generic handle for a running Vitess process.
// It can be spawned manually or through one of the available
// helper methods.
type VtProcess struct {
	Name         string
	Directory    string
	LogDirectory string
	Binary       string
	ExtraArgs    []string
	Env          []string
	Port         int
	PortGrpc     int
	HealthCheck  HealthChecker

	proc *exec.Cmd
}

// GetVars returns the JSON contents of the `/debug/vars` endpoint
// of this Vitess-based process. If an error is returned, it probably
// means that the Vitess service has not started successfully.
func (vtp *VtProcess) GetVars() ([]byte, error) {
	url := fmt.Sprintf("http://%s/debug/vars", vtp.Address())
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// IsHealthy returns whether the monitored Vitess process has started
// successfully.
func (vtp *VtProcess) IsHealthy() bool {
	if vtp.HealthCheck != nil && !vtp.HealthCheck(vtp.Address()) {
		return false
	}
	_, err := vtp.GetVars()
	return err == nil
}

// Address returns the main address for this Vitess process.
// This is usually the main HTTP endpoint for the service.
func (vtp *VtProcess) Address() string {
	return fmt.Sprintf("localhost:%d", vtp.Port)
}

// Kill kills the running Vitess process. If the process is not running,
// this is a no-op.
func (vtp *VtProcess) Kill() {
	if vtp.proc != nil && vtp.proc.Process != nil {
		vtp.proc.Process.Kill()
	}
}

// Wait waits for the Vitess process to terminate and returns any exit errors.
// Note that most Vitess processes are long-running services, so you will need
// to call Kill on them before you can Wait.
func (vtp *VtProcess) Wait() error {
	if vtp.proc == nil {
		return nil
	}

	err := vtp.proc.Wait()
	if _, ok := err.(*exec.ExitError); ok {
		return nil
	}

	return err
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
		"-port", fmt.Sprintf("%d", vtp.Port),
		"-log_dir", vtp.LogDirectory,
		"-alsologtostderr",
	)

	if vtp.PortGrpc != 0 {
		vtp.proc.Args = append(vtp.proc.Args, "-grpc_port")
		vtp.proc.Args = append(vtp.proc.Args, fmt.Sprintf("%d", vtp.PortGrpc))
	}

	vtp.proc.Args = append(vtp.proc.Args, vtp.ExtraArgs...)

	logfile := path.Join(vtp.LogDirectory, fmt.Sprintf("%s.%d.log", vtp.Name, vtp.Port))
	vtp.proc.Stdout, err = os.Create(logfile)
	if err != nil {
		return
	}

	vtp.proc.Env = append(vtp.proc.Env, os.Environ()...)
	vtp.proc.Env = append(vtp.proc.Env, vtp.Env...)

	err = vtp.proc.Start()
	if err != nil {
		return
	}

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtp.IsHealthy() {
			return nil
		}

		if err := vtp.proc.Process.Signal(syscall.Signal(0)); err != nil {
			return fmt.Errorf("process '%s' exited prematurely", vtp.Name)
		}

		time.Sleep(300 * time.Millisecond)
	}

	vtp.proc.Process.Kill()
	return fmt.Errorf("process '%s' timed out after 60s", vtp.Name)
}

// DefaultCharset is the default charset used by MySQL instances
const DefaultCharset = "utf8"

// QueryServerArgs are the default arguments passed to all Vitess query servers
var QueryServerArgs = []string{
	"-queryserver-config-pool-size", "4",
	"-queryserver-config-query-timeout", "300",
	"-queryserver-config-schema-reload-time", "60",
	"-queryserver-config-stream-pool-size", "4",
	"-queryserver-config-transaction-cap", "4",
	"-queryserver-config-transaction-timeout", "300",
	"-queryserver-config-txpool-timeout", "300",
}

// VtcomboProcess returns a VtProcess handle for a local `vtcombo` service,
// configured with the given Config.
// The process must be manually started by calling WaitStart()
func VtcomboProcess(env Environment, args *Config, mysql MySQLManager) *VtProcess {
	vt := &VtProcess{
		Name:         "vtcombo",
		Directory:    env.Directory(),
		LogDirectory: env.LogDirectory(),
		Binary:       env.BinaryPath("vtcombo"),
		Port:         env.PortForProtocol("vtcombo", ""),
		PortGrpc:     env.PortForProtocol("vtcombo", "grpc"),
		HealthCheck:  env.ProcessHealthCheck("vtcombo"),
		Env:          env.EnvVars(),
	}

	user, pass := mysql.Auth()
	socket := mysql.UnixSocket()
	charset := args.Charset
	if charset == "" {
		charset = DefaultCharset
	}

	vt.ExtraArgs = append(vt.ExtraArgs, []string{
		"-db-config-app-charset", charset,
		"-db-config-app-uname", user,
		"-db-config-app-pass", pass,
		"-db-config-dba-charset", charset,
		"-db-config-dba-uname", user,
		"-db-config-dba-pass", pass,
		"-proto_topo", proto.CompactTextString(args.Topology),
		"-mycnf_server_id", "1",
		"-mycnf_socket_file", socket,
		"-normalize_queries",
	}...)

	vt.ExtraArgs = append(vt.ExtraArgs, QueryServerArgs...)
	vt.ExtraArgs = append(vt.ExtraArgs, env.VtcomboArguments()...)

	if args.SchemaDir != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"-schema_dir", args.SchemaDir}...)
	}
	if args.WebDir != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"-web_dir", args.WebDir}...)
	}
	if args.WebDir2 != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"-web_dir2", args.WebDir2}...)
	}

	if socket != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"-db-config-app-unixsocket", socket,
			"-db-config-dba-unixsocket", socket,
		}...)
	} else {
		hostname, p := mysql.Address()
		port := fmt.Sprintf("%d", p)

		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"-db-config-app-host", hostname,
			"-db-config-app-port", port,
			"-db-config-dba-host", hostname,
			"-db-config-dba-port", port,
		}...)
	}

	vtcomboMysqlPort := env.PortForProtocol("vtcombo_mysql_port", "")
	vt.ExtraArgs = append(vt.ExtraArgs, []string{
		"-mysql_auth_server_impl", "none",
		"-mysql_server_port", fmt.Sprintf("%d", vtcomboMysqlPort),
		"-mysql_server_bind_address", "localhost",
	}...)

	return vt
}
