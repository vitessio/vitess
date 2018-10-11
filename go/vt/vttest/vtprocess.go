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

// Package vttest contains helpers to set up Vitess for testing.
package vttest

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"

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
	return ioutil.ReadAll(resp.Body)
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
	return fmt.Sprintf("localhost:%d", vtp.Port)
}

// WaitTerminate attemps to gracefully shutdown the Vitess process by sending
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
		"-port", fmt.Sprintf("%d", vtp.Port),
		"-log_dir", vtp.LogDirectory,
		"-alsologtostderr",
	)

	if vtp.PortGrpc != 0 {
		vtp.proc.Args = append(vtp.proc.Args, "-grpc_port")
		vtp.proc.Args = append(vtp.proc.Args, fmt.Sprintf("%d", vtp.PortGrpc))
	}

	vtp.proc.Args = append(vtp.proc.Args, vtp.ExtraArgs...)

	vtp.proc.Stderr = os.Stderr
	vtp.proc.Stdout = os.Stdout

	vtp.proc.Env = append(vtp.proc.Env, os.Environ()...)
	vtp.proc.Env = append(vtp.proc.Env, vtp.Env...)

	vtp.proc.Stderr = os.Stderr
	vtp.proc.Stderr = os.Stdout

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
		"-db_charset", charset,
		"-db_app_user", user,
		"-db_app_password", pass,
		"-db_dba_user", user,
		"-db_dba_password", pass,
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
	if args.TransactionMode != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{"-transaction_mode", args.TransactionMode}...)
	}

	if socket != "" {
		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"-db_socket", socket,
		}...)
	} else {
		hostname, p := mysql.Address()
		port := fmt.Sprintf("%d", p)

		vt.ExtraArgs = append(vt.ExtraArgs, []string{
			"-db_host", hostname,
			"-db_port", port,
		}...)
	}

	vtcomboMysqlPort := env.PortForProtocol("vtcombo_mysql_port", "")
	vtcomboMysqlBindAddress := "localhost"
	if args.MySQLBindHost != "" {
		vtcomboMysqlBindAddress = args.MySQLBindHost
	}

	vt.ExtraArgs = append(vt.ExtraArgs, []string{
		"-mysql_auth_server_impl", "none",
		"-mysql_server_port", fmt.Sprintf("%d", vtcomboMysqlPort),
		"-mysql_server_bind_address", vtcomboMysqlBindAddress,
	}...)

	return vt
}
