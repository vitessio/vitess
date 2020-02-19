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

package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VtworkerProcess is a generic handle for a running vtworker .
// It can be spawned manually
type VtworkerProcess struct {
	Name                   string
	Binary                 string
	CommonArg              VtctlProcess
	ServiceMap             string
	LogDir                 string
	Port                   int
	GrpcPort               int
	VerifyURL              string
	Directory              string
	ExecuteRetryTime       string
	Cell                   string
	Server                 string
	CommandDisplayInterval string
	ExtraArgs              []string

	proc *exec.Cmd
	exit chan error
}

// Setup starts vtworker process with required arguements
func (vtworker *VtworkerProcess) Setup(cell string) (err error) {

	vtworker.proc = exec.Command(
		vtworker.Binary,
		"-log_dir", vtworker.LogDir,
		"-port", fmt.Sprintf("%d", vtworker.Port),
		"-executefetch_retry_time", vtworker.ExecuteRetryTime,
		"-tablet_manager_protocol", "grpc",
		"-tablet_protocol", "grpc",
		"-topo_implementation", vtworker.CommonArg.TopoImplementation,
		"-topo_global_server_address", vtworker.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vtworker.CommonArg.TopoGlobalRoot,
		"-service_map", vtworker.ServiceMap,
		"-grpc_port", fmt.Sprintf("%d", vtworker.GrpcPort),
		"-cell", cell,
		"-command_display_interval", "10ms",
	)
	vtworker.proc.Args = append(vtworker.proc.Args, vtworker.ExtraArgs...)

	vtworker.proc.Stderr = os.Stderr
	vtworker.proc.Stdout = os.Stdout

	vtworker.proc.Env = append(vtworker.proc.Env, os.Environ()...)

	log.Infof("%v", strings.Join(vtworker.proc.Args, " "))

	err = vtworker.proc.Start()
	if err != nil {
		return
	}

	vtworker.exit = make(chan error)
	go func() {
		vtworker.exit <- vtworker.proc.Wait()
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtworker.IsHealthy() {
			return nil
		}
		select {
		case err := <-vtworker.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vtworker.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vtworker.Name, <-vtworker.exit)
}

// IsHealthy function checks if vtworker process is up and running
func (vtworker *VtworkerProcess) IsHealthy() bool {
	resp, err := http.Get(vtworker.VerifyURL)
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		return true
	}
	return false
}

// TearDown shutdowns the running vtworker process
func (vtworker *VtworkerProcess) TearDown() error {
	if vtworker.proc == nil || vtworker.exit == nil {
		return nil
	}

	// Attempt graceful shutdown with SIGTERM first
	vtworker.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-vtworker.exit:
		vtworker.proc = nil
		return err

	case <-time.After(10 * time.Second):
		vtworker.proc.Process.Kill()
		vtworker.proc = nil
		return <-vtworker.exit
	}
}

// ExecuteCommand executes any vtworker command
func (vtworker *VtworkerProcess) ExecuteCommand(args ...string) (err error) {
	args = append([]string{"-vtworker_client_protocol", "grpc",
		"-server", vtworker.Server, "-log_dir", vtworker.LogDir, "-stderrthreshold", "info"}, args...)
	tmpProcess := exec.Command(
		"vtworkerclient",
		args...,
	)
	log.Info(fmt.Sprintf("Executing vtworkerclient with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

func (vtworker *VtworkerProcess) ExecuteCommandInBg(args ...string) (*exec.Cmd, error) {
	args = append([]string{"-vtworker_client_protocol", "grpc",
		"-server", vtworker.Server, "-log_dir", vtworker.LogDir, "-stderrthreshold", "info"}, args...)
	tmpProcess := exec.Command(
		"vtworkerclient",
		args...,
	)
	log.Info(fmt.Sprintf("Executing vtworkerclient with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess, tmpProcess.Start()
}

// ExecuteVtworkerCommand executes any vtworker command
func (vtworker *VtworkerProcess) ExecuteVtworkerCommand(port int, grpcPort int, args ...string) (err error) {
	args = append([]string{
		"-port", fmt.Sprintf("%d", port),
		"-executefetch_retry_time", vtworker.ExecuteRetryTime,
		"-tablet_manager_protocol", "grpc",
		"-tablet_protocol", "grpc",
		"-topo_implementation", vtworker.CommonArg.TopoImplementation,
		"-topo_global_server_address", vtworker.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vtworker.CommonArg.TopoGlobalRoot,
		"-service_map", vtworker.ServiceMap,
		"-grpc_port", fmt.Sprintf("%d", grpcPort),
		"-cell", vtworker.Cell,
		"-log_dir", vtworker.LogDir, "-stderrthreshold", "1"}, args...)
	tmpProcess := exec.Command(
		"vtworker",
		args...,
	)
	log.Info(fmt.Sprintf("Executing vtworker with arguments %v", strings.Join(tmpProcess.Args, " ")))
	return tmpProcess.Run()
}

// VtworkerProcessInstance returns a vtworker handle
// configured with the given Config.
// The process must be manually started by calling Setup()
func VtworkerProcessInstance(httpPort int, grpcPort int, topoPort int, hostname string, tmpDirectory string) *VtworkerProcess {
	vtctl := VtctlProcessInstance(topoPort, hostname)
	vtworker := &VtworkerProcess{
		Name:                   "vtworker",
		Binary:                 "vtworker",
		CommonArg:              *vtctl,
		ServiceMap:             "grpc-tabletmanager,grpc-throttler,grpc-queryservice,grpc-updatestream,grpc-vtctl,grpc-vtworker,grpc-vtgateservice",
		LogDir:                 tmpDirectory,
		Port:                   httpPort,
		GrpcPort:               grpcPort,
		ExecuteRetryTime:       "1s",
		CommandDisplayInterval: "10ms",
		Directory:              os.Getenv("VTDATAROOT"),
		Server:                 fmt.Sprintf("%s:%d", hostname, grpcPort),
	}
	vtworker.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", hostname, vtworker.Port)
	return vtworker
}

// GetVars returns map of vars
func (vtworker *VtworkerProcess) GetVars() (map[string]interface{}, error) {
	resultMap := make(map[string]interface{})
	resp, err := http.Get(vtworker.VerifyURL)
	if err != nil {
		return nil, fmt.Errorf("error getting response from %s", vtworker.VerifyURL)
	}
	if resp.StatusCode == 200 {
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			return nil, fmt.Errorf("not able to parse response body")
		}
		return resultMap, nil
	}
	return nil, fmt.Errorf("unsuccessful response")
}
