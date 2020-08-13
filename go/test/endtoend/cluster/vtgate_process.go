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
	"path"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VtgateProcess is a generic handle for a running vtgate .
// It can be spawned manually
type VtgateProcess struct {
	Name                  string
	Binary                string
	CommonArg             VtctlProcess
	LogDir                string
	FileToLogQueries      string
	Port                  int
	GrpcPort              int
	MySQLServerPort       int
	MySQLServerSocketPath string
	Cell                  string
	CellsToWatch          string
	TabletTypesToWait     string
	GatewayImplementation string
	ServiceMap            string
	MySQLAuthServerImpl   string
	Directory             string
	VerifyURL             string
	SysVarSetEnabled      bool
	//Extra Args to be set before starting the vtgate process
	ExtraArgs []string

	proc *exec.Cmd
	exit chan error
}

// Setup starts Vtgate process with required arguements
func (vtgate *VtgateProcess) Setup() (err error) {

	args := []string{
		"-topo_implementation", vtgate.CommonArg.TopoImplementation,
		"-topo_global_server_address", vtgate.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vtgate.CommonArg.TopoGlobalRoot,
		"-log_dir", vtgate.LogDir,
		"-log_queries_to_file", vtgate.FileToLogQueries,
		"-port", fmt.Sprintf("%d", vtgate.Port),
		"-grpc_port", fmt.Sprintf("%d", vtgate.GrpcPort),
		"-mysql_server_port", fmt.Sprintf("%d", vtgate.MySQLServerPort),
		"-mysql_server_socket_path", vtgate.MySQLServerSocketPath,
		"-cell", vtgate.Cell,
		"-cells_to_watch", vtgate.CellsToWatch,
		"-tablet_types_to_wait", vtgate.TabletTypesToWait,
		"-gateway_implementation", vtgate.GatewayImplementation,
		"-service_map", vtgate.ServiceMap,
		"-mysql_auth_server_impl", vtgate.MySQLAuthServerImpl,
	}
	if vtgate.SysVarSetEnabled {
		args = append(args, "-enable_system_settings")
	}
	vtgate.proc = exec.Command(
		vtgate.Binary,
		args...,
	)
	if *isCoverage {
		vtgate.proc.Args = append(vtgate.proc.Args, "-test.coverprofile="+getCoveragePath("vtgate.out"))
	}

	vtgate.proc.Args = append(vtgate.proc.Args, vtgate.ExtraArgs...)

	errFile, _ := os.Create(path.Join(vtgate.LogDir, "vtgate-stderr.txt"))
	vtgate.proc.Stderr = errFile

	vtgate.proc.Env = append(vtgate.proc.Env, os.Environ()...)

	log.Infof("Running vtgate with command: %v", strings.Join(vtgate.proc.Args, " "))

	err = vtgate.proc.Start()
	if err != nil {
		return
	}
	vtgate.exit = make(chan error)
	go func() {
		if vtgate.proc != nil {
			vtgate.exit <- vtgate.proc.Wait()
		}
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtgate.WaitForStatus() {
			return nil
		}
		select {
		case err := <-vtgate.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vtgate.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vtgate.Name, <-vtgate.exit)
}

// WaitForStatus function checks if vtgate process is up and running
func (vtgate *VtgateProcess) WaitForStatus() bool {
	resp, err := http.Get(vtgate.VerifyURL)
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		return true
	}
	return false
}

// GetStatusForTabletOfShard function gets status for a specific tablet of a shard in keyspace
// endPointsCount : number of endpoints
func (vtgate *VtgateProcess) GetStatusForTabletOfShard(name string, endPointsCount int) bool {
	resp, err := http.Get(vtgate.VerifyURL)
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			panic(err)
		}
		object := reflect.ValueOf(resultMap["HealthcheckConnections"])
		masterConnectionExist := false
		if object.Kind() == reflect.Map {
			for _, key := range object.MapKeys() {
				if key.String() == name {
					value := fmt.Sprintf("%v", object.MapIndex(key))
					countStr := strconv.Itoa(endPointsCount)
					return value == countStr
				}
			}
		}
		return masterConnectionExist
	}
	return false
}

// WaitForStatusOfTabletInShard function waits till status of a tablet in shard is 1
// endPointsCount: how many endpoints to wait for
func (vtgate *VtgateProcess) WaitForStatusOfTabletInShard(name string, endPointsCount int) error {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		if vtgate.GetStatusForTabletOfShard(name, endPointsCount) {
			return nil
		}
		select {
		case err := <-vtgate.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vtgate.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}
	return fmt.Errorf("wait for %s failed", name)
}

// TearDown shuts down the running vtgate service
func (vtgate *VtgateProcess) TearDown() error {
	if vtgate.proc == nil || vtgate.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	vtgate.proc.Process.Signal(syscall.SIGTERM)

	// We are not checking vtgate's exit code because it sometimes
	// returns exit code 2, even though vtgate terminates cleanly.
	select {
	case <-vtgate.exit:
		vtgate.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		vtgate.proc.Process.Kill()
		vtgate.proc = nil
		return <-vtgate.exit
	}
}

// VtgateProcessInstance returns a Vtgate handle for vtgate process
// configured with the given Config.
// The process must be manually started by calling setup()
func VtgateProcessInstance(port int, grpcPort int, mySQLServerPort int, cell string, cellsToWatch string, hostname string, tabletTypesToWait string, topoPort int, tmpDirectory string, extraArgs []string) *VtgateProcess {
	vtctl := VtctlProcessInstance(topoPort, hostname)
	vtgate := &VtgateProcess{
		Name:                  "vtgate",
		Binary:                "vtgate",
		FileToLogQueries:      path.Join(tmpDirectory, "/vtgate_querylog.txt"),
		Directory:             os.Getenv("VTDATAROOT"),
		ServiceMap:            "grpc-tabletmanager,grpc-throttler,grpc-queryservice,grpc-updatestream,grpc-vtctl,grpc-vtworker,grpc-vtgateservice",
		LogDir:                tmpDirectory,
		Port:                  port,
		GrpcPort:              grpcPort,
		MySQLServerPort:       mySQLServerPort,
		MySQLServerSocketPath: path.Join(tmpDirectory, "mysql.sock"),
		Cell:                  cell,
		CellsToWatch:          cellsToWatch,
		TabletTypesToWait:     tabletTypesToWait,
		GatewayImplementation: "tabletgateway",
		CommonArg:             *vtctl,
		MySQLAuthServerImpl:   "none",
		ExtraArgs:             extraArgs,
	}

	vtgate.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", hostname, port)

	return vtgate
}

// GetVars returns map of vars
func (vtgate *VtgateProcess) GetVars() (map[string]interface{}, error) {
	resultMap := make(map[string]interface{})
	resp, err := http.Get(vtgate.VerifyURL)
	if err != nil {
		return nil, fmt.Errorf("error getting response from %s", vtgate.VerifyURL)
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
