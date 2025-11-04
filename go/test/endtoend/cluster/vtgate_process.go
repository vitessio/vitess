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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// VtgateProcess is a generic handle for a running vtgate .
// It can be spawned manually
type VtgateProcess struct {
	VtProcess
	LogDir                string
	ErrorLog              string
	FileToLogQueries      string
	Port                  int
	GrpcPort              int
	MySQLServerPort       int
	MySQLServerSocketPath string
	Cell                  string
	CellsToWatch          string
	TabletTypesToWait     string
	ServiceMap            string
	MySQLAuthServerImpl   string
	Directory             string
	VerifyURL             string
	VSchemaURL            string
	QueryPlanURL          string
	ConfigFile            string
	Config                VTGateConfiguration
	SysVarSetEnabled      bool
	PlannerVersion        plancontext.PlannerVersion
	// Extra Args to be set before starting the vtgate process
	ExtraArgs []string

	proc *exec.Cmd
	exit chan error
}

type VTGateConfiguration struct {
	TransactionMode                   string `json:"transaction_mode,omitempty"`
	DiscoveryLowReplicationLag        string `json:"discovery_low_replication_lag,omitempty"`
	DiscoveryHighReplicationLag       string `json:"discovery_high_replication_lag,omitempty"`
	DiscoveryMinServingVttablets      string `json:"discovery_min_number_serving_vttablets,omitempty"`
	DiscoveryLegacyReplicationLagAlgo string `json:"discovery_legacy_replication_lag_algorithm"`
}

// ToJSONString will marshal this configuration as JSON
func (config *VTGateConfiguration) ToJSONString() string {
	b, _ := json.MarshalIndent(config, "", "\t")
	return string(b)
}

func (vtgate *VtgateProcess) RewriteConfiguration() error {
	return os.WriteFile(vtgate.ConfigFile, []byte(vtgate.Config.ToJSONString()), 0644)
}

// WaitForConfig waits for the expectedConfig to be present in the vtgate configuration.
func (vtgate *VtgateProcess) WaitForConfig(expectedConfig string) error {
	timeout := time.After(30 * time.Second)
	var response string
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for api to work. Last response - %s", response)
		default:
			_, response, _ = vtgate.MakeAPICall("/debug/config")
			if strings.Contains(response, expectedConfig) {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// MakeAPICall makes an API call on the given endpoint of VTOrc
func (vtgate *VtgateProcess) MakeAPICall(endpoint string) (status int, response string, err error) {
	url := fmt.Sprintf("http://localhost:%d/%s", vtgate.Port, endpoint)
	resp, err := http.Get(url)
	if err != nil {
		if resp != nil {
			status = resp.StatusCode
		}
		return status, "", err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	respByte, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respByte), err
}

// MakeAPICallRetry is used to make an API call and retries until success
func (vtgate *VtgateProcess) MakeAPICallRetry(t *testing.T, url string) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for api to work")
			return
		default:
			status, _, err := vtgate.MakeAPICall(url)
			if err == nil && status == 200 {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

const defaultVtGatePlannerVersion = planbuilder.Gen4

// Setup starts Vtgate process with required arguements
func (vtgate *VtgateProcess) Setup() (err error) {
	args := []string{
		//TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
		"--topo_implementation", vtgate.TopoImplementation,
		"--topo_global_server_address", vtgate.TopoGlobalAddress,
		"--topo_global_root", vtgate.TopoGlobalRoot,
		"--config-file", vtgate.ConfigFile,
		"--log_dir", vtgate.LogDir,
		"--log_queries_to_file", vtgate.FileToLogQueries,
		"--port", strconv.Itoa(vtgate.Port),
		"--grpc_port", strconv.Itoa(vtgate.GrpcPort),
		"--mysql_server_port", strconv.Itoa(vtgate.MySQLServerPort),
		"--mysql_server_socket_path", vtgate.MySQLServerSocketPath,
		"--cell", vtgate.Cell,
		"--cells_to_watch", vtgate.CellsToWatch,
		"--tablet_types_to_wait", vtgate.TabletTypesToWait,
		"--service_map", vtgate.ServiceMap,
		"--mysql_auth_server_impl", vtgate.MySQLAuthServerImpl,
		"--bind-address", "127.0.0.1",
		"--grpc_bind_address", "127.0.0.1",
	}

	// If no explicit mysql_server_version has been specified then we autodetect
	// the MySQL version that will be used for the test and base the vtgate's
	// mysql server version on that.
	msvflag := false
	for _, f := range vtgate.ExtraArgs {
		// TODO: Replace flag with dashed version in v25
		if strings.Contains(f, "mysql_server_version") {
			msvflag = true
			break
		}
	}
	configFile, err := os.Create(vtgate.ConfigFile)
	if err != nil {
		log.Errorf("cannot create config file for vtgate: %v", err)
		return err
	}
	_, err = configFile.WriteString(vtgate.Config.ToJSONString())
	if err != nil {
		return err
	}
	err = configFile.Close()
	if err != nil {
		return err
	}
	if !msvflag {
		version, err := mysqlctl.GetVersionString()
		if err != nil {
			return err
		}
		_, vers, err := mysqlctl.ParseVersionString(version)
		if err != nil {
			return err
		}
		mysqlvers := fmt.Sprintf("%d.%d.%d-vitess", vers.Major, vers.Minor, vers.Patch)
		// TODO: Replace flag with dashed version in v25
		args = append(args, "--mysql_server_version", mysqlvers)
	}
	if vtgate.PlannerVersion > 0 {
		args = append(args, "--planner-version", vtgate.PlannerVersion.String())
	}
	if vtgate.SysVarSetEnabled {
		args = append(args, "--enable_system_settings")
	}
	vtgate.proc = exec.Command(
		vtgate.Binary,
		args...,
	)
	if *isCoverage {
		vtgate.proc.Args = append(vtgate.proc.Args, "--test.coverprofile="+getCoveragePath("vtgate.out"))
	}

	vtgate.proc.Args = append(vtgate.proc.Args, vtgate.ExtraArgs...)

	errFile, err := os.Create(path.Join(vtgate.LogDir, "vtgate-stderr.txt"))
	if err != nil {
		log.Errorf("cannot create error log file for vtgate: %v", err)
		return err
	}
	vtgate.proc.Stderr = errFile
	vtgate.ErrorLog = errFile.Name()

	vtgate.proc.Env = append(vtgate.proc.Env, os.Environ()...)
	vtgate.proc.Env = append(vtgate.proc.Env, DefaultVttestEnv)

	log.Infof("Running vtgate with command: %v", strings.Join(vtgate.proc.Args, " "))

	err = vtgate.proc.Start()
	if err != nil {
		return
	}
	vtgate.exit = make(chan error)
	go func() {
		if vtgate.proc != nil {
			vtgate.exit <- vtgate.proc.Wait()
			close(vtgate.exit)
		}
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtgate.WaitForStatus() {
			return nil
		}
		select {
		case err := <-vtgate.exit:
			errBytes, ferr := os.ReadFile(vtgate.ErrorLog)
			if ferr == nil {
				log.Errorf("vtgate error log contents:\n%s", string(errBytes))
			} else {
				log.Errorf("Failed to read the vtgate error log file %q: %v", vtgate.ErrorLog, ferr)
			}
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
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

// GetStatusForTabletOfShard function gets status for a specific tablet of a shard in keyspace
// endPointsCount : number of endpoints
func (vtgate *VtgateProcess) GetStatusForTabletOfShard(name string, endPointsCount int) bool {
	resp, err := http.Get(vtgate.VerifyURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		resultMap := make(map[string]any)
		respByte, _ := io.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			panic(err)
		}
		object := reflect.ValueOf(resultMap["HealthcheckConnections"])
		if object.Kind() == reflect.Map {
			for _, key := range object.MapKeys() {
				if key.String() == name {
					value := fmt.Sprintf("%v", object.MapIndex(key))
					countStr := strconv.Itoa(endPointsCount)
					return value == countStr
				}
			}
		}
	}
	return false
}

// WaitForStatusOfTabletInShard function waits till status of a tablet in shard is 1
// endPointsCount: how many endpoints to wait for
func (vtgate *VtgateProcess) WaitForStatusOfTabletInShard(name string, endPointsCount int, timeout time.Duration) error {
	log.Infof("Waiting for healthy status of %d %s tablets in cell %s",
		endPointsCount, name, vtgate.Cell)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
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

// IsShutdown checks if the vtgate process is shutdown
func (vtgate *VtgateProcess) IsShutdown() bool {
	return !vtgate.WaitForStatus()
}

// Terminate sends a SIGTERM to vtgate
func (vtgate *VtgateProcess) Terminate() error {
	if vtgate.proc == nil {
		return nil
	}
	return vtgate.proc.Process.Signal(syscall.SIGTERM)
}

// TearDown shuts down the running vtgate service
func (vtgate *VtgateProcess) TearDown() error {
	if vtgate.proc == nil || vtgate.exit == nil {
		return nil
	}
	// graceful shutdown is not currently working with vtgate, attempting a force-kill to make tests less flaky
	// Attempt graceful shutdown with SIGTERM first
	vtgate.proc.Process.Signal(syscall.SIGTERM)

	// We are not checking vtgate's exit code because it sometimes
	// returns exit code 2, even though vtgate terminates cleanly.
	select {
	case <-vtgate.exit:
		vtgate.proc = nil
		return nil

	case <-time.After(30 * time.Second):
		vtgate.proc.Process.Kill()
		err := <-vtgate.exit
		vtgate.proc = nil
		return err
	}
}

// VtgateProcessInstance returns a Vtgate handle for vtgate process
// configured with the given Config.
// The process must be manually started by calling setup()
func VtgateProcessInstance(
	port, grpcPort, mySQLServerPort int,
	cell, cellsToWatch, hostname, tabletTypesToWait string,
	topoPort int,
	tmpDirectory string,
	extraArgs []string,
	plannerVersion plancontext.PlannerVersion,
) *VtgateProcess {
	base := VtProcessInstance("vtgate", "vtgate", topoPort, hostname)
	vtgate := &VtgateProcess{
		VtProcess:             base,
		FileToLogQueries:      path.Join(tmpDirectory, "/vtgate_querylog.txt"),
		ConfigFile:            path.Join(tmpDirectory, fmt.Sprintf("vtgate-config-%d.json", port)),
		Directory:             os.Getenv("VTDATAROOT"),
		ServiceMap:            "grpc-tabletmanager,grpc-throttler,grpc-queryservice,grpc-updatestream,grpc-vtctl,grpc-vtgateservice",
		LogDir:                tmpDirectory,
		Port:                  port,
		GrpcPort:              grpcPort,
		MySQLServerPort:       mySQLServerPort,
		MySQLServerSocketPath: path.Join(tmpDirectory, "mysql.sock"),
		Cell:                  cell,
		CellsToWatch:          cellsToWatch,
		TabletTypesToWait:     tabletTypesToWait,
		MySQLAuthServerImpl:   "none",
		ExtraArgs:             extraArgs,
		PlannerVersion:        plannerVersion,
	}

	vtgate.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", hostname, port)
	vtgate.VSchemaURL = fmt.Sprintf("http://%s:%d/debug/vschema", hostname, port)
	vtgate.QueryPlanURL = fmt.Sprintf("http://%s:%d/debug/query_plans", hostname, port)

	return vtgate
}

// GetVars returns map of vars
func (vtgate *VtgateProcess) GetVars() map[string]any {
	resultMap := make(map[string]any)
	resp, err := http.Get(vtgate.VerifyURL)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		respByte, _ := io.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			return nil
		}
		return resultMap
	}
	return nil
}

// ReadVSchema reads the vschema from the vtgate endpoint for it and returns
// a pointer to the interface. To read this vschema, the caller must convert it to a map
func (vtgate *VtgateProcess) ReadVSchema() (*interface{}, error) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(vtgate.VSchemaURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results interface{}
	err = json.Unmarshal(res, &results)
	if err != nil {
		return nil, err
	}
	return &results, nil
}

// ReadQueryPlans reads the query plans from the vtgate endpoint for it and returns
// a pointer to the interface. To read this query plans, the caller must convert it to a map
func (vtgate *VtgateProcess) ReadQueryPlans() (map[string]any, error) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(vtgate.QueryPlanURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results any
	err = json.Unmarshal(res, &results)
	if err != nil {
		return nil, err
	}
	output, ok := results.(map[string]any)
	if !ok {
		return nil, errors.New("result is not a map")
	}
	return output, nil
}
