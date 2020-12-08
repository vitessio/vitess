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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
)

// VttabletProcess is a generic handle for a running vttablet .
// It can be spawned manually
type VttabletProcess struct {
	Name                        string
	Binary                      string
	FileToLogQueries            string
	TabletUID                   int
	TabletPath                  string
	Cell                        string
	Port                        int
	GrpcPort                    int
	Shard                       string
	CommonArg                   VtctlProcess
	LogDir                      string
	TabletHostname              string
	Keyspace                    string
	TabletType                  string
	HealthCheckInterval         int
	BackupStorageImplementation string
	FileBackupStorageRoot       string
	ServiceMap                  string
	VtctldAddress               string
	Directory                   string
	VerifyURL                   string
	QueryzURL                   string
	StatusDetailsURL            string
	EnableSemiSync              bool
	SupportsBackup              bool
	ServingStatus               string
	DbPassword                  string
	DbPort                      int
	VreplicationTabletType      string
	//Extra Args to be set before starting the vttablet process
	ExtraArgs []string

	proc *exec.Cmd
	exit chan error
}

// Setup starts vttablet process with required arguements
func (vttablet *VttabletProcess) Setup() (err error) {

	vttablet.proc = exec.Command(
		vttablet.Binary,
		"-topo_implementation", vttablet.CommonArg.TopoImplementation,
		"-topo_global_server_address", vttablet.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vttablet.CommonArg.TopoGlobalRoot,
		"-log_queries_to_file", vttablet.FileToLogQueries,
		"-tablet-path", vttablet.TabletPath,
		"-port", fmt.Sprintf("%d", vttablet.Port),
		"-grpc_port", fmt.Sprintf("%d", vttablet.GrpcPort),
		"-init_shard", vttablet.Shard,
		"-log_dir", vttablet.LogDir,
		"-tablet_hostname", vttablet.TabletHostname,
		"-init_keyspace", vttablet.Keyspace,
		"-init_tablet_type", vttablet.TabletType,
		"-health_check_interval", fmt.Sprintf("%ds", vttablet.HealthCheckInterval),
		"-enable_replication_reporter",
		"-backup_storage_implementation", vttablet.BackupStorageImplementation,
		"-file_backup_storage_root", vttablet.FileBackupStorageRoot,
		"-service_map", vttablet.ServiceMap,
		"-vtctld_addr", vttablet.VtctldAddress,
		"-vtctld_addr", vttablet.VtctldAddress,
		"-vreplication_tablet_type", vttablet.VreplicationTabletType,
	)
	if *isCoverage {
		vttablet.proc.Args = append(vttablet.proc.Args, "-test.coverprofile="+getCoveragePath("vttablet.out"))
	}

	if vttablet.SupportsBackup {
		vttablet.proc.Args = append(vttablet.proc.Args, "-restore_from_backup")
	}
	if vttablet.EnableSemiSync {
		vttablet.proc.Args = append(vttablet.proc.Args, "-enable_semi_sync")
	}

	vttablet.proc.Args = append(vttablet.proc.Args, vttablet.ExtraArgs...)

	errFile, _ := os.Create(path.Join(vttablet.LogDir, vttablet.TabletPath+"-vttablet-stderr.txt"))
	vttablet.proc.Stderr = errFile

	vttablet.proc.Env = append(vttablet.proc.Env, os.Environ()...)

	log.Infof("Running vttablet with command: %v", strings.Join(vttablet.proc.Args, " "))

	err = vttablet.proc.Start()
	if err != nil {
		return
	}

	vttablet.exit = make(chan error)
	go func() {
		if vttablet.proc != nil {
			vttablet.exit <- vttablet.proc.Wait()
		}
	}()

	if vttablet.ServingStatus != "" {
		if err = vttablet.WaitForTabletType(vttablet.ServingStatus); err != nil {
			return fmt.Errorf("process '%s' timed out after 10s (err: %s)", vttablet.Name, err)
		}
	}
	return nil
}

// GetStatus returns /debug/status endpoint result
func (vttablet *VttabletProcess) GetStatus() string {
	URL := fmt.Sprintf("http://%s:%d/debug/status", vttablet.TabletHostname, vttablet.Port)
	resp, err := http.Get(URL)
	if err != nil {
		return ""
	}
	if resp.StatusCode == 200 {
		respByte, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return string(respByte)
	}
	return ""
}

// GetVars gets the debug vars as map
func (vttablet *VttabletProcess) GetVars() map[string]interface{} {
	resp, err := http.Get(vttablet.VerifyURL)
	if err != nil {
		return nil
	}
	if resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			return nil
		}
		return resultMap
	}
	return nil
}

// GetStatusDetails gets the status details
func (vttablet *VttabletProcess) GetStatusDetails() string {
	resp, err := http.Get(vttablet.StatusDetailsURL)
	if err != nil {
		return fmt.Sprintf("Status details failed: %v", err.Error())
	}
	respByte, _ := ioutil.ReadAll(resp.Body)
	return string(respByte)
}

// WaitForStatus waits till desired status of tablet is reached
func (vttablet *VttabletProcess) WaitForStatus(status string) bool {
	return vttablet.GetTabletStatus() == status
}

// GetTabletStatus returns the tablet state as seen in /debug/vars TabletStateName
func (vttablet *VttabletProcess) GetTabletStatus() string {
	resultMap := vttablet.GetVars()
	if resultMap != nil {
		return reflect.ValueOf(resultMap["TabletStateName"]).String()
	}
	return ""
}

// WaitForTabletType waits for 10 second till expected type reached
func (vttablet *VttabletProcess) WaitForTabletType(expectedType string) error {
	return vttablet.WaitForTabletTypesForTimeout([]string{expectedType}, 10*time.Second)
}

// WaitForTabletTypes waits for 10 second till expected type reached
func (vttablet *VttabletProcess) WaitForTabletTypes(expectedTypes []string) error {
	return vttablet.WaitForTabletTypesForTimeout(expectedTypes, 10*time.Second)
}

// WaitForTabletTypesForTimeout waits till the tablet reaches to any of the provided status
func (vttablet *VttabletProcess) WaitForTabletTypesForTimeout(expectedTypes []string, timeout time.Duration) error {
	timeToWait := time.Now().Add(timeout)
	var status string
	for time.Now().Before(timeToWait) {
		status = vttablet.GetTabletStatus()
		if contains(expectedTypes, status) {
			return nil
		}
		select {
		case err := <-vttablet.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vttablet.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}
	return fmt.Errorf("Vttablet %s, current status = %s, expected status [%s] not reached, details: %v",
		vttablet.TabletPath, status, strings.Join(expectedTypes, ","), vttablet.GetStatusDetails())
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

// WaitForBinLogPlayerCount waits till binlog player count var matches
func (vttablet *VttabletProcess) WaitForBinLogPlayerCount(expectedCount int) error {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		if vttablet.getVReplStreamCount() == fmt.Sprintf("%d", expectedCount) {
			return nil
		}
		select {
		case err := <-vttablet.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vttablet.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}
	return fmt.Errorf("vttablet %s, expected status not reached", vttablet.TabletPath)
}

// WaitForBinlogServerState wait for the tablet's binlog server to be in the provided state.
func (vttablet *VttabletProcess) WaitForBinlogServerState(expectedStatus string) error {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		if vttablet.getVarValue("UpdateStreamState") == expectedStatus {
			return nil
		}
		select {
		case err := <-vttablet.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vttablet.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}
	return fmt.Errorf("vttablet %s, expected status not reached", vttablet.TabletPath)
}

func (vttablet *VttabletProcess) getVReplStreamCount() string {
	return vttablet.getVarValue("VReplicationStreamCount")
}

func (vttablet *VttabletProcess) getVarValue(keyname string) string {
	resultMap := vttablet.GetVars()
	object := reflect.ValueOf(resultMap[keyname])
	return fmt.Sprintf("%v", object)
}

// TearDown shuts down the running vttablet service
func (vttablet *VttabletProcess) TearDown() error {
	if vttablet.proc == nil || vttablet.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	vttablet.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-vttablet.exit:
		vttablet.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		vttablet.proc.Process.Kill()
		vttablet.proc = nil
		return <-vttablet.exit
	}
}

// CreateDB creates the database for keyspace
func (vttablet *VttabletProcess) CreateDB(keyspace string) error {
	_, _ = vttablet.QueryTablet(fmt.Sprintf("drop database IF EXISTS vt_%s", keyspace), keyspace, false)
	_, err := vttablet.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace), keyspace, false)
	return err
}

// QueryTablet lets you execute a query in this tablet and get the result
func (vttablet *VttabletProcess) QueryTablet(query string, keyspace string, useDb bool) (*sqltypes.Result, error) {
	if !useDb {
		keyspace = ""
	}
	dbParams := NewConnParams(vttablet.DbPort, vttablet.DbPassword, path.Join(vttablet.Directory, "mysql.sock"), keyspace)
	return executeQuery(dbParams, query)
}

// QueryTabletWithDB lets you execute query on a specific DB in this tablet and get the result
func (vttablet *VttabletProcess) QueryTabletWithDB(query string, dbname string) (*sqltypes.Result, error) {
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.Directory, "mysql.sock"),
		DbName:     dbname,
	}
	if vttablet.DbPassword != "" {
		dbParams.Pass = vttablet.DbPassword
	}
	return executeQuery(dbParams, query)
}

func executeQuery(dbParams mysql.ConnParams, query string) (*sqltypes.Result, error) {
	ctx := context.Background()
	dbConn, err := mysql.Connect(ctx, &dbParams)
	if err != nil {
		return nil, err
	}
	defer dbConn.Close()
	qr, err := dbConn.ExecuteFetch(query, 10000, true)
	return qr, err
}

// GetDBVar returns first matching database variable's value
func (vttablet *VttabletProcess) GetDBVar(varName string, ksName string) (string, error) {
	return vttablet.getDBSystemValues("variables", varName, ksName)
}

// GetDBStatus returns first matching database variable's value
func (vttablet *VttabletProcess) GetDBStatus(status string, ksName string) (string, error) {
	return vttablet.getDBSystemValues("status", status, ksName)
}

func (vttablet *VttabletProcess) getDBSystemValues(placeholder string, value string, ksName string) (string, error) {
	output, err := vttablet.QueryTablet(fmt.Sprintf("show %s like '%s'", placeholder, value), ksName, true)
	if err != nil || output.Rows == nil {
		return "", err
	}
	if len(output.Rows) > 0 {
		return fmt.Sprintf("%s", output.Rows[0][1].ToBytes()), nil
	}
	return "", nil
}

// VttabletProcessInstance returns a VttabletProcess handle for vttablet process
// configured with the given Config.
// The process must be manually started by calling setup()
func VttabletProcessInstance(port int, grpcPort int, tabletUID int, cell string, shard string, keyspace string, vtctldPort int, tabletType string, topoPort int, hostname string, tmpDirectory string, extraArgs []string, enableSemiSync bool) *VttabletProcess {
	vtctl := VtctlProcessInstance(topoPort, hostname)
	vttablet := &VttabletProcess{
		Name:                        "vttablet",
		Binary:                      "vttablet",
		FileToLogQueries:            path.Join(tmpDirectory, fmt.Sprintf("/vt_%010d_querylog.txt", tabletUID)),
		Directory:                   path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tabletUID)),
		TabletPath:                  fmt.Sprintf("%s-%010d", cell, tabletUID),
		ServiceMap:                  "grpc-queryservice,grpc-tabletmanager,grpc-updatestream,grpc-throttler",
		LogDir:                      tmpDirectory,
		Shard:                       shard,
		TabletHostname:              hostname,
		Keyspace:                    keyspace,
		TabletType:                  "replica",
		CommonArg:                   *vtctl,
		HealthCheckInterval:         5,
		Port:                        port,
		GrpcPort:                    grpcPort,
		VtctldAddress:               fmt.Sprintf("http://%s:%d", hostname, vtctldPort),
		ExtraArgs:                   extraArgs,
		EnableSemiSync:              enableSemiSync,
		SupportsBackup:              true,
		ServingStatus:               "NOT_SERVING",
		BackupStorageImplementation: "file",
		FileBackupStorageRoot:       path.Join(os.Getenv("VTDATAROOT"), "/backups"),
		VreplicationTabletType:      "replica",
		TabletUID:                   tabletUID,
	}

	if tabletType == "rdonly" {
		vttablet.TabletType = tabletType
	}
	vttablet.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", hostname, port)
	vttablet.QueryzURL = fmt.Sprintf("http://%s:%d/queryz", hostname, port)
	vttablet.StatusDetailsURL = fmt.Sprintf("http://%s:%d/debug/status_details", hostname, port)

	return vttablet
}
