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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
)

const vttabletStateTimeout = 30 * time.Second

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
	DbFlavor                    string
	Charset                     string

	//Extra Args to be set before starting the vttablet process
	ExtraArgs []string

	proc *exec.Cmd
	exit chan error
}

// Setup starts vttablet process with required arguements
func (vttablet *VttabletProcess) Setup() (err error) {

	vttablet.proc = exec.Command(
		vttablet.Binary,
		"--topo_implementation", vttablet.CommonArg.TopoImplementation,
		"--topo_global_server_address", vttablet.CommonArg.TopoGlobalAddress,
		"--topo_global_root", vttablet.CommonArg.TopoGlobalRoot,
		"--log_queries_to_file", vttablet.FileToLogQueries,
		"--tablet-path", vttablet.TabletPath,
		"--port", fmt.Sprintf("%d", vttablet.Port),
		"--grpc_port", fmt.Sprintf("%d", vttablet.GrpcPort),
		"--init_shard", vttablet.Shard,
		"--log_dir", vttablet.LogDir,
		"--tablet_hostname", vttablet.TabletHostname,
		"--init_keyspace", vttablet.Keyspace,
		"--init_tablet_type", vttablet.TabletType,
		"--health_check_interval", fmt.Sprintf("%ds", vttablet.HealthCheckInterval),
		"--enable_replication_reporter",
		"--backup_storage_implementation", vttablet.BackupStorageImplementation,
		"--file_backup_storage_root", vttablet.FileBackupStorageRoot,
		"--service_map", vttablet.ServiceMap,
		"--vtctld_addr", vttablet.VtctldAddress,
		"--vtctld_addr", vttablet.VtctldAddress,
		"--vreplication_tablet_type", vttablet.VreplicationTabletType,
		"--db_charset", vttablet.Charset,
	)
	if *isCoverage {
		vttablet.proc.Args = append(vttablet.proc.Args, "--test.coverprofile="+getCoveragePath("vttablet.out"))
	}
	if *PerfTest {
		vttablet.proc.Args = append(vttablet.proc.Args, "--pprof", fmt.Sprintf("cpu,waitSig,path=vttablet_pprof_%s", vttablet.Name))
	}

	if vttablet.SupportsBackup {
		vttablet.proc.Args = append(vttablet.proc.Args, "--restore_from_backup")
	}
	if vttablet.EnableSemiSync {
		vttablet.proc.Args = append(vttablet.proc.Args, "--enable_semi_sync")
	}
	if vttablet.DbFlavor != "" {
		vttablet.proc.Args = append(vttablet.proc.Args, fmt.Sprintf("--db_flavor=%s", vttablet.DbFlavor))
	}

	vttablet.proc.Args = append(vttablet.proc.Args, vttablet.ExtraArgs...)
	fname := path.Join(vttablet.LogDir, vttablet.TabletPath+"-vttablet-stderr.txt")
	errFile, _ := os.Create(fname)
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
		if err = vttablet.WaitForTabletStatus(vttablet.ServingStatus); err != nil {
			errFileContent, _ := os.ReadFile(fname)
			if errFileContent != nil {
				log.Infof("vttablet error:\n%s\n", string(errFileContent))
			}
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
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		respByte, _ := io.ReadAll(resp.Body)
		return string(respByte)
	}
	return ""
}

// GetVars gets the debug vars as map
func (vttablet *VttabletProcess) GetVars() map[string]any {
	resp, err := http.Get(vttablet.VerifyURL)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		resultMap := make(map[string]any)
		respByte, _ := io.ReadAll(resp.Body)
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
	defer resp.Body.Close()

	respByte, _ := io.ReadAll(resp.Body)
	return string(respByte)
}

// WaitForStatus waits till desired status of tablet is reached
func (vttablet *VttabletProcess) WaitForStatus(status string, howLong time.Duration) bool {
	ticker := time.NewTicker(howLong)
	for range ticker.C {
		if vttablet.GetTabletStatus() == status {
			return true
		}
	}
	return false
}

// GetTabletStatus returns the tablet state as seen in /debug/vars TabletStateName
func (vttablet *VttabletProcess) GetTabletStatus() string {
	resultMap := vttablet.GetVars()
	if resultMap != nil {
		return reflect.ValueOf(resultMap["TabletStateName"]).String()
	}
	return ""
}

// GetTabletType returns the tablet type as seen in /debug/vars TabletType
func (vttablet *VttabletProcess) GetTabletType() string {
	resultMap := vttablet.GetVars()
	if resultMap != nil {
		return reflect.ValueOf(resultMap["TabletType"]).String()
	}
	return ""
}

// WaitForTabletStatus waits for one of the expected statuses to be reached
func (vttablet *VttabletProcess) WaitForTabletStatus(expectedStatus string) error {
	return vttablet.WaitForTabletStatusesForTimeout([]string{expectedStatus}, vttabletStateTimeout)
}

// WaitForTabletStatuses waits for one of expected statuses is reached
func (vttablet *VttabletProcess) WaitForTabletStatuses(expectedStatuses []string) error {
	return vttablet.WaitForTabletStatusesForTimeout(expectedStatuses, vttabletStateTimeout)
}

// WaitForTabletTypes waits for 10 second till one of expected statuses is reached
func (vttablet *VttabletProcess) WaitForTabletTypes(expectedTypes []string) error {
	return vttablet.WaitForTabletTypesForTimeout(expectedTypes, vttabletStateTimeout)
}

// WaitForTabletStatusesForTimeout waits till the tablet reaches to any of the provided statuses
func (vttablet *VttabletProcess) WaitForTabletStatusesForTimeout(expectedStatuses []string, timeout time.Duration) error {
	waitUntil := time.Now().Add(timeout)
	var status string
	for time.Now().Before(waitUntil) {
		status = vttablet.GetTabletStatus()
		if contains(expectedStatuses, status) {
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
		vttablet.TabletPath, status, strings.Join(expectedStatuses, ","), vttablet.GetStatusDetails())
}

// WaitForTabletTypesForTimeout waits till the tablet reaches to any of the provided types
func (vttablet *VttabletProcess) WaitForTabletTypesForTimeout(expectedTypes []string, timeout time.Duration) error {
	waitUntil := time.Now().Add(timeout)
	var tabletType string
	for time.Now().Before(waitUntil) {
		tabletType = vttablet.GetTabletType()
		if contains(expectedTypes, tabletType) {
			return nil
		}
		select {
		case err := <-vttablet.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vttablet.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}
	return fmt.Errorf("Vttablet %s, current type = %s, expected type [%s] not reached, status details: %v",
		vttablet.TabletPath, tabletType, strings.Join(expectedTypes, ","), vttablet.GetStatusDetails())
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
	timeout := time.Now().Add(vttabletStateTimeout)
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

// TearDown shuts down the running vttablet service and fails after the timeout
func (vttablet *VttabletProcess) TearDown() error {
	return vttablet.TearDownWithTimeout(vttabletStateTimeout)
}

// TearDownWithTimeout shuts down the running vttablet service and fails once the given
// duration has elapsed.
func (vttablet *VttabletProcess) TearDownWithTimeout(timeout time.Duration) error {
	if vttablet.proc == nil || vttablet.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	vttablet.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-vttablet.exit:
		vttablet.proc = nil
		return nil

	case <-time.After(timeout):
		proc := vttablet.proc
		if proc != nil {
			vttablet.proc.Process.Kill()
			vttablet.proc = nil
		}
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
	conn, err := vttablet.conn(&dbParams)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return executeQuery(conn, query)
}

func (vttablet *VttabletProcess) defaultConn(dbname string) (*mysql.Conn, error) {
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.Directory, "mysql.sock"),
		DbName:     dbname,
	}
	if vttablet.DbPassword != "" {
		dbParams.Pass = vttablet.DbPassword
	}
	return vttablet.conn(&dbParams)
}

func (vttablet *VttabletProcess) conn(dbParams *mysql.ConnParams) (*mysql.Conn, error) {
	ctx := context.Background()
	return mysql.Connect(ctx, dbParams)
}

// QueryTabletWithDB lets you execute query on a specific DB in this tablet and get the result
func (vttablet *VttabletProcess) QueryTabletWithDB(query string, dbname string) (*sqltypes.Result, error) {
	conn, err := vttablet.defaultConn(dbname)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return executeQuery(conn, query)
}

// executeQuery will retry the query up to 10 times with a small sleep in between each try.
// This allows the tests to be more robust in the face of transient failures.
func executeQuery(dbConn *mysql.Conn, query string) (*sqltypes.Result, error) {
	var (
		err    error
		result *sqltypes.Result
	)
	retries := 10
	retryDelay := 1 * time.Second
	for i := 0; i < retries; i++ {
		if i > 0 {
			// We only audit from 2nd attempt and onwards, otherwise this is just too verbose.
			log.Infof("Executing query %s (attempt %d of %d)", query, (i + 1), retries)
		}
		result, err = dbConn.ExecuteFetch(query, 10000, true)
		if err == nil {
			break
		}
		time.Sleep(retryDelay)
	}

	return result, err
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
		return output.Rows[0][1].ToString(), nil
	}
	return "", nil
}

// ToggleProfiling enables or disables the configured CPU profiler on this vttablet
func (vttablet *VttabletProcess) ToggleProfiling() error {
	return vttablet.proc.Process.Signal(syscall.SIGUSR1)
}

// WaitForVReplicationToCatchup waits for "workflow" to finish copying
func (vttablet *VttabletProcess) WaitForVReplicationToCatchup(t testing.TB, workflow, database string, duration time.Duration) {
	queries := [3]string{
		fmt.Sprintf(`select count(*) from _vt.vreplication where workflow = "%s" and db_name = "%s" and pos = ''`, workflow, database),
		"select count(*) from information_schema.tables where table_schema='_vt' and table_name='copy_state' limit 1;",
		fmt.Sprintf(`select count(*) from _vt.copy_state where vrepl_id in (select id from _vt.vreplication where workflow = "%s" and db_name = "%s" )`, workflow, database),
	}
	results := [3]string{"[INT64(0)]", "[INT64(1)]", "[INT64(0)]"}

	conn, err := vttablet.defaultConn("")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var lastChecked time.Time
	for ind, query := range queries {
		waitDuration := 500 * time.Millisecond
		for duration > 0 {
			log.Infof("Executing query %s on %s", query, vttablet.Name)
			lastChecked = time.Now()
			qr, err := executeQuery(conn, query)
			if err != nil {
				t.Fatal(err)
			}
			if qr != nil && qr.Rows != nil && len(qr.Rows) > 0 && fmt.Sprintf("%v", qr.Rows[0]) == string(results[ind]) {
				break
			} else {
				log.Infof("In WaitForVReplicationToCatchup: %s %+v", query, qr.Rows)
			}
			time.Sleep(waitDuration)
			duration -= waitDuration
		}
		if duration <= 0 {
			t.Fatalf("WaitForVReplicationToCatchup timed out for workflow %s, keyspace %s", workflow, database)
		}
	}
	log.Infof("WaitForVReplicationToCatchup succeeded at %v", lastChecked)
}

// BulkLoad performs a bulk load of rows into a given vttablet.
func (vttablet *VttabletProcess) BulkLoad(t testing.TB, db, table string, bulkInsert func(io.Writer)) {
	tmpbulk, err := os.CreateTemp(path.Join(vttablet.Directory, "tmp"), "bulk_load")
	if err != nil {
		t.Fatalf("failed to create tmp file for loading: %v", err)
	}
	defer os.Remove(tmpbulk.Name())

	log.Infof("create temporary file for bulk loading %q", tmpbulk.Name())
	bufStart := time.Now()

	bulkBuffer := bufio.NewWriter(tmpbulk)
	bulkInsert(bulkBuffer)
	bulkBuffer.Flush()

	pos, _ := tmpbulk.Seek(0, 1)
	bufFinish := time.Now()
	log.Infof("bulk loading %d bytes from %q...", pos, tmpbulk.Name())

	if err := tmpbulk.Close(); err != nil {
		t.Fatal(err)
	}

	conn, err := vttablet.defaultConn("vt_" + db)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	query := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE `%s` FIELDS TERMINATED BY ',' ENCLOSED BY '\"'", tmpbulk.Name(), table)
	_, err = executeQuery(conn, query)
	if err != nil {
		t.Fatal(err)
	}

	end := time.Now()
	log.Infof("bulk insert successful (write tmp file = %v, mysql bulk load = %v, total = %v",
		bufFinish.Sub(bufStart), end.Sub(bufFinish), end.Sub(bufStart))
}

// IsShutdown returns whether a vttablet is shutdown or not
func (vttablet *VttabletProcess) IsShutdown() bool {
	return vttablet.proc == nil
}

// VttabletProcessInstance returns a VttabletProcess handle for vttablet process
// configured with the given Config.
// The process must be manually started by calling setup()
func VttabletProcessInstance(port, grpcPort, tabletUID int, cell, shard, keyspace string, vtctldPort int, tabletType string, topoPort int, hostname, tmpDirectory string, extraArgs []string, enableSemiSync bool, charset string) *VttabletProcess {
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
		Charset:                     charset,
	}

	if tabletType == "rdonly" {
		vttablet.TabletType = tabletType
	}
	vttablet.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", hostname, port)
	vttablet.QueryzURL = fmt.Sprintf("http://%s:%d/queryz", hostname, port)
	vttablet.StatusDetailsURL = fmt.Sprintf("http://%s:%d/debug/status_details", hostname, port)

	return vttablet
}
