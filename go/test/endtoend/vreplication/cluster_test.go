/*
Copyright 2022 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/log"
)

var (
	debugMode = false // set to true for local debugging: this uses the local env vtdataroot and does not teardown clusters

	originalVtdataroot string
	vtdataroot         string
	// If you query the sidecar database directly against mysqld then you will need to specify the
	// sidecarDBIdentifier
	sidecarDBName         = "__vt_e2e-test" // test a non-default sidecar database name that also needs to be escaped
	sidecarDBIdentifier   = sqlparser.String(sqlparser.NewIdentifierCS(sidecarDBName))
	mainClusterConfig     *ClusterConfig
	externalClusterConfig *ClusterConfig
	extraVTGateArgs       = []string{"--tablet_refresh_interval", "10ms", "--enable_buffer", "--buffer_window", loadTestBufferingWindowDurationStr,
		"--buffer_size", "100000", "--buffer_min_time_between_failovers", "0s", "--buffer_max_failover_duration", loadTestBufferingWindowDurationStr}
	extraVtctldArgs = []string{"--remote_operation_timeout", "600s", "--topo_etcd_lease_ttl", "120"}
	// This variable can be used within specific tests to alter vttablet behavior
	extraVTTabletArgs = []string{}

	parallelInsertWorkers = "--vreplication-parallel-insert-workers=4"

	throttlerConfig = throttler.Config{Threshold: 15}
)

// ClusterConfig defines the parameters like ports, tmpDir, tablet types which uniquely define a vitess cluster
type ClusterConfig struct {
	charset              string
	hostname             string
	topoPort             int
	vtctldPort           int
	vtctldGrpcPort       int
	vtdataroot           string
	tmpDir               string
	vtgatePort           int
	vtgateGrpcPort       int
	vtgateMySQLPort      int
	vtgatePlannerVersion plancontext.PlannerVersion
	tabletTypes          string
	tabletPortBase       int
	tabletGrpcPortBase   int
	tabletMysqlPortBase  int
	vtorcPort            int

	vreplicationCompressGTID bool
}

// enableGTIDCompression enables GTID compression for the cluster and returns a function
// that can be used to disable it in a defer.
func (cc *ClusterConfig) enableGTIDCompression() func() {
	cc.vreplicationCompressGTID = true
	return func() {
		cc.vreplicationCompressGTID = false
	}
}

// setAllVTTabletExperimentalFlags sets all the experimental flags for vttablet and returns a function
// that can be used to reset them in a defer.
func setAllVTTabletExperimentalFlags() func() {
	experimentalArgs := fmt.Sprintf("--vreplication_experimental_flags=%d",
		vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching)
	oldArgs := extraVTTabletArgs
	extraVTTabletArgs = append(extraVTTabletArgs, experimentalArgs)
	return func() {
		extraVTTabletArgs = oldArgs
	}
}

// VitessCluster represents all components within the test cluster
type VitessCluster struct {
	t             *testing.T
	ClusterConfig *ClusterConfig
	Name          string
	CellNames     []string
	Cells         map[string]*Cell
	Topo          *cluster.TopoProcess
	Vtctld        *cluster.VtctldProcess
	Vtctl         *cluster.VtctlProcess
	VtctlClient   *cluster.VtctlClientProcess
	VtctldClient  *cluster.VtctldClientProcess
	VTOrcProcess  *cluster.VTOrcProcess
}

// Cell represents a Vitess cell within the test cluster
type Cell struct {
	Name      string
	Keyspaces map[string]*Keyspace
	Vtgates   []*cluster.VtgateProcess
}

// Keyspace represents a Vitess keyspace contained by a cell within the test cluster
type Keyspace struct {
	Name          string
	Shards        map[string]*Shard
	VSchema       string
	Schema        string
	SidecarDBName string
}

// Shard represents a Vitess shard in a keyspace
type Shard struct {
	Name      string
	IsSharded bool
	Tablets   map[string]*Tablet
}

// Tablet represents a vttablet within a shard
type Tablet struct {
	Name     string
	Vttablet *cluster.VttabletProcess
	DbServer *cluster.MysqlctlProcess
}

func setTempVtDataRoot() string {
	dirSuffix := 100000 + rand.Intn(999999-100000) // 6 digits
	if debugMode {
		vtdataroot = originalVtdataroot
	} else {
		vtdataroot = path.Join(originalVtdataroot, fmt.Sprintf("vreple2e_%d", dirSuffix))
	}
	if _, err := os.Stat(vtdataroot); os.IsNotExist(err) {
		os.Mkdir(vtdataroot, 0700)
	}
	_ = os.Setenv("VTDATAROOT", vtdataroot)
	fmt.Printf("VTDATAROOT is %s\n", vtdataroot)
	return vtdataroot
}

// StartVTOrc starts a VTOrc instance
func (vc *VitessCluster) StartVTOrc() error {
	// Start vtorc if not already running
	if vc.VTOrcProcess != nil {
		return nil
	}
	base := cluster.VtctlProcessInstance(vc.ClusterConfig.topoPort, vc.ClusterConfig.hostname)
	base.Binary = "vtorc"
	vtorcProcess := &cluster.VTOrcProcess{
		VtctlProcess: *base,
		LogDir:       vc.ClusterConfig.tmpDir,
		Config:       cluster.VTOrcConfiguration{},
		Port:         vc.ClusterConfig.vtorcPort,
	}
	err := vtorcProcess.Setup()
	if err != nil {
		log.Error(err.Error())
		return err
	}
	vc.VTOrcProcess = vtorcProcess
	return nil
}

// setVtMySQLRoot creates the root directory if it does not exist
// and saves the directory in the VT_MYSQL_ROOT OS env var.
// mysqlctl will then look for the mysql related binaries in the
// ./bin, ./sbin, and ./libexec subdirectories of VT_MYSQL_ROOT.
func setVtMySQLRoot(mysqlRoot string) error {
	if _, err := os.Stat(mysqlRoot); os.IsNotExist(err) {
		os.Mkdir(mysqlRoot, 0700)
	}
	err := os.Setenv("VT_MYSQL_ROOT", mysqlRoot)
	if err != nil {
		return err
	}
	fmt.Printf("VT_MYSQL_ROOT is %s\n", mysqlRoot)
	return nil
}

func unsetVtMySQLRoot() {
	_ = os.Unsetenv("VT_MYSQL_ROOT")
}

// getDBTypeVersionInUse checks the major DB version of the mysqld binary
// that mysqlctl would currently use, e.g. 5.7 or 8.0 (in semantic versioning
// this would be major.minor but in MySQL it's effectively the major version).
func getDBTypeVersionInUse() (string, error) {
	var dbTypeMajorVersion string
	versionStr, err := mysqlctl.GetVersionString()
	if err != nil {
		return dbTypeMajorVersion, err
	}
	flavor, version, err := mysqlctl.ParseVersionString(versionStr)
	if err != nil {
		return dbTypeMajorVersion, err
	}
	majorVersion := fmt.Sprintf("%d.%d", version.Major, version.Minor)
	if flavor == mysqlctl.FlavorMySQL || flavor == mysqlctl.FlavorPercona {
		dbTypeMajorVersion = fmt.Sprintf("mysql-%s", majorVersion)
	} else {
		dbTypeMajorVersion = fmt.Sprintf("%s-%s", strings.ToLower(string(flavor)), majorVersion)
	}
	return dbTypeMajorVersion, nil
}

// downloadDBTypeVersion downloads a recent major version release build for the specified
// DB type.
// If the file already exists, it will not download it again.
// The artifact will be downloaded and extracted in the specified path. So e.g. if you
// pass /tmp as the path and 5.7 as the majorVersion, mysqld will be installed in:
// /tmp/mysql-5.7/bin/mysqld
// You should then call setVtMySQLRoot() and setDBFlavor() to ensure that this new
// binary is used by mysqlctl along with the correct flavor specific config file.
func downloadDBTypeVersion(dbType string, majorVersion string, path string) error {
	client := http.Client{
		Timeout: 10 * time.Minute,
	}
	var url, file, versionFile string
	dbType = strings.ToLower(dbType)

	// This currently only supports x86_64 linux
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return fmt.Errorf("downloadDBTypeVersion() only supports x86_64 linux, current test environment is %s %s", runtime.GOARCH, runtime.GOOS)
	}

	if dbType == "mysql" && majorVersion == "5.7" {
		versionFile = "mysql-5.7.37-linux-glibc2.12-x86_64.tar.gz"
		url = "https://dev.mysql.com/get/Downloads/MySQL-5.7/" + versionFile
	} else if dbType == "mysql" && majorVersion == "8.0" {
		versionFile = "mysql-8.0.28-linux-glibc2.17-x86_64-minimal.tar.xz"
		url = "https://dev.mysql.com/get/Downloads/MySQL-8.0/" + versionFile
	} else if dbType == "mariadb" && majorVersion == "10.10" {
		versionFile = "mariadb-10.10.3-linux-systemd-x86_64.tar.gz"
		url = "https://github.com/vitessio/vitess-resources/releases/download/v4.0/" + versionFile
	} else {
		return fmt.Errorf("invalid/unsupported major version: %s for database: %s", majorVersion, dbType)
	}
	file = fmt.Sprintf("%s/%s", path, versionFile)
	// Let's not download the file again if we already have it
	if _, err := os.Stat(file); err == nil {
		return nil
	}
	downloadFile := func() error {
		resp, err := client.Get(url)
		if err != nil {
			return fmt.Errorf("error downloading contents of %s to %s. Error: %v", url, file, err)
		}
		defer resp.Body.Close()
		out, err := os.Create(file)
		if err != nil {
			return fmt.Errorf("error creating file %s to save the contents of %s. Error: %v", file, url, err)
		}
		defer out.Close()
		_, err = io.Copy(out, resp.Body)
		if err != nil {
			return fmt.Errorf("error saving contents of %s to %s. Error: %v", url, file, err)
		}
		return nil
	}
	retries := 5
	var dlerr error
	for i := 0; i < retries; i++ {
		if dlerr = downloadFile(); dlerr == nil {
			break
		}
	}
	if dlerr != nil {
		return dlerr
	}

	untarCmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("tar xvf %s -C %s --strip-components=1", file, path))
	output, err := untarCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("exec: %v failed: %v output: %s", untarCmd, err, string(output))
	}

	return nil
}

func getClusterConfig(idx int, dataRootDir string) *ClusterConfig {
	basePort := 15000
	etcdPort := 2379

	basePort += idx * 10000
	etcdPort += idx * 10000
	if _, err := os.Stat(dataRootDir); os.IsNotExist(err) {
		os.Mkdir(dataRootDir, 0700)
	}

	return &ClusterConfig{
		// The ipv4 loopback address is used with the mysql client so that tcp is used in the test ("localhost" causes the socket file to be used, which fails)
		hostname:            "127.0.0.1",
		topoPort:            etcdPort,
		vtctldPort:          basePort,
		vtctldGrpcPort:      basePort + 999,
		tmpDir:              dataRootDir + "/tmp",
		vtgatePort:          basePort + 1,
		vtgateGrpcPort:      basePort + 991,
		vtgateMySQLPort:     basePort + 306,
		tabletTypes:         "primary",
		vtdataroot:          dataRootDir,
		tabletPortBase:      basePort + 1000,
		tabletGrpcPortBase:  basePort + 1991,
		tabletMysqlPortBase: basePort + 1306,
		vtorcPort:           basePort + 2639,
		charset:             "utf8mb4",
	}
}

func init() {
	// for local debugging set this variable so that each run uses VTDATAROOT instead of a random dir
	// and also does not teardown the cluster for inspecting logs and the databases
	if os.Getenv("VREPLICATION_E2E_DEBUG") != "" {
		debugMode = true
	}
	originalVtdataroot = os.Getenv("VTDATAROOT")
	var mainVtDataRoot string
	if debugMode {
		mainVtDataRoot = originalVtdataroot
	} else {
		mainVtDataRoot = setTempVtDataRoot()
	}
	mainClusterConfig = getClusterConfig(0, mainVtDataRoot)
	externalClusterConfig = getClusterConfig(1, mainVtDataRoot+"/ext")
}

type clusterOptions struct {
	cells         []string
	clusterConfig *ClusterConfig
}

func getClusterOptions(opts *clusterOptions) *clusterOptions {
	if opts == nil {
		opts = &clusterOptions{}
	}
	if opts.cells == nil {
		opts.cells = []string{"zone1"}
	}
	if opts.clusterConfig == nil {
		opts.clusterConfig = mainClusterConfig
	}
	return opts
}

// NewVitessCluster starts a basic cluster with vtgate, vtctld and the topo
func NewVitessCluster(t *testing.T, opts *clusterOptions) *VitessCluster {
	opts = getClusterOptions(opts)
	vc := &VitessCluster{t: t, Name: t.Name(), CellNames: opts.cells, Cells: make(map[string]*Cell), ClusterConfig: opts.clusterConfig}
	require.NotNil(t, vc)

	vc.CleanupDataroot(t, true)

	topo := cluster.TopoProcessInstance(vc.ClusterConfig.topoPort, vc.ClusterConfig.topoPort+1, vc.ClusterConfig.hostname, "etcd2", "global")

	require.NotNil(t, topo)
	require.Nil(t, topo.Setup("etcd2", nil))
	err := topo.ManageTopoDir("mkdir", "/vitess/global")
	require.NoError(t, err)
	vc.Topo = topo
	for _, cellName := range opts.cells {
		err := topo.ManageTopoDir("mkdir", "/vitess/"+cellName)
		require.NoError(t, err)
	}

	vc.setupVtctld()
	vc.setupVtctl()
	vc.setupVtctlClient()
	vc.setupVtctldClient()

	return vc
}

func (vc *VitessCluster) setupVtctld() {
	vc.Vtctld = cluster.VtctldProcessInstance(vc.ClusterConfig.vtctldPort, vc.ClusterConfig.vtctldGrpcPort,
		vc.ClusterConfig.topoPort, vc.ClusterConfig.hostname, vc.ClusterConfig.tmpDir)
	require.NotNil(vc.t, vc.Vtctld)
	// use first cell as `-cell`
	vc.Vtctld.Setup(vc.CellNames[0], extraVtctldArgs...)
}

func (vc *VitessCluster) setupVtctl() {
	vc.Vtctl = cluster.VtctlProcessInstance(vc.ClusterConfig.topoPort, vc.ClusterConfig.hostname)
	require.NotNil(vc.t, vc.Vtctl)
	for _, cellName := range vc.CellNames {
		vc.Vtctl.AddCellInfo(cellName)
		cell, err := vc.AddCell(vc.t, cellName)
		require.NoError(vc.t, err)
		require.NotNil(vc.t, cell)
	}
}

func (vc *VitessCluster) setupVtctlClient() {
	vc.VtctlClient = cluster.VtctlClientProcessInstance(vc.ClusterConfig.hostname, vc.Vtctld.GrpcPort, vc.ClusterConfig.tmpDir)
	require.NotNil(vc.t, vc.VtctlClient)
}

func (vc *VitessCluster) setupVtctldClient() {
	vc.VtctldClient = cluster.VtctldClientProcessInstance(vc.ClusterConfig.hostname, vc.Vtctld.GrpcPort, vc.ClusterConfig.tmpDir)
	require.NotNil(vc.t, vc.VtctldClient)
}

// CleanupDataroot deletes the vtdataroot directory. Since we run multiple tests sequentially in a single CI test shard,
// we can run out of disk space due to all the leftover artifacts from previous tests.
func (vc *VitessCluster) CleanupDataroot(t *testing.T, recreate bool) {
	// This is always set to "true" on GitHub Actions runners:
	// https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables
	ci, ok := os.LookupEnv("CI")
	if !ok || strings.ToLower(ci) != "true" {
		// Leave the directory in place to support local debugging.
		return
	}
	dir := vc.ClusterConfig.vtdataroot
	// The directory cleanup sometimes fails with a "directory not empty" error as
	// everything in the test is shutting down and cleaning up. So we retry a few
	// times to deal with that non-problematic and ephemeral issue.
	var err error
	retries := 3
	for i := 1; i <= retries; i++ {
		if err = os.RemoveAll(dir); err == nil {
			log.Infof("Deleted vtdataroot %q", dir)
			break
		}
		log.Errorf("Failed to delete vtdataroot (attempt %d of %d) %q: %v", i, retries, dir, err)
		time.Sleep(1 * time.Second)
	}
	require.NoError(t, err)
	if recreate {
		err = os.Mkdir(dir, 0700)
		require.NoError(t, err)
	}
}

// AddKeyspace creates a keyspace with specified shard keys and number of replica/read-only tablets.
// You can pass optional key value pairs (opts) if you want conditional behavior.
func (vc *VitessCluster) AddKeyspace(t *testing.T, cells []*Cell, ksName string, shards string, vschema string, schema string, numReplicas int, numRdonly int, tabletIDBase int, opts map[string]string) (*Keyspace, error) {
	keyspace := &Keyspace{
		Name:          ksName,
		Shards:        make(map[string]*Shard),
		SidecarDBName: sidecarDBName,
	}

	if err := vc.VtctldClient.CreateKeyspace(keyspace.Name, keyspace.SidecarDBName); err != nil {
		t.Fatalf(err.Error())
	}

	log.Infof("Applying throttler config for keyspace %s", keyspace.Name)
	res, err := throttler.UpdateThrottlerTopoConfigRaw(vc.VtctldClient, keyspace.Name, true, false, throttlerConfig.Threshold, throttlerConfig.Query, nil)
	require.NoError(t, err, res)

	cellsToWatch := ""
	for i, cell := range cells {
		if i > 0 {
			cellsToWatch = cellsToWatch + ","
		}
		cell.Keyspaces[ksName] = keyspace
		cellsToWatch = cellsToWatch + cell.Name
	}
	for _, cell := range cells {
		if len(cell.Vtgates) == 0 {
			log.Infof("Starting vtgate")
			vc.StartVtgate(t, cell, cellsToWatch)
		}
	}

	require.NoError(t, vc.AddShards(t, cells, keyspace, shards, numReplicas, numRdonly, tabletIDBase, opts))
	if schema != "" {
		if err := vc.VtctlClient.ApplySchema(ksName, schema); err != nil {
			t.Fatalf(err.Error())
		}
	}
	keyspace.Schema = schema
	if vschema != "" {
		if err := vc.VtctlClient.ApplyVSchema(ksName, vschema); err != nil {
			t.Fatalf(err.Error())
		}
	}
	keyspace.VSchema = vschema

	err = vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", ksName)
	require.NoError(t, err)
	return keyspace, nil
}

// AddTablet creates new tablet with specified attributes
func (vc *VitessCluster) AddTablet(t testing.TB, cell *Cell, keyspace *Keyspace, shard *Shard, tabletType string, tabletID int) (*Tablet, *exec.Cmd, error) {
	tablet := &Tablet{}

	options := []string{
		"--queryserver-config-schema-reload-time", "5s",
		"--heartbeat_on_demand_duration", "5s",
		"--heartbeat_interval", "250ms",
	} // FIXME: for multi-cell initial schema doesn't seem to load without "--queryserver-config-schema-reload-time"
	options = append(options, extraVTTabletArgs...)

	if mainClusterConfig.vreplicationCompressGTID {
		options = append(options, "--vreplication_store_compressed_gtid=true")
	}

	vttablet := cluster.VttabletProcessInstance(
		vc.ClusterConfig.tabletPortBase+tabletID,
		vc.ClusterConfig.tabletGrpcPortBase+tabletID,
		tabletID,
		cell.Name,
		shard.Name,
		keyspace.Name,
		vc.ClusterConfig.vtctldPort,
		tabletType,
		vc.Topo.Port,
		vc.ClusterConfig.hostname,
		vc.ClusterConfig.tmpDir,
		options,
		vc.ClusterConfig.charset)

	require.NotNil(t, vttablet)
	vttablet.SupportsBackup = false

	mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tabletID, vc.ClusterConfig.tabletMysqlPortBase+tabletID, vc.ClusterConfig.tmpDir)
	require.NoError(t, err)
	tablet.DbServer = mysqlctlProcess
	require.NotNil(t, tablet.DbServer)
	tablet.DbServer.InitMysql = true
	proc, err := tablet.DbServer.StartProcess()
	if err != nil {
		t.Fatal(err.Error())
	}
	require.NotNil(t, proc)
	tablet.Name = fmt.Sprintf("%s-%d", cell.Name, tabletID)
	vttablet.Name = tablet.Name
	tablet.Vttablet = vttablet
	shard.Tablets[tablet.Name] = tablet

	return tablet, proc, nil
}

// AddShards creates shards given list of comma-separated keys with specified tablets in each shard
func (vc *VitessCluster) AddShards(t *testing.T, cells []*Cell, keyspace *Keyspace, names string, numReplicas int, numRdonly int, tabletIDBase int, opts map[string]string) error {
	// Add a VTOrc instance if one is not already running
	if err := vc.StartVTOrc(); err != nil {
		return err
	}
	// Disable global recoveries until the shard has been added.
	// We need this because we run ISP in the end. Running ISP after VTOrc has already run PRS
	// causes issues.
	vc.VTOrcProcess.DisableGlobalRecoveries(t)
	defer vc.VTOrcProcess.EnableGlobalRecoveries(t)

	if value, exists := opts["DBTypeVersion"]; exists {
		if resetFunc := setupDBTypeVersion(t, value); resetFunc != nil {
			defer resetFunc()
		}
	}

	shardNames := strings.Split(names, ",")
	log.Infof("Addshards got %d shards with %+v", len(shardNames), shardNames)
	isSharded := len(shardNames) > 1
	primaryTabletUID := 0
	for ind, shardName := range shardNames {
		tabletID := tabletIDBase + ind*100
		tabletIndex := 0
		shard := &Shard{Name: shardName, IsSharded: isSharded, Tablets: make(map[string]*Tablet, 1)}
		if _, ok := keyspace.Shards[shardName]; ok {
			log.Infof("Shard %s already exists, not adding", shardName)
		} else {
			log.Infof("Adding Shard %s", shardName)
			if err := vc.VtctlClient.ExecuteCommand("CreateShard", keyspace.Name+"/"+shardName); err != nil {
				t.Fatalf("CreateShard command failed with %+v\n", err)
			}
			keyspace.Shards[shardName] = shard
		}
		for i, cell := range cells {
			dbProcesses := make([]*exec.Cmd, 0)
			tablets := make([]*Tablet, 0)
			if i == 0 {
				// only add primary tablet for first cell, so first time CreateShard is called
				log.Infof("Adding Primary tablet")
				primary, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica", tabletID+tabletIndex)
				require.NoError(t, err)
				require.NotNil(t, primary)
				tabletIndex++
				tablets = append(tablets, primary)
				dbProcesses = append(dbProcesses, proc)
				primaryTabletUID = primary.Vttablet.TabletUID
				primary.Vttablet.IsPrimary = true
			}

			for i := 0; i < numReplicas; i++ {
				log.Infof("Adding Replica tablet")
				tablet, proc, err := vc.AddTablet(t, cell, keyspace, shard, "replica", tabletID+tabletIndex)
				require.NoError(t, err)
				require.NotNil(t, tablet)
				tabletIndex++
				tablets = append(tablets, tablet)
				dbProcesses = append(dbProcesses, proc)
			}
			// Only create RDONLY tablets in the default cell
			if cell.Name == cluster.DefaultCell {
				for i := 0; i < numRdonly; i++ {
					log.Infof("Adding RdOnly tablet")
					tablet, proc, err := vc.AddTablet(t, cell, keyspace, shard, "rdonly", tabletID+tabletIndex)
					require.NoError(t, err)
					require.NotNil(t, tablet)
					tabletIndex++
					tablets = append(tablets, tablet)
					dbProcesses = append(dbProcesses, proc)
				}
			}

			for ind, proc := range dbProcesses {
				log.Infof("Waiting for mysql process for tablet %s", tablets[ind].Name)
				if err := proc.Wait(); err != nil {
					// Retry starting the database process before giving up.
					t.Logf("%v :: Unable to start mysql server for %v. Will cleanup files and processes, then retry...", err, tablets[ind].Vttablet)
					tablets[ind].DbServer.CleanupFiles(tablets[ind].Vttablet.TabletUID)
					// Kill any process we own that's listening on the port we
					// want to use as that is the most common problem.
					tablets[ind].DbServer.Stop()
					if _, err = exec.Command("fuser", "-n", "tcp", "-k", fmt.Sprintf("%d", tablets[ind].DbServer.MySQLPort)).Output(); err != nil {
						log.Errorf("Failed to kill process listening on port %d: %v", tablets[ind].DbServer.MySQLPort, err)
					}
					// Sleep for the kernel's TCP TIME_WAIT timeout to avoid the
					// port already in use error, which is the common cause for
					// the process not starting. It's a long wait, but it's worth
					// avoiding the test/workflow failure that otherwise occurs.
					time.Sleep(60 * time.Second)
					dbcmd, err := tablets[ind].DbServer.StartProcess()
					require.NoError(t, err)
					if err = dbcmd.Wait(); err != nil {
						// Get logs to help understand why it failed...
						vtdataroot := os.Getenv("VTDATAROOT")
						mysqlctlLog := path.Join(vtdataroot, "/tmp/mysqlctl.INFO")
						logBytes, ferr := os.ReadFile(mysqlctlLog)
						if ferr == nil {
							log.Errorf("mysqlctl log contents:\n%s", string(logBytes))
						} else {
							log.Errorf("Failed to read the mysqlctl log file %q: %v", mysqlctlLog, ferr)
						}
						mysqldLog := path.Join(vtdataroot, fmt.Sprintf("/vt_%010d/error.log", tablets[ind].Vttablet.TabletUID))
						logBytes, ferr = os.ReadFile(mysqldLog)
						if ferr == nil {
							log.Errorf("mysqld error log contents:\n%s", string(logBytes))
						} else {
							log.Errorf("Failed to read the mysqld error log file %q: %v", mysqldLog, ferr)
						}
						output, _ := dbcmd.CombinedOutput()
						t.Fatalf("%v :: Unable to start mysql server for %v; Output: %s", err,
							tablets[ind].Vttablet, string(output))
					}
				}
			}
			for ind, tablet := range tablets {
				log.Infof("Running Setup() for vttablet %s", tablets[ind].Name)
				if err := tablet.Vttablet.Setup(); err != nil {
					t.Fatalf(err.Error())
				}
				// Set time_zone to UTC for all tablets. Without this it fails locally on some MacOS setups.
				query := "SET GLOBAL time_zone = '+00:00';"
				qr, err := tablet.Vttablet.QueryTablet(query, tablet.Vttablet.Keyspace, false)
				if err != nil {
					t.Fatalf("failed to set time_zone: %v, output: %v", err, qr)
				}
			}
		}
		require.NotEqual(t, 0, primaryTabletUID, "Should have created a primary tablet")
		log.Infof("InitializeShard and make %d primary", primaryTabletUID)
		require.NoError(t, vc.VtctlClient.InitializeShard(keyspace.Name, shardName, cells[0].Name, primaryTabletUID))

		log.Infof("Finished creating shard %s", shard.Name)
	}
	for _, shard := range shardNames {
		require.NoError(t, cluster.WaitForHealthyShard(vc.VtctldClient, keyspace.Name, shard))
	}

	waitTimeout := 30 * time.Second
	vtgate := cells[0].Vtgates[0]
	for _, shardName := range shardNames {
		shard := keyspace.Shards[shardName]
		numReplicas, numRDOnly := 0, 0
		for _, tablet := range shard.Tablets {
			switch strings.ToLower(tablet.Vttablet.TabletType) {
			case "replica":
				numReplicas++
			case "rdonly":
				numRDOnly++
			}
		}
		numReplicas-- // account for primary, which also has replica type
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspace.Name, shard.Name), 1, waitTimeout); err != nil {
			return err
		}
		if numReplicas > 0 {
			if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspace.Name, shard.Name), numReplicas, waitTimeout); err != nil {
				return err
			}
		}
		if numRDOnly > 0 {
			if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspace.Name, shard.Name), numRDOnly, waitTimeout); err != nil {
				return err
			}
		}
	}

	err := vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", keyspace.Name)
	require.NoError(t, err)

	log.Infof("Waiting for throttler config to be applied on all shards")
	for _, shardName := range shardNames {
		shard := keyspace.Shards[shardName]
		for _, tablet := range shard.Tablets {
			clusterTablet := &cluster.Vttablet{
				Alias:    tablet.Name,
				HTTPPort: tablet.Vttablet.Port,
			}
			log.Infof("+ Waiting for throttler config to be applied on %s, type=%v", tablet.Name, tablet.Vttablet.TabletType)
			throttler.WaitForThrottlerStatusEnabled(t, clusterTablet, true, nil, time.Minute)
		}
	}
	log.Infof("Throttler config applied on all shards")

	return nil
}

// DeleteShard deletes a shard
func (vc *VitessCluster) DeleteShard(t testing.TB, cellName string, ksName string, shardName string) {
	shard := vc.Cells[cellName].Keyspaces[ksName].Shards[shardName]
	require.NotNil(t, shard)
	for _, tab := range shard.Tablets {
		log.Infof("Shutting down tablet %s", tab.Name)
		tab.Vttablet.TearDown()
	}
	log.Infof("Deleting Shard %s", shardName)
	// TODO how can we avoid the use of even_if_serving?
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("DeleteShard", "--", "--recursive", "--even_if_serving", ksName+"/"+shardName); err != nil {
		t.Fatalf("DeleteShard command failed with error %+v and output %s\n", err, output)
	}

}

// StartVtgate starts a vtgate process
func (vc *VitessCluster) StartVtgate(t testing.TB, cell *Cell, cellsToWatch string) {
	vtgate := cluster.VtgateProcessInstance(
		vc.ClusterConfig.vtgatePort,
		vc.ClusterConfig.vtgateGrpcPort,
		vc.ClusterConfig.vtgateMySQLPort,
		cell.Name,
		cellsToWatch,
		vc.ClusterConfig.hostname,
		vc.ClusterConfig.tabletTypes,
		vc.ClusterConfig.topoPort,
		vc.ClusterConfig.tmpDir,
		extraVTGateArgs,
		vc.ClusterConfig.vtgatePlannerVersion)
	require.NotNil(t, vtgate)
	if err := vtgate.Setup(); err != nil {
		t.Fatalf(err.Error())
	}
	cell.Vtgates = append(cell.Vtgates, vtgate)
}

// AddCell adds a new cell to the cluster
func (vc *VitessCluster) AddCell(t testing.TB, name string) (*Cell, error) {
	cell := &Cell{Name: name, Keyspaces: make(map[string]*Keyspace), Vtgates: make([]*cluster.VtgateProcess, 0)}
	vc.Cells[name] = cell
	return cell, nil
}

func (vc *VitessCluster) teardown() {
	for _, cell := range vc.Cells {
		for _, vtgate := range cell.Vtgates {
			if err := vtgate.TearDown(); err != nil {
				log.Errorf("Error in vtgate teardown - %s", err.Error())
			} else {
				log.Infof("vtgate teardown successful")
			}
		}
	}
	// collect unique keyspaces across cells
	keyspaces := make(map[string]*Keyspace)
	for _, cell := range vc.Cells {
		for _, keyspace := range cell.Keyspaces {
			keyspaces[keyspace.Name] = keyspace
		}
	}

	var wg sync.WaitGroup

	for _, keyspace := range keyspaces {
		for _, shard := range keyspace.Shards {
			for _, tablet := range shard.Tablets {
				wg.Add(1)
				go func(tablet2 *Tablet) {
					defer wg.Done()
					if tablet2.DbServer != nil && tablet2.DbServer.TabletUID > 0 {
						if err := tablet2.DbServer.Stop(); err != nil {
							log.Infof("Error stopping mysql process: %s", err.Error())
						}
					}
					if err := tablet2.Vttablet.TearDown(); err != nil {
						log.Infof("Error stopping vttablet %s %s", tablet2.Name, err.Error())
					} else {
						log.Infof("Successfully stopped vttablet %s", tablet2.Name)
					}
				}(tablet)
			}
		}
	}
	wg.Wait()
	if err := vc.Vtctld.TearDown(); err != nil {
		log.Infof("Error stopping Vtctld:  %s", err.Error())
	} else {
		log.Info("Successfully stopped vtctld")
	}

	for _, cell := range vc.Cells {
		if err := vc.Topo.TearDown(cell.Name, originalVtdataroot, vtdataroot, false, "etcd2"); err != nil {
			log.Infof("Error in etcd teardown - %s", err.Error())
		} else {
			log.Infof("Successfully tore down topo %s", vc.Topo.Name)
		}
	}

	if vc.VTOrcProcess != nil {
		if err := vc.VTOrcProcess.TearDown(); err != nil {
			log.Infof("Error stopping VTOrc: %s", err.Error())
		}
	}
}

// TearDown brings down a cluster, deleting processes, removing topo keys
func (vc *VitessCluster) TearDown() {
	if debugMode {
		return
	}
	done := make(chan bool)
	go func() {
		vc.teardown()
		done <- true
	}()
	select {
	case <-done:
		log.Infof("TearDown() was successful")
	case <-time.After(1 * time.Minute):
		log.Infof("TearDown() timed out")
	}
	// some processes seem to hang around for a bit
	time.Sleep(5 * time.Second)
	vc.CleanupDataroot(vc.t, false)
}

func (vc *VitessCluster) getVttabletsInKeyspace(t *testing.T, cell *Cell, ksName string, tabletType string) map[string]*cluster.VttabletProcess {
	keyspace := cell.Keyspaces[ksName]
	tablets := make(map[string]*cluster.VttabletProcess)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Tablets {
			if tablet.Vttablet.GetTabletStatus() == "SERVING" {
				log.Infof("Serving status of tablet %s is %s, %s", tablet.Name, tablet.Vttablet.ServingStatus, tablet.Vttablet.GetTabletStatus())
				tablets[tablet.Name] = tablet.Vttablet
			}
		}
	}
	return tablets
}

func (vc *VitessCluster) getPrimaryTablet(t *testing.T, ksName, shardName string) *cluster.VttabletProcess {
	for _, cell := range vc.Cells {
		keyspace := cell.Keyspaces[ksName]
		if keyspace == nil {
			continue
		}
		for _, shard := range keyspace.Shards {
			if shard.Name != shardName {
				continue
			}
			for _, tablet := range shard.Tablets {
				if tablet.Vttablet.IsPrimary {
					return tablet.Vttablet
				}
			}
		}
	}
	require.FailNow(t, "no primary found", "keyspace %s, shard %s", ksName, shardName)
	return nil
}

func (vc *VitessCluster) GetVTGateConn(t *testing.T) *mysql.Conn {
	return getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
}

func getVTGateConn() (*mysql.Conn, func()) {
	vtgateConn := vc.GetVTGateConn(vc.t)
	return vtgateConn, func() {
		vtgateConn.Close()
	}
}

func (vc *VitessCluster) startQuery(t *testing.T, query string) (func(t *testing.T), func(t *testing.T)) {
	conn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	_, err := conn.ExecuteFetch("begin", 1000, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch(query, 1000, false)
	require.NoError(t, err)

	commit := func(t *testing.T) {
		_, err = conn.ExecuteFetch("commit", 1000, false)
		log.Infof("startQuery:commit:err: %+v", err)
		conn.Close()
		log.Infof("startQuery:after closing connection")
	}
	rollback := func(t *testing.T) {
		defer conn.Close()
		_, err = conn.ExecuteFetch("rollback", 1000, false)
		log.Infof("startQuery:rollback:err: %+v", err)
	}
	return commit, rollback
}

// setupDBTypeVersion will perform any work needed to enable a specific
// database type and version if not already installed. It returns a
// function to reset any environment changes made.
func setupDBTypeVersion(t *testing.T, value string) func() {
	details := strings.Split(value, "-")
	if len(details) != 2 {
		t.Fatalf("Invalid database details: %s", value)
	}
	dbType := strings.ToLower(details[0])
	majorVersion := details[1]
	dbTypeMajorVersion := fmt.Sprintf("%s-%s", dbType, majorVersion)
	// Do nothing if this version is already installed
	dbVersionInUse, err := getDBTypeVersionInUse()
	if err != nil {
		t.Fatalf("Could not get details of database to be used for the keyspace: %v", err)
	}
	if dbTypeMajorVersion == dbVersionInUse {
		t.Logf("Requsted database version %s is already installed, doing nothing.", dbTypeMajorVersion)
		return func() {}
	}
	path := fmt.Sprintf("/tmp/%s", dbTypeMajorVersion)
	// Set the root path and create it if needed
	if err := setVtMySQLRoot(path); err != nil {
		t.Fatalf("Could not set VT_MYSQL_ROOT to %s, error: %v", path, err)
	}
	// Download and extract the version artifact if needed
	if err := downloadDBTypeVersion(dbType, majorVersion, path); err != nil {
		t.Fatalf("Could not download %s, error: %v", majorVersion, err)
	}
	return func() {
		unsetVtMySQLRoot()
	}
}
