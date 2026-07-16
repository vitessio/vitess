/*
Copyright 2026 The Vitess Authors.

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
	"context"
	_ "embed"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	// tabletHealthTimeout bounds how long a shard's tablets have to become
	// healthy and visible to vtgate.
	tabletHealthTimeout = 30 * time.Second

	// tabletHealthPollInterval is the interval at which vtgate's view of a
	// shard's tablets is polled.
	tabletHealthPollInterval = 300 * time.Millisecond

	// clusterOperationTimeout bounds cluster bring-up and teardown.
	clusterOperationTimeout = 10 * time.Minute
)

var (
	// If you query the sidecar database directly against mysqld then you will need to specify the
	// sidecarDBIdentifier
	sidecarDBName         = "__vt_e2e-test" // test a non-default sidecar database name that also needs to be escaped
	sidecarDBIdentifier   = sqlparser.String(sqlparser.NewIdentifierCS(sidecarDBName))
	mainClusterConfig     *ClusterConfig
	externalClusterConfig *ClusterConfig
	extraVTGateArgs       = []string{
		"--tablet-refresh-interval", "10ms", "--enable-buffer", "--buffer-window", loadTestBufferingWindowDuration.String(),
		"--buffer-size", "250000", "--buffer-min-time-between-failovers", "1s", "--buffer-max-failover-duration", loadTestBufferingWindowDuration.String(),
		"--buffer-drain-concurrency", "10",
	}
	extraVtctldArgs = []string{"--remote-operation-timeout", "600s", "--topo-etcd-lease-ttl", "120"}
	// This variable can be used within specific tests to alter vttablet behavior.
	extraVTTabletArgs = []string{}

	parallelInsertWorkers = "--vreplication-parallel-insert-workers=4"

	throttlerConfig = vitesst.ThrottlerConfig{Threshold: 15}

	// defaultHeartbeatOptions are the heartbeat flags every vttablet gets
	// unless the cluster overrides them: on demand (5s) with a 250ms interval.
	defaultHeartbeatOptions = []string{
		"--heartbeat-on-demand-duration", "5s",
		"--heartbeat-interval", "250ms",
	}

	// mariaDBImage is the Docker image MariaDB source keyspaces run.
	mariaDBImage = "vitesst:mariadb"

	// dbTypeVersionImages maps the DBTypeVersion keyspace option to the Docker
	// image a keyspace's tablets run.
	dbTypeVersionImages = map[string]string{
		"mysql-8.0":     vitesst.Image("8.0"),
		"mysql-8.4":     vitesst.Image("8.4"),
		"mariadb-10.10": mariaDBImage,
	}

	// mariaDBInitDBSQL initializes MariaDB tablets. MariaDB has no
	// super_read_only system variable, so it sets read_only instead. Everything
	// else matches the standard init_db.sql.
	//
	//go:embed testdata/config/init_testserver_db.sql
	mariaDBInitDBSQL string
)

type (
	// ClusterConfig holds the settings that distinguish one test cluster from
	// another.
	ClusterConfig struct {
		// namePrefix distinguishes a second cluster's containers from the main
		// cluster's when both share a Docker network.
		namePrefix string

		vreplicationCompressGTID bool
		// Set overrideHeartbeatOptions to true to override the default heartbeat options:
		// which are set to only on demand (5s) and 250ms interval.
		overrideHeartbeatOptions bool
	}

	// VitessCluster represents all components within the test cluster
	VitessCluster struct {
		t             *testing.T
		ClusterConfig *ClusterConfig
		Name          string
		CellNames     []string
		Cells         map[string]*Cell
		Cluster       *vitesst.Cluster
		Network       *testcontainers.DockerNetwork
		VtctldClient  *VtctldClient
		VTOrcProcess  *vitesst.VTOrc

		// ownsNetwork records whether this cluster created its Docker network
		// and so must remove it at teardown.
		ownsNetwork bool

		// The component arguments are read from the package globals when the
		// cluster is created, so a test that changes a global afterwards does
		// not reconfigure a live cluster, and so nothing leaks into the next
		// cluster.
		sidecarDBName string
		vtgateArgs    []string
		vttabletArgs  []string
		vtctldArgs    []string
		tabletFiles   []vitesst.ContainerFile
		tabletEnv     map[string]string

		cleanup func(context.Context) error
	}

	// Cell represents a Vitess cell within the test cluster
	Cell struct {
		Name      string
		Keyspaces map[string]*Keyspace
		Vtgates   []*vitesst.VTGate
	}

	// Keyspace represents a Vitess keyspace contained by a cell within the test cluster
	Keyspace struct {
		Name          string
		Shards        map[string]*Shard
		VSchema       string
		Schema        string
		SidecarDBName string

		// tablets hands out the tablet UID, cell and type of every tablet the
		// keyspace's shards start.
		tablets *tabletAllocator
	}

	// Shard represents a Vitess shard in a keyspace
	Shard struct {
		Name      string
		IsSharded bool
		Tablets   map[string]*Tablet
	}

	// Tablet represents a vttablet within a shard
	Tablet struct {
		Name     string
		Vttablet *vitesst.Tablet
	}

	// VtctldClient runs vtctldclient commands against the cluster's vtctld.
	VtctldClient struct {
		vc *VitessCluster
	}

	// tabletPlacement is one tablet of a shard: its UID, the cell it lives in,
	// and the type it starts as.
	tabletPlacement struct {
		uid  int
		cell string
		typ  string
	}

	// tabletAllocator pins the identity of every tablet a keyspace starts.
	// AddShards reprograms it before each batch of shards, because each batch
	// comes with its own tablet ID base and cell list.
	tabletAllocator struct {
		mu    sync.Mutex
		plans map[string][]tabletPlacement
		next  map[string]int
	}

	clusterOptions struct {
		cells         []string
		clusterConfig *ClusterConfig

		// network, when set, is joined instead of creating one, so that two
		// clusters can reach each other's components.
		network *testcontainers.DockerNetwork

		// tabletFiles and tabletEnv are placed into every tablet container,
		// e.g. an EXTRA_MY_CNF snippet and the variable naming it.
		tabletFiles []vitesst.ContainerFile
		tabletEnv   map[string]string
	}
)

func init() {
	mainClusterConfig = &ClusterConfig{}
	externalClusterConfig = &ClusterConfig{namePrefix: "ext-"}
}

// enableGTIDCompression enables GTID compression for the cluster and returns a function
// that can be used to disable it in a defer.
func (cc *ClusterConfig) enableGTIDCompression() func() {
	cc.vreplicationCompressGTID = true
	return func() {
		cc.vreplicationCompressGTID = false
	}
}

func getClusterOptions(opts *clusterOptions) *clusterOptions {
	if opts == nil {
		opts = &clusterOptions{}
	}
	if opts.cells == nil {
		opts.cells = []string{defaultCellName}
	}
	if opts.clusterConfig == nil {
		opts.clusterConfig = mainClusterConfig
	}
	return opts
}

// NewVitessCluster prepares a cluster with vtgate, vtctld, VTOrc and the topo.
// The containers come up with the first keyspace, which AddKeyspace creates.
func NewVitessCluster(t *testing.T, opts *clusterOptions) *VitessCluster {
	opts = getClusterOptions(opts)
	vc := &VitessCluster{
		t:             t,
		Name:          t.Name(),
		CellNames:     opts.cells,
		Cells:         make(map[string]*Cell),
		ClusterConfig: opts.clusterConfig,
		Network:       opts.network,
		sidecarDBName: sidecarDBName,
		vtgateArgs:    append([]string{}, extraVTGateArgs...),
		vtctldArgs:    append([]string{}, extraVtctldArgs...),
		tabletFiles:   opts.tabletFiles,
		tabletEnv:     opts.tabletEnv,
	}
	vc.VtctldClient = &VtctldClient{vc: vc}

	if !vc.ClusterConfig.overrideHeartbeatOptions {
		vc.vttabletArgs = append(vc.vttabletArgs, defaultHeartbeatOptions...)
	}
	vc.vttabletArgs = append(vc.vttabletArgs, extraVTTabletArgs...)
	if vc.ClusterConfig.vreplicationCompressGTID {
		vc.vttabletArgs = append(vc.vttabletArgs, "--vreplication-store-compressed-gtid=true")
	}

	if vc.Network == nil {
		nw, err := network.New(t.Context(), network.WithDriver("bridge"))
		require.NoError(t, err, "creating the cluster network")
		vc.Network = nw
		vc.ownsNetwork = true
	}

	for _, cellName := range opts.cells {
		cell, err := vc.AddCell(t, cellName)
		require.NoError(t, err)
		require.NotNil(t, cell)
	}

	return vc
}

// clusterOptions returns the vitesst options every component of the cluster is
// started with.
func (vc *VitessCluster) clusterOptions() []vitesst.ClusterOption {
	opts := []vitesst.ClusterOption{
		vitesst.WithCells(vc.CellNames...),
		vitesst.WithNetwork(vc.Network),
		vitesst.WithNamePrefix(vc.ClusterConfig.namePrefix),
		vitesst.WithVTCtldArgs(vc.vtctldArgs...),
		vitesst.WithVTTabletArgs(vc.vttabletArgs...),
		vitesst.WithVTGateArgs(vc.vtgateArgs...),
		// A vtgate belongs to a cell and watches the cells its keyspace lives
		// in, so AddKeyspace starts them itself.
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc(),
	}
	if len(vc.tabletFiles) > 0 {
		opts = append(opts, vitesst.WithTabletFiles(vc.tabletFiles...))
	}
	if len(vc.tabletEnv) > 0 {
		opts = append(opts, vitesst.WithTabletEnv(vc.tabletEnv))
	}
	return opts
}

// AddKeyspace creates a keyspace with specified shard keys and number of replica/read-only tablets.
// You can pass optional key value pairs (opts) if you want conditional behavior.
func (vc *VitessCluster) AddKeyspace(t *testing.T, cells []*Cell, ksName string, shards string, vschema string, schema string, numReplicas int, numRdonly int, tabletIDBase int, opts map[string]string) (*Keyspace, error) {
	keyspace := &Keyspace{
		Name:          ksName,
		Shards:        make(map[string]*Shard),
		SidecarDBName: vc.sidecarDBName,
		tablets:       &tabletAllocator{},
	}

	shardNames := strings.Split(shards, ",")
	cellNames := cellNames(cells)
	replicas, rdonly := keyspace.tablets.program(shardNames, cellNames, numReplicas, numRdonly, tabletIDBase)

	kb := vitesst.WithKeyspace(ksName).
		WithShardNames(shardNames...).
		WithReplicas(replicas).
		WithRDOnly(rdonly).
		WithSidecarDBName(keyspace.SidecarDBName).
		WithSchema(schema).
		WithVSchema(vschema).
		WithTabletSpec(keyspace.tablets.assign)
	if image := keyspaceImage(t, opts); image != "" {
		kb.WithImage(image)
		if image == mariaDBImage {
			kb.WithInitDBSQL(mariaDBInitDBSQL)
		}
	}

	for _, cell := range cells {
		cell.Keyspaces[ksName] = keyspace
	}

	log.Info("Adding keyspace " + ksName)
	if vc.Cluster == nil {
		c, err := vitesst.NewCluster(append(vc.clusterOptions(), kb)...)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), clusterOperationTimeout)
		defer cancel()
		cleanup, err := c.Start(ctx)
		vc.Cluster = c
		vc.cleanup = cleanup
		require.NoError(t, err)
		vc.VTOrcProcess = c.VTOrc()
	} else {
		vc.withRecoveriesDisabled(t, func() {
			ctx, cancel := context.WithTimeout(t.Context(), clusterOperationTimeout)
			defer cancel()
			_, err := vc.Cluster.AddKeyspace(ctx, kb)
			require.NoError(t, err)
		})
	}

	keyspace.Schema = schema
	keyspace.VSchema = vschema

	cellsToWatch := strings.Join(cellNames, ",")
	for _, cell := range cells {
		if len(cell.Vtgates) == 0 {
			log.Info("Starting vtgate")
			vc.StartVtgate(t, cell, cellsToWatch)
		}
	}

	vc.setupShards(t, cells, keyspace, shardNames, numReplicas, numRdonly)
	return keyspace, nil
}

// AddShards creates shards given list of comma-separated keys with specified tablets in each shard
func (vc *VitessCluster) AddShards(t *testing.T, cells []*Cell, keyspace *Keyspace, names string, numReplicas int, numRdonly int, tabletIDBase int, opts map[string]string) error {
	shardNames := strings.Split(names, ",")
	log.Info(fmt.Sprintf("Addshards got %d shards with %+v", len(shardNames), shardNames))
	replicas, rdonly := keyspace.tablets.program(shardNames, cellNames(cells), numReplicas, numRdonly, tabletIDBase)

	vc.withRecoveriesDisabled(t, func() {
		for _, shardName := range shardNames {
			if _, ok := keyspace.Shards[shardName]; ok {
				log.Info(fmt.Sprintf("Shard %s already exists, not adding", shardName))
				continue
			}

			log.Info("Adding Shard " + shardName)
			ctx, cancel := context.WithTimeout(t.Context(), clusterOperationTimeout)
			_, err := vc.Cluster.AddShard(ctx, keyspace.Name, shardName, replicas, rdonly)
			cancel()
			require.NoError(t, err)
			log.Info("Finished creating shard " + shardName)
		}
	})

	vc.setupShards(t, cells, keyspace, shardNames, numReplicas, numRdonly)
	return nil
}

// setupShards records the started shards and their tablets, brings the tablets
// to a known time zone, and waits until they are healthy, visible to vtgate and
// running the keyspace's throttler config.
func (vc *VitessCluster) setupShards(t *testing.T, cells []*Cell, keyspace *Keyspace, shardNames []string, numReplicas, numRdonly int) {
	ctx := t.Context()
	isSharded := len(shardNames) > 1

	for _, shardName := range shardNames {
		vitesstShard := vc.Cluster.Keyspace(keyspace.Name).Shard(shardName)
		require.NotNil(t, vitesstShard, "shard %s/%s was not started", keyspace.Name, shardName)

		shard, exists := keyspace.Shards[shardName]
		if !exists {
			shard = &Shard{Name: shardName, IsSharded: isSharded, Tablets: make(map[string]*Tablet, 1)}
			keyspace.Shards[shardName] = shard
		}

		for _, vttablet := range vitesstShard.Tablets() {
			tablet := &Tablet{Name: vttablet.Alias(), Vttablet: vttablet}
			shard.Tablets[tablet.Name] = tablet

			// Set time_zone to UTC for all tablets. Without this it fails locally on some MacOS setups.
			query := "SET GLOBAL time_zone = '+00:00';"
			qr, err := vttablet.QueryTablet(ctx, query)
			require.NoErrorf(t, err, "failed to set time_zone: %v, output: %v", err, qr)
		}
	}

	for _, shardName := range shardNames {
		require.NoError(t, vc.Cluster.WaitForHealthyShard(ctx, keyspace.Name, shardName, tabletHealthTimeout))
	}

	vtgate := cells[0].Vtgates[0]
	for _, shardName := range shardNames {
		target := fmt.Sprintf("%s.%s", keyspace.Name, shardName)
		require.NoError(t, waitForTabletsInShard(ctx, vtgate, target+".primary", 1))
		if replicas := len(cells) * numReplicas; replicas > 0 {
			require.NoError(t, waitForTabletsInShard(ctx, vtgate, target+".replica", replicas))
		}
		if numRdonly > 0 && hasCell(cells, defaultCellName) {
			require.NoError(t, waitForTabletsInShard(ctx, vtgate, target+".rdonly", numRdonly))
		}
	}

	require.NoError(t, vc.VtctldClient.ExecuteCommand("RebuildKeyspaceGraph", keyspace.Name))

	log.Info("Applying throttler config for keyspace " + keyspace.Name)
	req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true, Threshold: throttlerConfig.Threshold, CustomQuery: throttlerConfig.Query}
	res, err := vc.Cluster.Keyspace(keyspace.Name).Throttler().UpdateConfig(ctx, req, nil, nil)
	require.NoError(t, err, res)

	log.Info("Waiting for throttler config to be applied on all shards")
	for _, shardName := range shardNames {
		for _, tablet := range keyspace.Shards[shardName].Tablets {
			log.Info("+ Waiting for throttler config to be applied on " + tablet.Name)
			tablet.Vttablet.Throttler().WaitForStatusEnabled(ctx, t, true, nil, time.Minute)
		}
	}
	log.Info("Throttler config applied on all shards")
}

// withRecoveriesDisabled runs an action that changes the shards of the cluster
// with VTOrc recoveries disabled. We need this because we run ISP in the end.
// Running ISP after VTOrc has already run PRS causes issues.
func (vc *VitessCluster) withRecoveriesDisabled(t *testing.T, action func()) {
	if vc.VTOrcProcess == nil {
		action()
		return
	}

	ctx := t.Context()
	require.NoError(t, vc.VTOrcProcess.DisableGlobalRecoveries(ctx))
	defer func() {
		require.NoError(t, vc.VTOrcProcess.EnableGlobalRecoveries(ctx))
	}()
	action()
}

// keyspaceImage returns the Docker image the keyspace's tablets run, for a
// keyspace that pins its database type and version.
func keyspaceImage(t *testing.T, opts map[string]string) string {
	value, exists := opts["DBTypeVersion"]
	if !exists {
		return ""
	}

	image, supported := dbTypeVersionImages[value]
	require.Truef(t, supported, "unsupported DBTypeVersion %q, supported values are %v", value, mapKeys(dbTypeVersionImages))
	return image
}

func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// program lays out the tablets of every shard: the first cell hosts the
// primary, every cell hosts the replicas, and only the default cell hosts the
// rdonly tablets. It returns the number of replica and rdonly tablets per
// shard.
func (a *tabletAllocator) program(shardNames, cellNames []string, numReplicas, numRdonly, tabletIDBase int) (replicas, rdonly int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.plans = make(map[string][]tabletPlacement, len(shardNames))
	a.next = make(map[string]int, len(shardNames))

	for ind, shardName := range shardNames {
		plan := make([]tabletPlacement, 0, 1+len(cellNames)*numReplicas+numRdonly)
		for i, cell := range cellNames {
			if i == 0 {
				plan = append(plan, tabletPlacement{cell: cell, typ: "primary"})
			}
			for range numReplicas {
				plan = append(plan, tabletPlacement{cell: cell, typ: "replica"})
			}
			if cell == defaultCellName {
				for range numRdonly {
					plan = append(plan, tabletPlacement{cell: cell, typ: "rdonly"})
				}
			}
		}
		for i := range plan {
			plan[i].uid = tabletIDBase + ind*100 + i
		}
		a.plans[shardName] = plan
	}

	for _, placement := range a.plans[shardNames[0]] {
		switch placement.typ {
		case "replica":
			replicas++
		case "rdonly":
			rdonly++
		}
	}
	return replicas, rdonly
}

// assign gives one tablet of a shard the identity the current plan holds for it.
func (a *tabletAllocator) assign(spec *vitesst.TabletSpec) {
	a.mu.Lock()
	defer a.mu.Unlock()

	plan := a.plans[spec.Shard]
	index := a.next[spec.Shard]
	if index >= len(plan) {
		return
	}
	a.next[spec.Shard]++

	spec.UID = plan[index].uid
	spec.Cell = plan[index].cell
	spec.Type = plan[index].typ
}

// waitForTabletsInShard waits until vtgate's health check has the wanted number
// of tablets of a shard and tablet type, e.g. "product.0.replica".
func waitForTabletsInShard(ctx context.Context, vtgate *vitesst.VTGate, name string, count int) error {
	log.Info(fmt.Sprintf("Waiting for healthy status of %d %s tablets", count, name))
	ctx, cancel := context.WithTimeout(ctx, tabletHealthTimeout)
	defer cancel()

	for {
		vars, err := vtgate.GetVars(ctx)
		if err == nil {
			if connections, ok := vars["HealthcheckConnections"].(map[string]any); ok {
				if got, ok := connections[name].(float64); ok && int(got) == count {
					return nil
				}
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %d %s tablets failed", count, name)
		case <-time.After(tabletHealthPollInterval):
		}
	}
}

// cellNames returns the names of the given cells.
func cellNames(cells []*Cell) []string {
	names := make([]string, 0, len(cells))
	for _, cell := range cells {
		names = append(names, cell.Name)
	}
	return names
}

// hasCell reports whether the given cells include the named one.
func hasCell(cells []*Cell, name string) bool {
	for _, cell := range cells {
		if cell.Name == name {
			return true
		}
	}
	return false
}

// DeleteShard deletes a shard
func (vc *VitessCluster) DeleteShard(t testing.TB, cellName string, ksName string, shardName string) {
	keyspace := vc.Cells[cellName].Keyspaces[ksName]
	require.NotNil(t, keyspace)
	shard := keyspace.Shards[shardName]
	require.NotNil(t, shard)

	log.Info("Shutting down tablets of shard " + shardName)
	ctx, cancel := context.WithTimeout(context.Background(), clusterOperationTimeout)
	defer cancel()
	require.NoError(t, vc.Cluster.RemoveShard(ctx, ksName, shardName))
	delete(keyspace.Shards, shardName)

	log.Info("Deleting Shard " + shardName)
	// TODO how can we avoid the use of even_if_serving?
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("DeleteShard", "--recursive", "--even-if-serving", ksName+"/"+shardName)
	require.NoErrorf(t, err, "DeleteShard command failed with error %+v and output %s\n", err, output)
}

// StartVtgate starts a vtgate process
func (vc *VitessCluster) StartVtgate(t testing.TB, cell *Cell, cellsToWatch string) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterOperationTimeout)
	defer cancel()

	vtgate, err := vc.Cluster.AddVTGateSpec(ctx, vitesst.VTGateSpec{
		Cell:         cell.Name,
		CellsToWatch: strings.Split(cellsToWatch, ","),
		ExtraArgs:    vc.vtgateArgs,
	})
	require.NoError(t, err)
	cell.Vtgates = append(cell.Vtgates, vtgate)
}

// AddCell adds a new cell to the cluster
func (vc *VitessCluster) AddCell(t testing.TB, name string) (*Cell, error) {
	cell := &Cell{Name: name, Keyspaces: make(map[string]*Keyspace), Vtgates: make([]*vitesst.VTGate, 0)}
	vc.Cells[name] = cell
	return cell, nil
}

// TearDownKeyspace brings down a keyspace's tablets, leaving its topology
// records in place.
func (vc *VitessCluster) TearDownKeyspace(ks *Keyspace) error {
	ctx, cancel := context.WithTimeout(context.Background(), clusterOperationTimeout)
	defer cancel()

	if err := vc.Cluster.RemoveKeyspace(ctx, ks.Name); err != nil {
		return err
	}
	for name := range ks.Shards {
		delete(ks.Shards, name)
	}
	return nil
}

func (vc *VitessCluster) DeleteKeyspace(t testing.TB, ksName string) {
	out, err := vc.VtctldClient.ExecuteCommandWithOutput("DeleteKeyspace", ksName, "--recursive")
	if err != nil {
		log.Error(fmt.Sprintf("DeleteKeyspace failed with error: %v, output: %s", err, out))
	}
	require.NoError(t, err)
}

// TearDown brings down a cluster, deleting its containers and its network
func (vc *VitessCluster) TearDown() {
	ctx, cancel := context.WithTimeout(context.Background(), clusterOperationTimeout)
	defer cancel()

	if vc.Cluster != nil && vc.t.Failed() {
		vc.Cluster.DumpDiagnostics(ctx, vc.t.Logf)
	}
	if vc.cleanup != nil {
		if err := vc.cleanup(ctx); err != nil {
			log.Error("Error in cluster teardown - " + err.Error())
		} else {
			log.Info("TearDown() was successful")
		}
	}
	if vc.ownsNetwork {
		if err := vc.Network.Remove(ctx); err != nil {
			log.Error("Error removing the cluster network - " + err.Error())
		}
	}
}

func (vc *VitessCluster) getVttabletsInKeyspace(t *testing.T, cell *Cell, ksName string, tabletType string) map[string]*vitesst.Tablet {
	keyspace := cell.Keyspaces[ksName]
	tablets := make(map[string]*vitesst.Tablet)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Tablets {
			status, typ := tabletStatusAndType(t.Context(), tablet.Vttablet)
			if status == "SERVING" && (tabletType == "" || strings.EqualFold(typ, tabletType)) {
				log.Info(fmt.Sprintf("Serving status of tablet %s is %s, %s", tablet.Name, status, typ))
				tablets[tablet.Name] = tablet.Vttablet
			}
		}
	}
	return tablets
}

// tabletStatusAndType returns a tablet's serving status and its type, e.g.
// "SERVING" and "PRIMARY".
func tabletStatusAndType(ctx context.Context, tablet *vitesst.Tablet) (status string, typ string) {
	vars, err := tablet.GetVars(ctx)
	if err != nil {
		return "", ""
	}
	status, _ = vars["TabletStateName"].(string)
	typ, _ = vars["TabletType"].(string)
	return status, typ
}

func (vc *VitessCluster) getPrimaryTablet(t *testing.T, ksName, shardName string) *vitesst.Tablet {
	if keyspace := vc.Cluster.Keyspace(ksName); keyspace != nil {
		if shard := keyspace.Shard(shardName); shard != nil {
			if primary := shard.Primary(); primary != nil {
				return primary
			}
		}
	}
	require.FailNow(t, "no primary found", "keyspace %s, shard %s", ksName, shardName)
	return nil
}

// VTParams returns the connection parameters of the cluster's vtgate, with the
// given database selected.
func (vc *VitessCluster) VTParams(dbName string) mysql.ConnParams {
	return vc.Cluster.VTParams(context.Background(), dbName)
}

func (vc *VitessCluster) GetVTGateConn(t *testing.T) *mysql.Conn {
	return vc.Cluster.Connect(t)
}

func getVTGateConn() (*mysql.Conn, func()) {
	vtgateConn := vc.GetVTGateConn(vc.t)
	return vtgateConn, func() {
		vtgateConn.Close()
	}
}

func (vc *VitessCluster) startQuery(t *testing.T, query string) (func(t *testing.T), func(t *testing.T)) {
	conn := vc.GetVTGateConn(t)
	_, err := conn.ExecuteFetch("begin", 1000, false)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch(query, 1000, false)
	require.NoError(t, err)

	commit := func(t *testing.T) {
		_, err = conn.ExecuteFetch("commit", 1000, false)
		log.Info(fmt.Sprintf("startQuery:commit:err: %+v", err))
		conn.Close()
		log.Info("startQuery:after closing connection")
	}
	rollback := func(t *testing.T) {
		defer conn.Close()
		_, err = conn.ExecuteFetch("rollback", 1000, false)
		log.Info(fmt.Sprintf("startQuery:rollback:err: %+v", err))
	}
	return commit, rollback
}

// ExecuteCommand runs a vtctldclient command, discarding its output.
func (vtctldclient *VtctldClient) ExecuteCommand(args ...string) error {
	_, err := vtctldclient.ExecuteCommandWithOutput(args...)
	return err
}

// ExecuteCommandWithOutput runs a vtctldclient command and returns its output.
func (vtctldclient *VtctldClient) ExecuteCommandWithOutput(args ...string) (string, error) {
	return vtctldclient.vc.Cluster.Vtctld().ExecuteCommandWithOutput(context.Background(), args...)
}

// AddCellInfo registers a cell in the topology server, so that the cluster's
// components can place tablets and vtgates in it.
func (vtctldclient *VtctldClient) AddCellInfo(cell string) error {
	return vtctldclient.vc.Cluster.AddCell(context.Background(), cell)
}

// OnlineDDLShow returns the JSON status of an Online DDL migration.
func (vtctldclient *VtctldClient) OnlineDDLShow(keyspace, workflow string) (string, error) {
	return vtctldclient.ExecuteCommandWithOutput("OnlineDDL", "show", "--json", keyspace, workflow)
}
