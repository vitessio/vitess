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

package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	// Register topo implementations.
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	tableName    = "chaos_t"
)

var cells = []string{"zone1", "zone2", "zone3"}

// Node is one cell of the reference deployment: one tablet (mysqld + vttablet) and one VTOrc,
// plus that cell's local topo server. Each lives in its own cgroup/network group.
type Node struct {
	Idx       int
	Cell      string
	Tablet    *cluster.Vttablet
	Orc       *cluster.VTOrcProcess
	Etcd      *cluster.TopoProcess
	Group     string // tablet group (mysqld + vttablet)
	OrcGroup  string
	EtcdGroup string
	db        *sql.DB // unix socket connection used by the observer/invariants
}

// Socket returns the path of the tablet's mysqld unix socket.
func (n *Node) Socket() string {
	return path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", n.Tablet.TabletUID), "mysql.sock")
}

// DataDir returns the tablet's mysql directory.
func (n *Node) DataDir() string {
	return path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", n.Tablet.TabletUID))
}

// Options configures a chaos cluster.
type Options struct {
	OrcExtraArgs    []string
	OrcConfig       cluster.VTOrcConfiguration
	TabletExtraArgs []string
}

// Chaos is a running reference deployment plus fault injection machinery.
type Chaos struct {
	t        *testing.T
	CI       *cluster.LocalProcessCluster
	Ts       *topo.Server
	Nodes    []*Node
	Net      *NetFault
	wrapDir  string
	realBins map[string]string
	Log      *EventLog
	vtgateDB *sql.DB

	stoppedMu sync.Mutex
}

// NewChaos builds the reference deployment:
// 1 keyspace, 1 shard, 3 tablets each in its own cell (zone1..3) with a separate etcd per cell
// (cell-local topo) plus a global etcd, durability policy cross_cell, 3 VTOrcs (one per cell)
// and a vtgate in zone1 watching all cells.
func NewChaos(t *testing.T, name string, opts Options) *Chaos {
	ok, cg := inHarnessCgroup()
	if !ok {
		t.Skipf("chaos harness must run inside cgroup /%s/%s (currently %q); use the chaos_run.sh wrapper", cgroupRelRoot, harnessGroup, cg)
	}
	c := &Chaos{t: t, realBins: map[string]string{}, Log: NewEventLog(name)}
	t.Cleanup(c.teardown)

	nf, err := NewNetFault(func(s string) { c.Log.Add("net", s) })
	require.NoError(t, err)
	c.Net = nf
	require.NoError(t, nf.Init())

	groups := []string{harnessGroup, "infra", "vtgate"}
	for i := range cells {
		groups = append(groups, fmt.Sprintf("tablet%d", i+1), fmt.Sprintf("orc%d", i+1), fmt.Sprintf("etcd%d", i+1))
	}
	for _, g := range groups {
		// Kill leftovers of an earlier crashed run.
		if g != harnessGroup {
			killGroup(g)
		}
		_, err := nf.AddGroup(g)
		require.NoError(t, err)
	}

	for _, b := range []string{"etcd", "vtctld", "vttablet", "mysqlctl", "vtorc", "vtgate"} {
		p, err := exec.LookPath(b)
		require.NoError(t, err)
		p, err = filepath.Abs(p)
		require.NoError(t, err)
		c.realBins[b] = p
	}
	c.wrapDir, err = os.MkdirTemp("", "chaos-wrap-")
	require.NoError(t, err)

	// Global etcd and vtctld run in the "infra" group: prepend a directory with wrappers to PATH
	// while StartTopo runs, since it looks those binaries up by name.
	infraDir := c.wrappers("infra", "etcd", "vtctld")
	oldPath := os.Getenv("PATH")
	os.Setenv("PATH", infraDir+":"+oldPath)
	ci := cluster.NewCluster(cells[0], "localhost")
	c.CI = ci
	err = ci.StartTopo()
	os.Setenv("PATH", oldPath)
	require.NoError(t, err)
	nf.SetPorts("infra", ci.TopoProcess.Port, portFromURL(ci.TopoProcess.PeerURL), ci.VtctldProcess.Port, ci.VtctldProcess.GrpcPort)

	// One etcd per cell as that cell's local topo.
	for i, cell := range cells {
		n := &Node{Idx: i, Cell: cell, Group: fmt.Sprintf("tablet%d", i+1), OrcGroup: fmt.Sprintf("orc%d", i+1), EtcdGroup: fmt.Sprintf("etcd%d", i+1)}
		c.Nodes = append(c.Nodes, n)
		port, peer := ci.GetAndReservePort(), ci.GetAndReservePort()
		etcd := cluster.TopoProcessInstance(port, peer, "localhost", "etcd2", cell)
		etcd.Binary = path.Join(c.wrappers(n.EtcdGroup, "etcd"), "etcd")
		require.NoError(t, etcd.SetupEtcd())
		n.Etcd = etcd
		nf.SetPorts(n.EtcdGroup, port, peer)
		addr := fmt.Sprintf("localhost:%d", port)
		cmd := "AddCellInfo"
		if cell == cells[0] {
			// StartTopo registered zone1 on the global etcd; point it at its own etcd instead.
			cmd = "UpdateCellInfo"
		}
		out, err := ci.VtctldClientProcess.ExecuteCommandWithOutput(cmd, "--root", "/vitess/"+cell, "--server-address", addr, cell)
		require.NoError(t, err, out)
	}

	ts, err := topo.OpenServer("etcd2", ci.VtctldClientProcess.TopoGlobalAddress, ci.VtctldClientProcess.TopoGlobalRoot)
	require.NoError(t, err)
	c.Ts = ts

	// Tablets.
	ci.VtTabletExtraArgs = append([]string{"--lock-tables-timeout", "5s"}, opts.TabletExtraArgs...)
	keyspace := &cluster.Keyspace{Name: keyspaceName, DurabilityPolicy: "cross_cell"}
	shard := cluster.Shard{Name: shardName}
	for i, n := range c.Nodes {
		tab := ci.NewVttabletInstance("replica", 100*(i+1), n.Cell)
		n.Tablet = tab
		shard.Vttablets = append(shard.Vttablets, tab)
	}
	require.NoError(t, ci.SetupCluster(keyspace, []cluster.Shard{shard}))
	for _, n := range c.Nodes {
		dir := c.wrappers(n.Group, "mysqlctl", "vttablet")
		n.Tablet.MysqlctlProcess.Binary = path.Join(dir, "mysqlctl")
		n.Tablet.VttabletProcess.Binary = path.Join(dir, "vttablet")
		nf.SetPorts(n.Group, n.Tablet.MySQLPort, n.Tablet.GrpcPort, n.Tablet.HTTPPort)
	}
	var procs []*exec.Cmd
	for _, n := range c.Nodes {
		p, err := n.Tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		procs = append(procs, p)
	}
	for _, p := range procs {
		require.NoError(t, p.Wait())
	}
	for _, n := range c.Nodes {
		n.Tablet.VttabletProcess.ServingStatus = ""
		require.NoError(t, n.Tablet.VttabletProcess.Setup())
	}
	for _, n := range c.Nodes {
		require.NoError(t, n.Tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"}))
		db, err := sql.Open("mysql", fmt.Sprintf("vt_dba@unix(%s)/?timeout=2s&readTimeout=2s&writeTimeout=2s&interpolateParams=true", n.Socket()))
		require.NoError(t, err)
		db.SetMaxOpenConns(4)
		db.SetConnMaxLifetime(time.Minute)
		n.db = db
	}

	// VTOrcs, one per cell.
	for _, n := range c.Nodes {
		orc := ci.NewVTOrcProcess(opts.OrcConfig, n.Cell)
		orc.Binary = path.Join(c.wrappers(n.OrcGroup, "vtorc"), "vtorc")
		orc.LogFileName = fmt.Sprintf("vtorc-%s-stderr.txt", n.Cell)
		orc.ExtraArgs = opts.OrcExtraArgs
		require.NoError(t, orc.Setup())
		ci.VTOrcProcesses = append(ci.VTOrcProcesses, orc)
		n.Orc = orc
		nf.SetPorts(n.OrcGroup, orc.Port)
	}

	// Wait for VTOrc to elect the initial primary.
	var primary *Node
	require.Eventually(t, func() bool {
		primary = c.topoPrimary()
		return primary != nil && primary.Tablet.VttabletProcess.GetTabletType() == "primary"
	}, 90*time.Second, time.Second, "no primary elected")

	// vtgate in zone1, watching all cells.
	vtg := ci.NewVtgateInstance()
	vtg.CellsToWatch = strings.Join(cells, ",")
	vtg.Binary = path.Join(c.wrappers("vtgate", "vtgate"), "vtgate")
	ci.VtgateProcess = *vtg
	require.NoError(t, ci.VtgateProcess.Setup())
	nf.SetPorts("vtgate", vtg.Port, vtg.GrpcPort, vtg.MySQLServerPort)

	c.vtgateDB, err = sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s?timeout=2s&readTimeout=5s&writeTimeout=5s&interpolateParams=true", vtg.MySQLServerPort, keyspaceName))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := c.vtgateDB.Exec(fmt.Sprintf("create table if not exists %s (id bigint not null primary key, worker int, src varchar(64), ts timestamp(6) default current_timestamp(6))", tableName))
		if err != nil {
			t.Logf("create table: %v", err)
		}
		return err == nil
	}, 60*time.Second, time.Second)

	c.WaitHealthy(90 * time.Second)
	c.Log.Add("setup", "cluster ready; primary="+primary.Tablet.Alias)
	return c
}

// wrappers creates, for the given cgroup leaf, shell wrappers that move themselves into the
// cgroup before exec'ing the real binary, so every socket of the process belongs to that group.
func (c *Chaos) wrappers(group string, bins ...string) string {
	dir := path.Join(c.wrapDir, group)
	require.NoError(c.t, os.MkdirAll(dir, 0o755))
	for _, b := range bins {
		script := fmt.Sprintf("#!/bin/sh\necho $$ > %s/%s/cgroup.procs || { echo 'chaos: cannot join cgroup %s' >&2; exit 97; }\nexec %s \"$@\"\n",
			cgroupRoot, group, group, c.realBins[b])
		require.NoError(c.t, os.WriteFile(path.Join(dir, b), []byte(script), 0o755))
	}
	return dir
}

func portFromURL(u string) int {
	i := strings.LastIndex(u, ":")
	var p int
	fmt.Sscanf(u[i+1:], "%d", &p)
	return p
}

// topoPrimary returns the node that is the shard primary in the global topo, or nil.
func (c *Chaos) topoPrimary() *Node {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	si, err := c.Ts.GetShard(ctx, keyspaceName, shardName)
	if err != nil || si.PrimaryAlias == nil {
		return nil
	}
	return c.nodeByAlias(si.PrimaryAlias)
}

func (c *Chaos) nodeByAlias(a *topodatapb.TabletAlias) *Node {
	s := topoproto.TabletAliasString(a)
	for _, n := range c.Nodes {
		if n.Tablet.Alias == s {
			return n
		}
	}
	return nil
}

func (c *Chaos) nodeByUUID(uuid string) *Node {
	for _, n := range c.Nodes {
		if n.serverUUID() == uuid {
			return n
		}
	}
	return nil
}

// teardown heals everything, resumes stopped processes, saves logs and stops the cluster.
func (c *Chaos) teardown() {
	if c.Net != nil {
		_ = c.Net.HealAll()
	}
	for _, n := range c.Nodes {
		signalGroup(n.Group, syscall.SIGCONT)
		signalGroup(n.OrcGroup, syscall.SIGCONT)
		signalGroup(n.EtcdGroup, syscall.SIGCONT)
	}
	if c.CI != nil {
		c.saveLogs()
		c.CI.Teardown()
	}
	for _, n := range c.Nodes {
		if n.Etcd != nil {
			_ = n.Etcd.TearDown(n.Cell, "", "", true, "etcd2")
		}
	}
	if c.Net != nil {
		for name := range c.Net.groups {
			if name != harnessGroup {
				killGroup(name)
			}
		}
		c.Net.Close()
	}
	if c.wrapDir != "" {
		os.RemoveAll(c.wrapDir)
	}
}

// resultsDir is where scenario reports and log copies go.
func resultsDir() string {
	if d := os.Getenv("CHAOS_RESULTS_DIR"); d != "" {
		return d
	}
	return path.Join(os.TempDir(), "chaos-results")
}

func (c *Chaos) saveLogs() {
	dst := path.Join(resultsDir(), c.Log.Name, "logs")
	_ = os.MkdirAll(dst, 0o755)
	_ = exec.Command("sh", "-c", fmt.Sprintf("cp -r %s/*.txt %s/ 2>/dev/null; true", c.CI.TmpDirectory, dst)).Run()
	for _, n := range c.Nodes {
		_ = exec.Command("sh", "-c", fmt.Sprintf("cp %s/error.log %s/mysql-%s-error.log 2>/dev/null; true", n.DataDir(), dst, n.Cell)).Run()
	}
	_ = os.WriteFile(path.Join(resultsDir(), c.Log.Name, "events.txt"), []byte(c.Log.String()), 0o644)
}
