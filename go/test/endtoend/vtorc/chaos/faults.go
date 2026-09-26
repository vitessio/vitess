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
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// KillMysqld kill -9's the node's mysqld. With noRestart, mysqld_safe is killed first so it does
// not restart mysqld (a hard crash that stays down until RestartMysqld).
func (c *Chaos) KillMysqld(n *Node, noRestart bool) {
	if noRestart {
		p := signalGroup(n.Group, syscall.SIGKILL, "mysqld_safe")
		c.Log.Add("fault", fmt.Sprintf("kill -9 mysqld_safe of %s pids=%v", n.Tablet.Alias, p))
	}
	p := signalGroup(n.Group, syscall.SIGKILL, "mysqld")
	c.Log.Add("fault", fmt.Sprintf("kill -9 mysqld of %s pids=%v", n.Tablet.Alias, p))
}

// RestartMysqld starts a crashed mysqld again (mysqlctl start).
func (c *Chaos) RestartMysqld(n *Node) error {
	c.Log.Add("heal", "restarting mysqld of "+n.Tablet.Alias)
	err := n.Tablet.MysqlctlProcess.StartProvideInit(false)
	c.Log.Add("heal", fmt.Sprintf("mysqld of %s restarted err=%v", n.Tablet.Alias, err))
	return err
}

// StopNode SIGSTOPs the node's mysqld and vttablet (a hung host).
func (c *Chaos) StopNode(n *Node) {
	p := signalGroup(n.Group, syscall.SIGSTOP, "mysqld", "vttablet")
	c.Log.Add("fault", fmt.Sprintf("kill -STOP mysqld+vttablet of %s pids=%v", n.Tablet.Alias, p))
}

// ResumeNode SIGCONTs the node's processes.
func (c *Chaos) ResumeNode(n *Node) {
	p := signalGroup(n.Group, syscall.SIGCONT)
	c.Log.Add("heal", fmt.Sprintf("kill -CONT %s pids=%v", n.Tablet.Alias, p))
}

// KillNode kill -9's everything of a tablet node (mysqld_safe, mysqld, vttablet).
func (c *Chaos) KillNode(n *Node) {
	p := signalGroup(n.Group, syscall.SIGKILL, "mysqld_safe")
	p = append(p, signalGroup(n.Group, syscall.SIGKILL)...)
	c.Log.Add("fault", fmt.Sprintf("kill -9 all processes of %s pids=%v", n.Tablet.Alias, p))
}

// RestartVttablet restarts a killed vttablet.
func (c *Chaos) RestartVttablet(n *Node) error {
	_ = n.Tablet.VttabletProcess.Kill()
	n.Tablet.VttabletProcess.ServingStatus = ""
	err := n.Tablet.VttabletProcess.Setup()
	c.Log.Add("heal", fmt.Sprintf("vttablet of %s restarted err=%v", n.Tablet.Alias, err))
	return err
}

// Isolate cuts a group off the network (except from the harness).
func (c *Chaos) Isolate(group string) {
	if err := c.Net.Isolate(group); err != nil {
		c.t.Fatal(err)
	}
	c.Log.Add("fault", "network-isolate "+group)
}

// Block drops connections initiated by `from` to `to`.
func (c *Chaos) Block(from, to string) {
	if err := c.Net.Block(from, to); err != nil {
		c.t.Fatal(err)
	}
	c.Log.Add("fault", fmt.Sprintf("block %s -> %s", from, to))
}

// Partition blocks both directions between a and b.
func (c *Chaos) Partition(a, b string) {
	if err := c.Net.Partition(a, b); err != nil {
		c.t.Fatal(err)
	}
	c.Log.Add("fault", fmt.Sprintf("partition %s <-> %s", a, b))
}

// Heal removes every network fault.
func (c *Chaos) Heal() {
	if err := c.Net.HealAll(); err != nil {
		c.t.Fatal(err)
	}
	c.Log.Add("heal", "network healed")
}

// WaitFor polls cond until it's true or the timeout expires, returning the elapsed time.
func (c *Chaos) WaitFor(what string, timeout time.Duration, cond func() bool) (time.Duration, bool) {
	start := time.Now()
	for time.Since(start) < timeout {
		if cond() {
			c.Log.Add("wait", fmt.Sprintf("%s after %.2fs", what, time.Since(start).Seconds()))
			return time.Since(start), true
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.Log.Add("wait", fmt.Sprintf("TIMEOUT waiting for %s (%v)", what, timeout))
	return time.Since(start), false
}

// PrimaryChanged returns a condition that is true once the topo shard primary is not `old`.
func (c *Chaos) PrimaryChanged(old *Node) func() bool {
	return func() bool {
		p := c.topoPrimary()
		return p != nil && p != old
	}
}

// GrepLogs returns matching lines from the cluster's log dir (for reports).
func (c *Chaos) GrepLogs(pattern string, max int) string {
	out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s/vtorc-*-stderr.txt 2>/dev/null | head -n %d", pattern, c.CI.TmpDirectory, max)).CombinedOutput()
	return string(out)
}

// VTOrcSummary summarizes the recoveries each VTOrc ran (from its log) for the report.
func (c *Chaos) VTOrcSummary() []string {
	var res []string
	for _, n := range c.Nodes {
		f := fmt.Sprintf("%s/%s", c.CI.TmpDirectory, n.Orc.LogFileName)
		script := fmt.Sprintf(`grep -oE 'Unlocking shard ks/0 for (successful )?action VTOrc Recovery for [A-Za-z]+ on [a-z0-9-]+( with error [^:]*)?' %s | sed -E 's/Unlocking shard ks\/0 for //' | sort | uniq -c | sort -rn | head -12`, f)
		out, _ := exec.Command("sh", "-c", script).CombinedOutput()
		for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
			if l != "" {
				res = append(res, fmt.Sprintf("vtorc-%s: %s", n.Cell, strings.TrimSpace(l)))
			}
		}
		script = fmt.Sprintf(`grep -E ' (WRN|ERR) ' %s | grep -E 'Recovery for (DeadPrimary|PrimarySemiSyncBlocked|PrimaryTabletUnreachableByQuorum|IncapacitatedPrimary|DeadPrimaryAndSomeReplicas)|EmergencyReparent|could not reach' | cut -c1-400 | head -8`, f)
		out, _ = exec.Command("sh", "-c", script).CombinedOutput()
		for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
			if l != "" {
				res = append(res, fmt.Sprintf("vtorc-%s ERS-err: %s", n.Cell, l))
			}
		}
	}
	return res
}

// RestartEtcd restarts a cell's etcd (after it was killed).
func (c *Chaos) RestartEtcd(n *Node) error {
	err := n.Etcd.SetupEtcd()
	c.Log.Add("heal", fmt.Sprintf("etcd of %s restarted err=%v", n.Cell, err))
	return err
}

// RestartOrc restarts a cell's VTOrc (after it was killed).
func (c *Chaos) RestartOrc(n *Node) error {
	err := n.Orc.Setup()
	c.Log.Add("heal", fmt.Sprintf("vtorc of %s restarted err=%v", n.Cell, err))
	return err
}

// KillCell kill -9's everything of a cell: tablet (mysqld_safe, mysqld, vttablet), VTOrc, etcd.
func (c *Chaos) KillCell(n *Node) {
	c.KillNode(n)
	killGroup(n.OrcGroup)
	killGroup(n.EtcdGroup)
	c.Log.Add("fault", fmt.Sprintf("kill -9 whole cell %s (tablet, vtorc, cell etcd)", n.Cell))
}

// WaitForOrcLog waits until some VTOrc log contains the regexp and returns that node.
func (c *Chaos) WaitForOrcLog(pattern string, since time.Time, timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			f := fmt.Sprintf("%s/%s", c.CI.TmpDirectory, n.Orc.LogFileName)
			out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -E %q %s | tail -1", pattern, f)).Output()
			l := strings.TrimSpace(string(out))
			if l == "" || len(l) < 23 {
				continue
			}
			ts, err := time.Parse("2006-01-02 15:04:05.000", l[:23])
			if err == nil && ts.After(since.UTC().Add(-50*time.Millisecond)) {
				c.Log.Add("orclog", fmt.Sprintf("vtorc-%s: %s", n.Cell, l[:min(len(l), 250)]))
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// KeepOnlyOrc stops every VTOrc except the one in n's cell (single-VTOrc deployments).
func (c *Chaos) KeepOnlyOrc(n *Node) {
	for _, m := range c.Nodes {
		if m != n {
			_ = m.Orc.TearDown()
		}
	}
	c.Log.Add("setup", "only vtorc-"+n.Cell+" is running")
}
