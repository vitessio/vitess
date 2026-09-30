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
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// All processes in the harness run on 127.0.0.1, so "nodes" cannot be told apart by IP.
// Instead every simulated node (a tablet's mysqld+vttablet, a VTOrc, a cell's etcd, vtgate, ...)
// runs in its own cgroup v2 leaf under cgroupRoot. The netfilter "cgroup" match identifies the
// cgroup of the socket that emits a packet; on the first packet of every TCP connection we stamp
// the conntrack entry with the id of the initiating group (CONNMARK), so that both directions
// of a connection carry the initiator's id. A partition rule "group A cannot reach group B" is
// then: drop every packet whose connmark is A and whose source or destination port is one of B's
// listening ports. This drops both directions of new and already-established connections.
//
// Only our own chains (chaosMarkChain, chaosDropChain) and the two jumps into them from OUTPUT
// are ever touched; unrelated iptables rules are left alone.

const (
	cgroupV2Mount  = "/sys/fs/cgroup/unified"
	cgroupRelRoot  = "chaos"
	chaosMarkChain = "CHAOS_MARK"
	chaosDropChain = "CHAOS_DROP"
	harnessGroup   = "harness"
)

var cgroupRoot = path.Join(cgroupV2Mount, cgroupRelRoot)

// Group is one simulated network endpoint (a cgroup leaf) with the TCP ports it listens on.
type Group struct {
	Name  string
	ID    int
	Ports []int
}

// NetFault manages iptables based partitions between Groups.
type NetFault struct {
	mu       sync.Mutex
	ipt      string
	groups   map[string]*Group
	nextID   int
	active   []string // human readable description of active rules
	inited   bool
	logRules func(string)
}

// NewNetFault returns a NetFault. Init must be called before any process is started so
// every connection gets marked.
func NewNetFault(logf func(string)) (*NetFault, error) {
	ipt := "/usr/sbin/iptables"
	if _, err := os.Stat(ipt); err != nil {
		p, err := exec.LookPath("iptables")
		if err != nil {
			return nil, fmt.Errorf("iptables not found: %w", err)
		}
		ipt = p
	}
	return &NetFault{ipt: ipt, groups: map[string]*Group{}, nextID: 1, logRules: logf}, nil
}

func (n *NetFault) run(args ...string) error {
	out, err := exec.Command(n.ipt, append([]string{"-w", "5"}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables %s: %v: %s", strings.Join(args, " "), err, string(out))
	}
	return nil
}

// Init creates our chains and hooks them into OUTPUT.
func (n *NetFault) Init() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Clean anything left over by an earlier (crashed) run of this harness.
	n.cleanupLocked()
	for _, c := range []string{chaosMarkChain, chaosDropChain} {
		if err := n.run("-N", c); err != nil {
			return err
		}
	}
	if err := n.run("-I", "OUTPUT", "1", "-j", chaosMarkChain); err != nil {
		return err
	}
	if err := n.run("-I", "OUTPUT", "2", "-j", chaosDropChain); err != nil {
		return err
	}
	n.inited = true
	return nil
}

// AddGroup registers a group and installs its connection marking rule. The cgroup must exist.
func (n *NetFault) AddGroup(name string) (*Group, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if g, ok := n.groups[name]; ok {
		return g, nil
	}
	if _, err := os.Stat(path.Join(cgroupRoot, name)); err != nil {
		return nil, fmt.Errorf("cgroup %s missing: %w", name, err)
	}
	g := &Group{Name: name, ID: n.nextID}
	n.nextID++
	if err := n.run("-A", chaosMarkChain, "-m", "conntrack", "--ctstate", "NEW", "-m", "cgroup", "--path", cgroupRelRoot+"/"+name,
		"-j", "CONNMARK", "--set-mark", strconv.Itoa(g.ID)); err != nil {
		return nil, err
	}
	n.groups[name] = g
	return g, nil
}

// SetPorts sets the listening ports of a group.
func (n *NetFault) SetPorts(name string, ports ...int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	g := n.groups[name]
	g.Ports = append(g.Ports, ports...)
}

// Block drops all traffic of connections initiated by `from` towards `to`'s listening ports
// (both directions of those connections).
func (n *NetFault) Block(from, to string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.blockLocked(from, to)
}

func (n *NetFault) blockLocked(from, to string) error {
	f, ok := n.groups[from]
	if !ok {
		return fmt.Errorf("unknown group %s", from)
	}
	t, ok := n.groups[to]
	if !ok {
		return fmt.Errorf("unknown group %s", to)
	}
	if len(t.Ports) == 0 {
		return nil
	}
	var ports []string
	for _, p := range t.Ports {
		ports = append(ports, strconv.Itoa(p))
	}
	if err := n.run("-A", chaosDropChain, "-p", "tcp", "-m", "connmark", "--mark", strconv.Itoa(f.ID),
		"-m", "multiport", "--ports", strings.Join(ports, ","), "-j", "DROP"); err != nil {
		return err
	}
	n.active = append(n.active, fmt.Sprintf("%s -> %s", from, to))
	return nil
}

// BlockPorts drops connections initiated by `from` to the given ports only.
func (n *NetFault) BlockPorts(from string, ports ...int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	f, ok := n.groups[from]
	if !ok {
		return fmt.Errorf("unknown group %s", from)
	}
	var ps []string
	for _, p := range ports {
		ps = append(ps, strconv.Itoa(p))
	}
	if err := n.run("-A", chaosDropChain, "-p", "tcp", "-m", "connmark", "--mark", strconv.Itoa(f.ID),
		"-m", "multiport", "--ports", strings.Join(ps, ","), "-j", "DROP"); err != nil {
		return err
	}
	n.active = append(n.active, fmt.Sprintf("%s -> ports %v", from, ports))
	return nil
}

// Partition blocks traffic in both directions between a and b.
func (n *NetFault) Partition(a, b string) error {
	if err := n.Block(a, b); err != nil {
		return err
	}
	return n.Block(b, a)
}

// Isolate cuts `name` off from every other group except the harness (the observer).
func (n *NetFault) Isolate(name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var others []string
	for o := range n.groups {
		if o != name && o != harnessGroup {
			others = append(others, o)
		}
	}
	sort.Strings(others)
	for _, o := range others {
		if err := n.blockLocked(name, o); err != nil {
			return err
		}
		if err := n.blockLocked(o, name); err != nil {
			return err
		}
	}
	return nil
}

// HealAll removes every partition rule.
func (n *NetFault) HealAll() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.active = nil
	if !n.inited {
		return nil
	}
	return n.run("-F", chaosDropChain)
}

// Active returns a description of the active partition rules.
func (n *NetFault) Active() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.active...)
}

// Close removes our chains.
func (n *NetFault) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cleanupLocked()
	n.inited = false
}

func (n *NetFault) cleanupLocked() {
	for _, c := range []string{chaosMarkChain, chaosDropChain} {
		for n.run("-D", "OUTPUT", "-j", c) == nil {
		}
		_ = n.run("-F", c)
		_ = n.run("-X", c)
	}
}

// ---- cgroup helpers ----

// inHarnessCgroup reports whether this process runs in <cgroupRoot>/harness, which is required
// so that it (as a non-root user) may move child processes into sibling cgroups.
func inHarnessCgroup() (bool, string) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return false, err.Error()
	}
	for l := range strings.SplitSeq(string(b), "\n") {
		if cg, ok := strings.CutPrefix(l, "0::"); ok {
			return cg == "/"+cgroupRelRoot+"/"+harnessGroup, cg
		}
	}
	return false, string(b)
}

// groupPids returns the pids currently in a cgroup leaf.
func groupPids(name string) []int {
	b, err := os.ReadFile(path.Join(cgroupRoot, name, "cgroup.procs"))
	if err != nil {
		return nil
	}
	var pids []int
	for f := range strings.FieldsSeq(string(b)) {
		if p, err := strconv.Atoi(f); err == nil {
			pids = append(pids, p)
		}
	}
	return pids
}

func procComm(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// signalGroup sends sig to the processes in group whose comm is in comms (all if comms is empty).
// It returns the pids signaled.
func signalGroup(name string, sig syscall.Signal, comms ...string) []int {
	var done []int
	for _, pid := range groupPids(name) {
		c := procComm(pid)
		if len(comms) > 0 {
			match := false
			for _, want := range comms {
				if c == want {
					match = true
				}
			}
			if !match {
				continue
			}
		}
		if err := syscall.Kill(pid, sig); err == nil {
			done = append(done, pid)
		}
	}
	return done
}

// killGroup kills every process in a cgroup leaf.
func killGroup(name string) {
	_ = os.WriteFile(path.Join(cgroupRoot, name, "cgroup.kill"), []byte("1"), 0)
	signalGroup(name, syscall.SIGKILL)
}
