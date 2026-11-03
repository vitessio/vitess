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
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// soakFault is one fault of the soak test: it injects the fault on a node, waits, and heals it.
type soakFault struct {
	name string
	run  func(s *Scenario, n *Node, rng *rand.Rand)
}

// between returns a random duration in [lo, hi].
func between(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(rng.Int64N(int64(hi-lo)+1))
}

// isolateCell cuts a node's whole cell (tablet, VTOrc, etcd) off from every other group except the
// harness: inside the cell they still reach each other.
func (s *Scenario) isolateCell(n *Node) {
	cell := []string{n.Group, n.OrcGroup, n.EtcdGroup}
	var others []string
	for name := range s.Net.groups {
		if name != harnessGroup && !slices.Contains(cell, name) {
			others = append(others, name)
		}
	}
	sort.Strings(others)
	for _, in := range cell {
		for _, name := range others {
			s.Partition(in, name)
		}
	}
	s.Log.Add("fault", "cell "+n.Cell+" isolated")
}

// soakFaults are drawn from the primitives of the other scenarios.
var soakFaults = []soakFault{
	{"kill-mysqld", func(s *Scenario, n *Node, rng *rand.Rand) {
		s.KillMysqld(n, true)
		s.Sleep(between(rng, 10*time.Second, 30*time.Second), "soak: mysqld down")
		_ = s.RestartMysqld(n)
	}},
	{"crash-mysqld", func(s *Scenario, n *Node, rng *rand.Rand) {
		s.KillMysqld(n, false) // mysqld_safe restarts it
		s.Sleep(15*time.Second, "soak: mysqld restarts")
	}},
	{"pause", func(s *Scenario, n *Node, rng *rand.Rand) {
		s.StopNode(n)
		s.Sleep(between(rng, 5*time.Second, 25*time.Second), "soak: mysqld and vttablet paused")
		s.ResumeNode(n)
	}},
	{"isolate-tablet", func(s *Scenario, n *Node, rng *rand.Rand) {
		s.Isolate(n.Group)
		s.Sleep(between(rng, 5*time.Second, 25*time.Second), "soak: tablet isolated")
		s.Heal()
	}},
	{"isolate-cell", func(s *Scenario, n *Node, rng *rand.Rand) {
		s.isolateCell(n)
		s.Sleep(between(rng, 10*time.Second, 25*time.Second), "soak: cell isolated")
		s.Heal()
	}},
	{"kill-vttablet", func(s *Scenario, n *Node, rng *rand.Rand) {
		p := signalGroup(n.Group, syscall.SIGKILL, "vttablet")
		s.Log.Add("fault", fmt.Sprintf("kill -9 vttablet of %s pids=%v", n.Tablet.Alias, p))
		s.Sleep(between(rng, 5*time.Second, 15*time.Second), "soak: vttablet down")
		_ = s.RestartVttablet(n)
	}},
	{"kill-vtorc", func(s *Scenario, n *Node, rng *rand.Rand) {
		killGroup(n.OrcGroup)
		s.Log.Add("fault", "kill -9 vtorc of "+n.Cell)
		s.Sleep(between(rng, 10*time.Second, 30*time.Second), "soak: vtorc down")
		_ = s.RestartOrc(n)
	}},
	{"flap", func(s *Scenario, n *Node, rng *rand.Rand) {
		for range 3 {
			s.Isolate(n.Group)
			time.Sleep(5 * time.Second)
			s.Heal()
			time.Sleep(4 * time.Second)
		}
	}},
}

// Soak: one cluster, for CHAOS_SOAK_DURATION (default 2h), a loop of faults drawn at random from
// soakFaults (CHAOS_SOAK_SEED fixes the draw), each on the primary or on a random other tablet, with
// the writers, the reader and the observer running throughout. After each fault the cluster must
// converge within 5 minutes; at the end, the usual checks: no acknowledged write lost, no
// violation, converged.
func TestSoakMixedFaults(t *testing.T) {
	duration := 2 * time.Hour
	if v := os.Getenv("CHAOS_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			t.Fatalf("CHAOS_SOAK_DURATION=%q: want a positive duration", v)
		}
		duration = d
	}
	seed := uint64(time.Now().UnixNano())
	if v := os.Getenv("CHAOS_SOAK_SEED"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("CHAOS_SOAK_SEED=%q: %v", v, err)
		}
		seed = n
	}
	runScenario(t, "SOAK-mixed-faults", Options{}, func(s *Scenario) {
		rng := rand.New(rand.NewPCG(seed, seed))
		s.R.outcome("soak: duration %v, seed %d", duration, seed)
		s.MarkFault()
		end := time.Now().Add(duration)
		counts := map[string]int{}
		failovers := 0
		var slowest time.Duration
		i := 0
		for time.Now().Before(end) {
			i++
			p := s.topoPrimary()
			target := p
			if target == nil || rng.IntN(2) == 0 {
				var others []*Node
				for _, n := range s.Nodes {
					if n != p {
						others = append(others, n)
					}
				}
				target = others[rng.IntN(len(others))]
			}
			f := soakFaults[rng.IntN(len(soakFaults))]
			counts[f.name]++
			s.Log.Add("soak", fmt.Sprintf("fault %d: %s on %s (primary %s)", i, f.name, target.Tablet.Alias, aliasOf(p)))
			from := time.Now()
			f.run(s, target, rng)
			healed := time.Now()
			np, probs, took := s.WaitConverged(5 * time.Minute)
			if np != nil && np != p {
				failovers++
			}
			if took > slowest {
				slowest = took
			}
			longest, _ := windowOutage(s.W.Records(), from, time.Now())
			s.R.timing("soak fault %d at +%.0fs: %s on %s (primary %s): converged %.1fs after the heal, primary now %s, longest write gap %.1fs",
				i, from.Sub(s.Fault).Seconds(), f.name, target.Tablet.Alias, aliasOf(p), took.Seconds(), aliasOf(np), longest.Seconds())
			if len(probs) > 0 {
				s.R.violation("soak fault %d (%s on %s): not converged 5 minutes after the heal at +%.0fs: %s",
					i, f.name, target.Tablet.Alias, healed.Sub(s.Fault).Seconds(), strings.Join(probs, "; "))
				break
			}
			s.Sleep(between(rng, 5*time.Second, 15*time.Second), "soak: steady state")
		}
		var names []string
		for name, k := range counts {
			names = append(names, fmt.Sprintf("%s=%d", name, k))
		}
		sort.Strings(names)
		s.R.outcome("soak: %d faults in %.0fs (%s), %d primary changes, slowest convergence %.1fs",
			i, time.Since(s.Fault).Seconds(), strings.Join(names, " "), failovers, slowest.Seconds())
	})
}
