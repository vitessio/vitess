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

package vitesst

import (
	"strings"
	"sync"

	"github.com/testcontainers/testcontainers-go"
)

const (
	// logRingCapacity bounds how many log lines are retained per container.
	logRingCapacity = 4096

	// logDumpTail is how many trailing lines per component are dumped to test
	// output when a test fails. Full logs go to VITESST_ARTIFACTS if set.
	logDumpTail = 100
)

type (
	// ringLogConsumer is a bounded ring buffer of container log lines. Every
	// container gets one so failure diagnostics are always available without
	// unbounded memory growth. When follow is set, lines are also forwarded
	// to the cluster log as they arrive.
	ringLogConsumer struct {
		name string

		mu    sync.Mutex
		lines []string
		next  int
		total int
	}
)

var _ testcontainers.LogConsumer = (*ringLogConsumer)(nil)

func newRingLogConsumer(name string) *ringLogConsumer {
	return &ringLogConsumer{
		name:  name,
		lines: make([]string, 0, logRingCapacity),
	}
}

// Accept implements testcontainers.LogConsumer.
func (rc *ringLogConsumer) Accept(l testcontainers.Log) {
	line := strings.TrimRight(string(l.Content), "\n")

	rc.mu.Lock()
	if len(rc.lines) < logRingCapacity {
		rc.lines = append(rc.lines, line)
	} else {
		rc.lines[rc.next] = line
	}
	rc.next = (rc.next + 1) % logRingCapacity
	rc.total++
	rc.mu.Unlock()
}

// tail returns up to n trailing log lines in arrival order.
func (rc *ringLogConsumer) tail(n int) []string {
	all := rc.snapshot()
	if len(all) > n {
		all = all[len(all)-n:]
	}
	return all
}

// snapshot returns all retained log lines in arrival order, oldest first.
func (rc *ringLogConsumer) snapshot() []string {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if len(rc.lines) < logRingCapacity {
		out := make([]string, len(rc.lines))
		copy(out, rc.lines)
		return out
	}

	out := make([]string, 0, logRingCapacity)
	out = append(out, rc.lines[rc.next:]...)
	out = append(out, rc.lines[:rc.next]...)
	return out
}

// dump writes the retained lines through the given log function, prefixed
// with the component name. When truncated, it notes how many lines were
// dropped.
func (rc *ringLogConsumer) dump(logf func(format string, args ...any), tail int) {
	lines := rc.tail(tail)

	rc.mu.Lock()
	total := rc.total
	rc.mu.Unlock()

	if dropped := total - len(lines); dropped > 0 {
		logf("[%s] ... %d earlier lines omitted ...", rc.name, dropped)
	}
	for _, line := range lines {
		logf("[%s] %s", rc.name, line)
	}
}

// newLogConsumer returns the ring-buffer consumer for a component,
// registering it for failure diagnostics. A component that recreates its
// container, like VTGate.Restart, keeps its existing ring so the pre-restart
// history survives and artifact files stay one-per-component.
func (c *Cluster) newLogConsumer(name string) *ringLogConsumer {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, rc := range c.logConsumers {
		if rc.name == name {
			return rc
		}
	}

	rc := newRingLogConsumer(name)
	c.logConsumers = append(c.logConsumers, rc)
	return rc
}

// dumpLogs writes the tail of every component's retained logs through logf.
func (c *Cluster) dumpLogs(logf func(format string, args ...any)) {
	c.mu.Lock()
	consumers := make([]*ringLogConsumer, len(c.logConsumers))
	copy(consumers, c.logConsumers)
	c.mu.Unlock()

	for _, rc := range consumers {
		logf("==== logs: %s ====", rc.name)
		rc.dump(logf, logDumpTail)
	}
}
