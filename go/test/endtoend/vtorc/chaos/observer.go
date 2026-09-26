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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var httpClient = &http.Client{Timeout: time.Second}

// EventLog is a timestamped scenario timeline.
type EventLog struct {
	Name  string
	mu    sync.Mutex
	start time.Time
	lines []string
}

func NewEventLog(name string) *EventLog { return &EventLog{Name: name, start: time.Now()} }

func (l *EventLog) Add(kind, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	line := fmt.Sprintf("%s +%7.2fs [%s] %s", now.Format("15:04:05.000"), now.Sub(l.start).Seconds(), kind, msg)
	l.lines = append(l.lines, line)
	fmt.Println("CHAOS " + line)
}

func (l *EventLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n") + "\n"
}

// Sample is one observation of one tablet.
type Sample struct {
	T             time.Time // when the responses arrived
	Start         time.Time // when the probe was sent
	MySQLOK       bool
	ReadOnly      bool
	SuperReadOnly bool
	GTID          string
	TypeOK        bool
	Type          string // tablet type as reported by the vttablet itself
}

// WritablePrimary is true when both mysqld accepts writes and the vttablet believes it is PRIMARY.
func (s Sample) WritablePrimary() bool {
	return s.MySQLOK && !s.ReadOnly && s.TypeOK && s.Type == "PRIMARY"
}

// TopoSample records the shard primary in the global topo.
type TopoSample struct {
	T       time.Time
	Primary string
	Term    time.Time
	Err     string
}

// Observer samples all tablets and the topo continuously during a scenario.
type Observer struct {
	c       *Chaos
	mu      sync.Mutex
	samples [][]Sample
	topo    []TopoSample
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	client  *http.Client
	lastTyp []string
}

func (c *Chaos) StartObserver() *Observer {
	ctx, cancel := context.WithCancel(context.Background())
	o := &Observer{
		c: c, cancel: cancel, samples: make([][]Sample, len(c.Nodes)), lastTyp: make([]string, len(c.Nodes)),
		client: &http.Client{Timeout: time.Second},
	}
	for i := range c.Nodes {
		o.wg.Add(1)
		go o.sampleNode(ctx, i)
	}
	o.wg.Add(1)
	go o.sampleTopo(ctx)
	return o
}

func (o *Observer) Stop() {
	o.cancel()
	o.wg.Wait()
}

func (o *Observer) sampleNode(ctx context.Context, i int) {
	defer o.wg.Done()
	n := o.c.Nodes[i]
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		s := Sample{T: time.Now()}
		var wg sync.WaitGroup
		wg.Go(func() {
			qctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
			defer cancel()
			var ro, sro int
			err := n.db.QueryRowContext(qctx, "select @@global.read_only, @@global.super_read_only, @@global.gtid_executed").Scan(&ro, &sro, &s.GTID)
			if err == nil {
				s.MySQLOK, s.ReadOnly, s.SuperReadOnly = true, ro == 1, sro == 1
			}
		})
		s.Type, s.TypeOK = o.tabletType(n)
		wg.Wait()
		// Timestamp the sample when the responses arrived: a request sent to a hung (SIGSTOPed)
		// process is answered only after it resumes, with the state as of the resume.
		s.Start, s.T = s.T, time.Now()
		o.mu.Lock()
		o.samples[i] = append(o.samples[i], s)
		prev := o.lastTyp[i]
		cur := describeSample(s)
		o.lastTyp[i] = cur
		o.mu.Unlock()
		if cur != prev {
			o.c.Log.Add("observe", fmt.Sprintf("%s: %s", n.Tablet.Alias, cur))
		}
	}
}

func describeSample(s Sample) string {
	my := "mysql=DOWN"
	if s.MySQLOK {
		my = fmt.Sprintf("read_only=%v super_read_only=%v", s.ReadOnly, s.SuperReadOnly)
	}
	typ := "vttablet=DOWN"
	if s.TypeOK {
		typ = "vttablet=" + s.Type
	}
	return typ + " " + my
}

func (o *Observer) tabletType(n *Node) (string, bool) {
	resp, err := o.client.Get(fmt.Sprintf("http://localhost:%d/debug/vars", n.Tablet.HTTPPort))
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		return "", false
	}
	var v struct {
		TabletType string
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return "", false
	}
	return strings.ToUpper(v.TabletType), true
}

func (o *Observer) sampleTopo(ctx context.Context) {
	defer o.wg.Done()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	last := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		s := TopoSample{T: time.Now()}
		qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		si, err := o.c.Ts.GetShard(qctx, keyspaceName, shardName)
		cancel()
		if err != nil {
			s.Err = err.Error()
		} else {
			s.Primary = topoproto.TabletAliasString(si.PrimaryAlias)
			if si.PrimaryTermStartTime != nil {
				s.Term = time.Unix(si.PrimaryTermStartTime.Seconds, int64(si.PrimaryTermStartTime.Nanoseconds))
			}
		}
		o.mu.Lock()
		o.topo = append(o.topo, s)
		o.mu.Unlock()
		desc := s.Primary
		if s.Err != "" {
			desc = "ERR"
		}
		if desc != last {
			o.c.Log.Add("topo", "shard primary = "+desc)
			last = desc
		}
	}
}

// FirstTopoChange returns the first time after `after` the topo primary differed from `from`.
func (o *Observer) FirstTopoChange(from string, after time.Time) (string, time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.topo {
		if s.T.After(after) && s.Err == "" && s.Primary != from && s.Primary != "" {
			return s.Primary, s.T, true
		}
	}
	return "", time.Time{}, false
}

// FirstTopoPrimary returns the first time after `after` the topo primary was `alias`.
func (o *Observer) FirstTopoPrimary(alias string, after time.Time) (time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.topo {
		if s.T.After(after) && s.Err == "" && s.Primary == alias {
			return s.T, true
		}
	}
	return time.Time{}, false
}

// SplitBrain is a period during which two tablets were both writable primaries.
type SplitBrain struct {
	A, B       string
	From, To   time.Time
	Samples    int
	MySQLOnly  bool // only the weaker condition (both mysqld read_only=OFF) held
	Confirmed  bool
	Descriptor string
}

// SplitBrains returns every pair of samples (from different tablets, taken within 250ms of each
// other) in which both tablets were writable primaries (or, as a weaker condition, both had a
// writable mysqld). Overlapping pairs are merged into periods.
func (o *Observer) SplitBrains() []SplitBrain {
	o.mu.Lock()
	defer o.mu.Unlock()
	var res []SplitBrain
	for weak := range 2 {
		for a := 0; a < len(o.samples); a++ {
			for b := a + 1; b < len(o.samples); b++ {
				var cur *SplitBrain
				j := 0
				for _, sa := range o.samples[a] {
					for j < len(o.samples[b]) && o.samples[b][j].T.Before(sa.T.Add(-250*time.Millisecond)) {
						j++
					}
					hit := false
					for k := j; k < len(o.samples[b]) && !o.samples[b][k].T.After(sa.T.Add(250*time.Millisecond)); k++ {
						sb := o.samples[b][k]
						if weak == 0 && sa.WritablePrimary() && sb.WritablePrimary() {
							hit = true
						}
						if weak == 1 && sa.MySQLOK && !sa.ReadOnly && sb.MySQLOK && !sb.ReadOnly {
							hit = true
						}
					}
					if hit {
						if cur == nil {
							cur = &SplitBrain{A: o.c.Nodes[a].Tablet.Alias, B: o.c.Nodes[b].Tablet.Alias, From: sa.T, MySQLOnly: weak == 1}
						}
						cur.To = sa.T
						cur.Samples++
					} else if cur != nil {
						res = append(res, *cur)
						cur = nil
					}
				}
				if cur != nil {
					res = append(res, *cur)
				}
			}
		}
	}
	return res
}

// LastSample returns the latest sample of node i.
func (o *Observer) LastSample(i int) (Sample, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.samples[i]) == 0 {
		return Sample{}, false
	}
	return o.samples[i][len(o.samples[i])-1], true
}

// FirstContaining returns the first sample time at which node i's gtid_executed contained set.
func (o *Observer) FirstContaining(i int, set string) (time.Time, bool) {
	want, err := replication.ParseMysql56GTIDSet(set)
	if err != nil {
		return time.Time{}, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.samples[i] {
		if !s.MySQLOK {
			continue
		}
		have, err := replication.ParseMysql56GTIDSet(s.GTID)
		if err == nil && have.Contains(want) {
			return s.T, true
		}
	}
	return time.Time{}, false
}
