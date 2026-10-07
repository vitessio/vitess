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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Race triggers of G12 (CHAOS_RACE_TRIGGER): when the primary's cell is cut off, relative to the
// restarted voter's (the joiner's) join.
const (
	// raceTriggerOffset cuts a fixed offset after the joiner's START GROUP_REPLICATION
	// (CHAOS_RACE_OFFSETS). Whether the group admitted the joiner by then depends on the host's
	// load.
	raceTriggerOffset = "offset"
	// raceTriggerBeforeAdmission cuts as soon as the joiner's START is logged, before the other
	// voter lists the joiner as a member: the group cannot admit it during the cut.
	raceTriggerBeforeAdmission = "before-admission"
	// raceTriggerAfterAdmission cuts once the other voter lists the joiner ONLINE or RECOVERING:
	// the joiner is a member, and votes with the other voter.
	raceTriggerAfterAdmission = "after-admission"
)

func raceTrigger() (string, error) {
	switch v := os.Getenv("CHAOS_RACE_TRIGGER"); v {
	case "", raceTriggerOffset:
		return raceTriggerOffset, nil
	case raceTriggerBeforeAdmission, raceTriggerAfterAdmission:
		return v, nil
	default:
		return "", fmt.Errorf("CHAOS_RACE_TRIGGER=%q: want %s, %s or %s", v, raceTriggerOffset, raceTriggerBeforeAdmission, raceTriggerAfterAdmission)
	}
}

// admissionWatch polls a member's view of its group for another member: when the member first
// lists it (in any state), and when it first lists it ONLINE or RECOVERING.
type admissionWatch struct {
	mu                    sync.Mutex
	listed, active        time.Time
	listedState           string
	cancel                context.CancelFunc
	done                  chan struct{}
	activeCh              chan struct{}
	activeOnce, closeOnce sync.Once
}

func watchAdmission(watcher *Node, uuid string) *admissionWatch {
	ctx, cancel := context.WithCancel(context.Background())
	w := &admissionWatch{cancel: cancel, done: make(chan struct{}), activeCh: make(chan struct{})}
	go func() {
		defer close(w.done)
		for ctx.Err() == nil {
			qctx, qcancel := context.WithTimeout(ctx, 500*time.Millisecond)
			rows, err := watcher.queryCtx(qctx, "select MEMBER_STATE from performance_schema.replication_group_members where MEMBER_ID = ?", uuid)
			qcancel()
			now := time.Now()
			if err == nil && len(rows) == 1 {
				state := rows[0]["MEMBER_STATE"]
				w.mu.Lock()
				if w.listed.IsZero() {
					w.listed, w.listedState = now, state
				}
				if w.active.IsZero() && (state == "ONLINE" || state == "RECOVERING") {
					w.active = now
					w.activeOnce.Do(func() { close(w.activeCh) })
				}
				w.mu.Unlock()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return w
}

// waitActive waits until the watched member is listed ONLINE or RECOVERING.
func (w *admissionWatch) waitActive(timeout time.Duration) bool {
	select {
	case <-w.activeCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (w *admissionWatch) stop() {
	w.closeOnce.Do(func() {
		w.cancel()
		<-w.done
	})
}

func (w *admissionWatch) times() (listed time.Time, state string, active time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.listed, w.listedState, w.active
}

// cpuPressure is a reading of /proc/pressure/cpu ("some" line): the share of time in which at
// least one runnable task waited for a CPU.
type cpuPressure struct {
	at    time.Time
	avg10 float64
	total time.Duration // cumulative stall time
	ok    bool
}

func readCPUPressure() cpuPressure {
	p := cpuPressure{at: time.Now()}
	b, err := os.ReadFile("/proc/pressure/cpu")
	if err != nil {
		return p
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if !strings.HasPrefix(line, "some ") {
			continue
		}
		for f := range strings.FieldsSeq(line) {
			k, v, _ := strings.Cut(f, "=")
			switch k {
			case "avg10":
				p.avg10, _ = strconv.ParseFloat(v, 64)
			case "total":
				us, _ := strconv.ParseInt(v, 10, 64)
				p.total = time.Duration(us) * time.Microsecond
			}
		}
		p.ok = true
	}
	return p
}

// stallShare returns the share of [a, b] in which some task waited for a CPU.
func stallShare(a, b cpuPressure) float64 {
	if !a.ok || !b.ok || !b.at.After(a.at) {
		return 0
	}
	return float64(b.total-a.total) / float64(b.at.Sub(a.at))
}

// underLoadShare is the CPU stall share above which a cycle counts as taken under load: the
// joiner's join timing, and so the race's outcome, then depends on the host. The scenario alone
// (three mysqld restarting and joining, the writers, the observer) stalls 5–11% of a cycle on a
// 4-core host.
const underLoadShare = 0.20
