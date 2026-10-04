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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ackedAt returns acknowledged writes that completed at the given offsets (in seconds) from t0.
func ackedAt(t0 time.Time, secs ...float64) []WriteRecord {
	var recs []WriteRecord
	for i, s := range secs {
		end := t0.Add(time.Duration(s * float64(time.Second)))
		recs = append(recs, WriteRecord{ID: int64(i), Start: end.Add(-5 * time.Millisecond), End: end, Acked: true})
	}
	return recs
}

// TestComputeStatsCountsOutageOngoingAtStop checks that an outage that lasts until the writers stop
// is the longest gap, measured from the last acked write to the stop, and counts as unavailable.
func TestComputeStatsCountsOutageOngoingAtStop(t *testing.T) {
	t0 := time.Now()
	// Acks every 0.5s until 10s, a 3s outage (10s to 13s), acks until 20s, then none until the
	// writers stop at 60s.
	var secs []float64
	for s := 0.0; s <= 10; s += 0.5 {
		secs = append(secs, s)
	}
	for s := 13.0; s <= 20; s += 0.5 {
		secs = append(secs, s)
	}
	recs := ackedAt(t0, secs...)
	recs = append(recs, WriteRecord{ID: 999, Start: t0.Add(30 * time.Second), End: t0.Add(34 * time.Second), Err: "timeout"})
	st := computeStats(recs, t0.Add(5*time.Second), t0.Add(60*time.Second))

	assert.Equal(t, len(secs)+1, st.Total)
	assert.Equal(t, 1, st.Failed)
	assert.Equal(t, 40*time.Second, st.LongestGap)
	assert.True(t, st.GapOngoing)
	assert.Equal(t, t0.Add(20*time.Second), st.GapFrom)
	assert.Equal(t, t0.Add(60*time.Second), st.GapTo)
	assert.True(t, st.FirstAckAfterGap.IsZero())
	require.Len(t, st.Outages, 2)
	assert.Equal(t, 3*time.Second, st.Outages[0].Duration())
	assert.False(t, st.Outages[0].Ongoing)
	assert.True(t, st.Outages[1].Ongoing)
	assert.Equal(t, 43*time.Second, st.Unavailable)
}

// TestComputeStatsRecoveredOutage checks the gap and the unavailable time of an outage that ended
// before the writers stopped.
func TestComputeStatsRecoveredOutage(t *testing.T) {
	t0 := time.Now()
	st := computeStats(ackedAt(t0, 0, 0.5, 1, 8, 8.5, 9, 9.5), t0.Add(time.Second), t0.Add(9500*time.Millisecond))
	assert.Equal(t, 7*time.Second, st.LongestGap)
	assert.False(t, st.GapOngoing)
	assert.Equal(t, t0.Add(8*time.Second), st.FirstAckAfterGap)
	require.Len(t, st.Outages, 1)
	assert.Equal(t, 7*time.Second, st.Unavailable)
}

// TestObserverRepromoted checks that a vttablet that was demoted and then promoted again counts as
// re-promoted from its last sample before the promotion, and that a primary that was never
// demoted, like an isolated old primary, does not.
func TestObserverRepromoted(t *testing.T) {
	t0 := time.Now()
	sample := func(secs float64, typ string) Sample {
		return Sample{T: t0.Add(time.Duration(secs * float64(time.Second))), TypeOK: true, Type: typ}
	}
	o := &Observer{samples: [][]Sample{
		{sample(1, "PRIMARY"), sample(2, "PRIMARY"), sample(3, "REPLICA"), sample(4, "REPLICA"), sample(5, "PRIMARY")},
		{sample(1, "PRIMARY"), sample(2, "PRIMARY"), sample(3, "PRIMARY")},
	}}
	at, ok := o.Repromoted(0, t0.Add(1500*time.Millisecond))
	require.True(t, ok)
	assert.Equal(t, t0.Add(4*time.Second), at)
	_, ok = o.Repromoted(1, t0)
	assert.False(t, ok)
}

// TestOldPrimaryReads checks how the primary reads after a fault are attributed: reads the old
// primary answered after another member was elected are stale, and reads after the old primary
// became the topo primary again are not counted.
func TestOldPrimaryReads(t *testing.T) {
	t0 := time.Now()
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	read := func(s float64, uuid, err string) ReadRecord {
		return ReadRecord{Start: at(s), End: at(s + 0.01), UUID: uuid, Err: err}
	}
	reads := []ReadRecord{
		read(-1, "old", ""),    // before the fault
		read(1, "old", ""),     // old primary, before the election
		read(6, "old", ""),     // stale: after the election at +5
		read(7, "", "timeout"), // failed
		read(8, "old", ""),     // stale, and after the topo change at +7.5
		read(9, "new", ""),     // new primary
		read(30, "old", ""),    // the old primary is the topo primary again from +20
	}
	st := oldPrimaryReads(reads, "old", t0, at(7.5), true, at(5), true, at(20), true)
	assert.Equal(t, 5, st.Total)
	assert.Equal(t, 1, st.Failed)
	assert.Equal(t, 3, st.Old)
	assert.Equal(t, 2, st.Stale)
	assert.Equal(t, 1, st.AfterTopoChange)
	assert.Equal(t, at(8.01), st.LastStale)
	assert.Equal(t, at(8.01), st.LastOld)
}

// TestOldPrimaryProbes checks which write probe transactions count as routed to the old primary:
// those it answered, and those whose first statement failed while vtgate's primary reads still
// went to it; and that the waits of the failed ones are reported.
func TestOldPrimaryProbes(t *testing.T) {
	t0 := time.Now()
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	probe := func(start, end float64, uuid, err string) ProbeRecord {
		return ProbeRecord{Start: at(start), End: at(end), UUID: uuid, Acked: err == "", Err: err}
	}
	reads := []ReadRecord{
		{Start: at(0.1), End: at(0.2), UUID: "old"},
		{Start: at(9), End: at(9.1), UUID: "new"},
	}
	probes := []ProbeRecord{
		// Before the fault.
		probe(-1, -0.9, "old", ""),
		probe(0.5, 8.5, "old", "Error 3101 (HY000): Plugin instructed the server to rollback (errno 3101)"),
		// Inferred from the read at +0.1.
		probe(2, 9, "", "context deadline exceeded"),
		// The new primary.
		probe(9.5, 9.6, "new", ""),
		// Not routed to the old primary: the read at +9 went to the new primary.
		probe(10, 10.1, "", "Error 1203 (42000): target: ks.0.primary: vttablet: rpc error"),
		// The old primary is the topo primary again from +20.
		probe(30, 30.1, "old", ""),
	}
	st := oldPrimaryProbes(probes, reads, "old", t0, at(20), true)
	assert.Equal(t, 4, st.Started)
	assert.Equal(t, 2, st.Routed)
	assert.Equal(t, 1, st.Inferred)
	assert.Equal(t, 2, st.Failed)
	assert.Equal(t, 0, st.Acked)
	assert.Equal(t, []time.Duration{7 * time.Second, 8 * time.Second}, st.Waits)
	assert.Equal(t, 8*time.Second, st.MaxWaitFirstSecond)
	assert.Equal(t, at(9), st.LastEnd)
	assert.Equal(t, map[string]int{"errno 3101": 1, "context deadline exceeded": 1}, st.Kinds)
	assert.Equal(t, "errno 1203", probeErrorKind(probes[4].Err))
}

func TestLastAckedBefore(t *testing.T) {
	t0 := time.Now()
	w := &Workload{acked: []ackedWrite{{id: 1, end: t0}, {id: 2, end: t0.Add(time.Second)}, {id: 3, end: t0.Add(2 * time.Second)}}}
	_, ok := w.lastAckedBefore(t0)
	assert.False(t, ok)
	got, ok := w.lastAckedBefore(t0.Add(1500 * time.Millisecond))
	assert.True(t, ok)
	assert.Equal(t, int64(2), got.id)
	got, _ = w.lastAckedBefore(t0.Add(time.Hour))
	assert.Equal(t, int64(3), got.id)
}

// TestFirstTopoPrimaryUsesNewTerm checks that a primary promoted again counts as back from the
// start of its new term, which the shard record names after the promotion, and not from when the
// shard record was observed.
func TestFirstTopoPrimaryUsesNewTerm(t *testing.T) {
	t0 := time.Now()
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	o := &Observer{topo: []TopoSample{
		{T: at(1), Primary: "zone1-0000000100", Term: at(-10)},
		{T: at(3), Primary: "zone2-0000000200", Term: at(2.5)},
		{T: at(9), Primary: "zone1-0000000100", Term: at(8.7)},
	}}
	got, ok := o.FirstTopoPrimary("zone1-0000000100", at(3))
	require.True(t, ok)
	assert.Equal(t, at(8.7), got)
	got, ok = o.FirstTopoPrimary("zone1-0000000100", at(0))
	require.True(t, ok)
	assert.Equal(t, at(1), got)
}
