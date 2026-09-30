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
