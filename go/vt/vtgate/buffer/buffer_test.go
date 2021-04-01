/*
Copyright 2019 The Vitess Authors.

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

package buffer

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	keyspace = "ks1"
	shard    = "0"
	// shard2 is only used for tests with two concurrent failovers.
	shard2 = "-80"
)

var (
	failoverErr = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
		"vttablet: rpc error: code = 9 desc = gRPCServerError: retry: operation not allowed in state SHUTTING_DOWN")
	nonFailoverErr = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
		"vttablet: rpc error: code = 9 desc = gRPCServerError: retry: TODO(mberlin): Insert here any realistic error not caused by a failover")

	statsKeyJoined = fmt.Sprintf("%s.%s", keyspace, shard)

	statsKeyJoinedFailoverEndDetected = statsKeyJoined + "." + string(stopFailoverEndDetected)

	statsKeyJoinedWindowExceeded = statsKeyJoined + "." + string(evictedWindowExceeded)

	statsKeyJoinedLastReparentTooRecent = statsKeyJoined + "." + string(skippedLastReparentTooRecent)
	statsKeyJoinedLastFailoverTooRecent = statsKeyJoined + "." + string(skippedLastFailoverTooRecent)

	oldMaster = &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Keyspace: keyspace,
		Shard:    shard,
		Type:     topodatapb.TabletType_MASTER,
		PortMap:  map[string]int32{"vt": int32(100)},
	}
	newMaster = &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 101},
		Keyspace: keyspace,
		Shard:    shard,
		Type:     topodatapb.TabletType_MASTER,
		PortMap:  map[string]int32{"vt": int32(101)},
	}
)

func TestBuffer(t *testing.T) {
	resetVariables()
	defer checkVariables(t)

	// Enable the buffer.
	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	// Dry-run mode will apply to other keyspaces and shards. Not tested here.
	flag.Set("enable_buffer_dry_run", "true")
	defer resetFlagsForTesting()

	// Create the buffer.
	now := time.Now()
	b := newWithNow(func() time.Time { return now })

	// Simulate that the current master reports its ExternallyReparentedTimestamp.
	// vtgate sees this at startup. Additional periodic updates will be sent out
	// after this. If the TabletExternallyReparented RPC is called regularly by
	// an external failover tool, the timestamp will be increased (even though
	// the master did not change.)
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              oldMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	// First request with failover error starts buffering.
	stopped := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	// Start counter must have been increased.
	if got, want := starts.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("buffering start was not tracked: got = %v, want = %v", got, want)
	}

	// Subsequent requests with errors not related to the failover are not buffered.
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, nonFailoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests with non-failover errors must never be buffered. err: %v retryDone: %v", err, retryDone)
	}

	// Subsequent requests are buffered (if their error is nil or caused by the failover).
	stopped2 := issueRequest(context.Background(), t, b, nil)
	stopped3 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 3); err != nil {
		t.Fatal(err)
	}

	// Mimic the failover end.
	now = now.Add(1 * time.Second)
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	// Check that the drain is successful.
	if err := <-stopped; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	if err := <-stopped2; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	if err := <-stopped3; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	// Failover time should have been be published.
	durations := failoverDurationSumMs.Counts()
	if _, ok := durations[statsKeyJoined]; !ok {
		t.Fatalf("a failover time must have been recorded: %v", durations)
	}
	// Recorded max buffer usage should be 3 now.
	if got, want := lastRequestsInFlightMax.Counts()[statsKeyJoined], int64(3); got != want {
		t.Fatalf("wrong value for BufferRequestsInFlightMax: got = %v, want = %v", got, want)
	}
	// Stop counter should have been increased.
	if got, want := stops.Counts()[statsKeyJoinedFailoverEndDetected], int64(1); got != want {
		t.Fatalf("buffering stop was not tracked: got = %v, want = %v", got, want)
	}
	// Utilization in percentage has increased.
	if got, want := utilizationSum.Counts()[statsKeyJoined], int64(30); got != want {
		t.Fatalf("wrong buffer utilization: got = %v, want = %v", got, want)
	}
	// Drain will reset the state to "idle" eventually.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}

	// Second failover: Buffering is skipped because last failover is too recent.
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("subsequent failovers must be skipped due to -buffer_min_time_between_failovers setting. err: %v retryDone: %v", err, retryDone)
	}
	if got, want := requestsSkipped.Counts()[statsKeyJoinedLastFailoverTooRecent], int64(1); got != want {
		t.Fatalf("skipped request was not tracked: got = %v, want = %v", got, want)
	}

	// Second failover is buffered if enough time has passed.
	now = now.Add(*minTimeBetweenFailovers)
	stopped4 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	// Recorded max buffer usage should be 1 for the second failover.
	if got, want := lastRequestsInFlightMax.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("wrong value for BufferRequestsInFlightMax: got = %v, want = %v", got, want)
	}
	// Start counter must have been increased for the second failover.
	if got, want := starts.Counts()[statsKeyJoined], int64(2); got != want {
		t.Fatalf("buffering start was not tracked: got = %v, want = %v", got, want)
	}
	// Stop buffering.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              oldMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})
	if err := <-stopped4; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}

	// Stop counter must have been increased for the second failover.
	if got, want := stops.Counts()[statsKeyJoinedFailoverEndDetected], int64(2); got != want {
		t.Fatalf("buffering stop was not tracked: got = %v, want = %v", got, want)
	}
	// Utilization in percentage has increased.
	if got, want := utilizationSum.Counts()[statsKeyJoined], int64(40); got != want {
		t.Fatalf("wrong buffer utilization: got = %v, want = %v", got, want)
	}
}

// issueRequest simulates executing a request which goes through the buffer.
// If the buffering returned an error, it will be sent on the returned channel.
func issueRequest(ctx context.Context, t *testing.T, b *Buffer, err error) chan error {
	return issueRequestAndBlockRetry(ctx, t, b, err, nil /* markRetryDone */)
}

// issueRequestAndBlockRetry is the same as issueRequest() but allows to signal
// when the buffer should be informed that the retry is done. (For that,
// the channel "markRetryDone" must be closed.)
func issueRequestAndBlockRetry(ctx context.Context, t *testing.T, b *Buffer, err error, markRetryDone chan struct{}) chan error {
	bufferingStopped := make(chan error)

	go func() {
		retryDone, err := b.WaitForFailoverEnd(ctx, keyspace, shard, failoverErr)
		if err != nil {
			bufferingStopped <- err
		}
		if markRetryDone != nil {
			// Wait for the test's signal before we tell the buffer that we retried.
			<-markRetryDone
		}
		if retryDone != nil {
			defer retryDone()
		}
		defer close(bufferingStopped)
	}()

	return bufferingStopped
}

// waitForRequestsInFlight blocks until the buffer queue has reached "count".
// This check is potentially racy and therefore retried up to a timeout of 10s.
func waitForRequestsInFlight(b *Buffer, count int) error {
	start := time.Now()
	sb := b.getOrCreateBuffer(keyspace, shard)
	for {
		got, want := sb.sizeForTesting(), count
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("wrong buffered requests in flight: got = %v, want = %v", got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// waitForState polls the buffer data for up to 10 seconds and returns an error
// if shardBuffer doesn't have the wanted state by then.
func waitForState(b *Buffer, want bufferState) error {
	sb := b.getOrCreateBuffer(keyspace, shard)
	start := time.Now()
	for {
		got := sb.stateForTesting()
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("wrong buffer state: got = %v, want = %v", got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// waitForPoolSlots waits up to 10s that all buffer pool slots have been
// returned. The wait is necessary because in some cases the buffer code
// does not block itself on the wait. But in any case, the slot should be
// returned when the request has finished. See also shardBuffer.unblockAndWait().
func waitForPoolSlots(b *Buffer, want int) error {
	start := time.Now()
	for {
		got := b.bufferSizeSema.Size()
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("not all pool slots were returned: got = %v, want = %v", got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// TestDryRun tests the case when only the dry-run mode is enabled globally.
func TestDryRun(t *testing.T) {
	resetVariables()

	flag.Set("enable_buffer_dry_run", "true")
	defer resetFlagsForTesting()
	b := New()

	// Request does not get buffered.
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests must not be buffered during dry-run. err: %v retryDone: %v", err, retryDone)
	}
	// But the internal state changes though.
	if err := waitForState(b, stateBuffering); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
	if got, want := starts.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("buffering start was not tracked: got = %v, want = %v", got, want)
	}
	if got, want := lastRequestsDryRunMax.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("dry-run request count did not increase: got = %v, want = %v", got, want)
	}

	// End of failover is tracked as well.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
	})
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if got, want := stops.Counts()[statsKeyJoinedFailoverEndDetected], int64(1); got != want {
		t.Fatalf("buffering stop was not tracked: got = %v, want = %v", got, want)
	}
	if got, want := utilizationDryRunSum.Counts()[statsKeyJoined], int64(10); got != want {
		t.Fatalf("wrong buffer utilization: got = %v, want = %v", got, want)
	}
}

// TestPassthrough tests the case when no failover is in progress and
// requests have no failover related error.
func TestPassthrough(t *testing.T) {
	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlagsForTesting()
	b := New()

	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, nil); err != nil || retryDone != nil {
		t.Fatalf("requests with no error must never be buffered. err: %v retryDone: %v", err, retryDone)
	}
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, nonFailoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests with non-failover errors must never be buffered. err: %v retryDone: %v", err, retryDone)
	}

	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
}

// TestLastReparentTooRecent_BufferingSkipped tests that buffering is skipped if
// we see the reparent (end) *before* any request failures due to it.
// We must not start buffering because we already observed the trigger for
// stopping buffering (the reparent) and may not see it again.
func TestLastReparentTooRecent_BufferingSkipped(t *testing.T) {
	resetVariables()

	flag.Set("enable_buffer", "true")
	// Enable the buffer (no explicit whitelist i.e. it applies to everything).
	defer resetFlagsForTesting()
	now := time.Now()
	b := newWithNow(func() time.Time { return now })

	// Simulate that the old master notified us about its reparented timestamp
	// very recently (time.Now()).
	// vtgate should see this immediately after the start.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              oldMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	// Failover to new master. Its end is detected faster than the beginning.
	// Do not start buffering.
	now = now.Add(1 * time.Second)
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests where the failover end was recently detected before the start must not be buffered. err: %v retryDone: %v", err, retryDone)
	}
	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
	if got, want := requestsSkipped.Counts()[statsKeyJoinedLastReparentTooRecent], int64(1); got != want {
		t.Fatalf("skipped request was not tracked: got = %v, want = %v", got, want)
	}
	if got, want := requestsBuffered.Counts()[statsKeyJoined], int64(0); got != want {
		t.Fatalf("no request should have been tracked as buffered: got = %v, want = %v", got, want)
	}
}

// TestLastReparentTooRecent_Buffering explicitly tests that the "too recent"
// skipping of the buffering does NOT get triggered because enough time has
// elapsed since the last seen reparent.
func TestLastReparentTooRecent_Buffering(t *testing.T) {
	resetVariables()

	flag.Set("enable_buffer", "true")
	// Enable the buffer (no explicit whitelist i.e. it applies to everything).
	defer resetFlagsForTesting()
	now := time.Now()
	b := newWithNow(func() time.Time { return now })

	// Simulate that the old master notified us about its reparented timestamp
	// very recently (time.Now()).
	// vtgate should see this immediately after the start.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              oldMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	// Failover to new master. Do not issue any requests before or after i.e.
	// there was 0 QPS traffic and no buffering was started.
	now = now.Add(1 * time.Second)
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	// After we're past the --buffer_min_time_between_failovers threshold, go
	// through a failover with non-zero QPS.
	now = now.Add(*minTimeBetweenFailovers)
	// We're seeing errors first.
	stopped := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	// And then the failover end.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: now.Unix(),
	})

	// Check that the drain is successful.
	if err := <-stopped; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	// Drain will reset the state to "idle" eventually.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}

	if got, want := requestsSkipped.Counts()[statsKeyJoinedLastReparentTooRecent], int64(0); got != want {
		t.Fatalf("request should not have been skipped: got = %v, want = %v", got, want)
	}
	if got, want := requestsBuffered.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("request should have been tracked as buffered: got = %v, want = %v", got, want)
	}
}

// TestPassthroughDuringDrain tests the behavior of requests while the buffer is
// in the drain phase: They should not be buffered and passed through instead.
func TestPassthroughDuringDrain(t *testing.T) {
	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlagsForTesting()
	b := New()

	// Buffer one request.
	markRetryDone := make(chan struct{})
	stopped := issueRequestAndBlockRetry(context.Background(), t, b, failoverErr, markRetryDone)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Stop buffering and trigger drain.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
	})
	if got, want := b.getOrCreateBuffer(keyspace, shard).state, stateDraining; got != want {
		t.Fatalf("wrong expected state. got = %v, want = %v", got, want)
	}

	// Requests during the drain will be passed through and not buffered.
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, nil); err != nil || retryDone != nil {
		t.Fatalf("requests with no error must not be buffered during a drain. err: %v retryDone: %v", err, retryDone)
	}
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests with failover errors must not be buffered during a drain. err: %v retryDone: %v", err, retryDone)
	}

	// Finish draining by telling the buffer that the retry is done.
	close(markRetryDone)
	<-stopped

	// Wait for the drain to complete to avoid races with other tests.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
}

// TestPassthroughIgnoredKeyspaceOrShard tests that the explicit whitelisting
// of keyspaces (and optionally shards) ignores entries which are not listed.
func TestPassthroughIgnoredKeyspaceOrShard(t *testing.T) {
	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlagsForTesting()
	b := New()

	ignoredKeyspace := "ignored_ks"
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), ignoredKeyspace, shard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests for ignored keyspaces must not be buffered. err: %v retryDone: %v", err, retryDone)
	}
	statsKeyJoined := strings.Join([]string{ignoredKeyspace, shard, skippedDisabled}, ".")
	if got, want := requestsSkipped.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("request was not skipped as disabled: got = %v, want = %v", got, want)
	}

	ignoredShard := "ff-"
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, ignoredShard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests for ignored shards must not be buffered. err: %v retryDone: %v", err, retryDone)
	}
	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
	statsKeyJoined = strings.Join([]string{keyspace, ignoredShard, skippedDisabled}, ".")
	if got, want := requestsSkipped.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("request was not skipped as disabled: got = %v, want = %v", got, want)
	}
}

// TestRequestCanceled_ExplicitEnd stops the buffering because the we see the
// new master.
func TestRequestCanceled_ExplicitEnd(t *testing.T) {
	testRequestCanceled(t, true)
}

// TestRequestCanceled_MaxDurationEnd stops the buffering because the max
// failover duration is reached.
func TestRequestCanceled_MaxDurationEnd(t *testing.T) {
	testRequestCanceled(t, false)
}

// testRequestCanceled tests the case when a buffered request is canceled
// (more precisively its context) before the failover/buffering ends.
func testRequestCanceled(t *testing.T, explicitEnd bool) {
	resetVariables()
	defer checkVariables(t)

	flag.Set("enable_buffer", "true")
	// Enable buffering for the complete keyspace and not just a specific shard.
	flag.Set("buffer_keyspace_shards", keyspace)
	defer resetFlagsForTesting()
	b := New()
	if !explicitEnd {
		// Set value after constructor to work-around hardcoded minimum values.
		flag.Set("buffer_window", "100ms")
		flag.Set("buffer_max_failover_duration", "100ms")
	}

	// Buffer 2 requests. The second will be canceled and the first will be drained.
	stopped1 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	stopped2 := issueRequest(ctx2, t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 2); err != nil {
		t.Fatal(err)
	}

	// Cancel second request before buffering stops.
	cancel2()
	// Canceled request will see an error from the buffer.
	if err := isCanceledError(<-stopped2); err != nil {
		t.Fatal(err)
	}
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	// Recorded max buffer usage stay at 2 although the second request was canceled.
	if got, want := lastRequestsInFlightMax.Counts()[statsKeyJoined], int64(2); got != want {
		t.Fatalf("wrong value for BufferRequestsInFlightMax: got = %v, want = %v", got, want)
	}

	if explicitEnd {
		b.StatsUpdate(&discovery.LegacyTabletStats{
			Tablet:                              newMaster,
			Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
			TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
		})
	}

	// Failover will end eventually.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	// First request must have been drained without an error.
	if err := <-stopped1; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}

	// If buffering stopped implicitly, the explicit signal will still happen
	// shortly after. In that case, the buffer should ignore it.
	if !explicitEnd {
		b.StatsUpdate(&discovery.LegacyTabletStats{
			Tablet:                              newMaster,
			Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
			TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
		})
	}
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
}

func TestEviction(t *testing.T) {
	resetVariables()
	defer checkVariables(t)

	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	flag.Set("buffer_size", "2")
	defer resetFlagsForTesting()
	b := New()

	stopped1 := issueRequest(context.Background(), t, b, failoverErr)
	// This wait is important because each request gets inserted asynchronously
	// in the buffer. Usually, they end up in the correct order (1, 2), but there
	// is a chance that it's reversed (2, 1). This wait ensures that 1 goes into
	// the buffer first.
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	stopped2 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 2); err != nil {
		t.Fatal(err)
	}

	// Third request will evict the oldest.
	stopped3 := issueRequest(context.Background(), t, b, failoverErr)

	// Evicted request will see an error from the buffer.
	if err := isEvictedError(<-stopped1); err != nil {
		t.Fatal(err)
	}

	// End of failover. Stop buffering.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
	})

	if err := <-stopped2; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	if err := <-stopped3; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, 2); err != nil {
		t.Fatal(err)
	}
}

func isCanceledError(err error) error {
	if err == nil {
		return fmt.Errorf("buffering should have stopped early and returned an error because the request was canceled from the outside")
	}
	if got, want := vterrors.Code(err), vtrpcpb.Code_UNAVAILABLE; got != want {
		return fmt.Errorf("wrong error code for canceled buffered request. got = %v, want = %v", got, want)
	}
	if got, want := err.Error(), "context was canceled before failover finished"; !strings.Contains(got, want) {
		return fmt.Errorf("canceled buffered request should return a different error message. got = %v, want = %v", got, want)
	}
	return nil
}

// isEvictedError returns nil if "err" is or contains "entryEvictedError".
func isEvictedError(err error) error {
	if err == nil {
		return errors.New("request should have been evicted because the buffer was full")
	}
	if got, want := vterrors.Code(err), vtrpcpb.Code_UNAVAILABLE; got != want {
		return fmt.Errorf("wrong error code for evicted buffered request. got = %v, want = %v full error: %v", got, want, err)
	}
	if got, want := err.Error(), entryEvictedError.Error(); !strings.Contains(got, want) {
		return fmt.Errorf("evicted buffered request should return a different error message. got = %v, want substring = %v", got, want)
	}
	return nil
}

// TestEvictionNotPossible tests the case that the buffer is a) fully in use
// by two failovers and b) the second failover doesn't use any slot in the
// buffer and therefore cannot evict older entries.
func TestEvictionNotPossible(t *testing.T) {
	resetVariables()
	defer checkVariables(t)

	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", fmt.Sprintf("%v,%v",
		topoproto.KeyspaceShardString(keyspace, shard),
		topoproto.KeyspaceShardString(keyspace, shard2)))
	flag.Set("buffer_size", "1")
	defer resetFlagsForTesting()
	b := New()

	// Make the buffer full (applies to all failovers).
	// Also triggers buffering for the first shard.
	stoppedFirstFailover := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Newer requests of the second failover cannot evict anything because
	// they have no entries buffered.
	retryDone, bufferErr := b.WaitForFailoverEnd(context.Background(), keyspace, shard2, failoverErr)
	if bufferErr == nil || retryDone != nil {
		t.Fatalf("buffer should have returned an error because it's full: err: %v retryDone: %v", bufferErr, retryDone)
	}
	if got, want := vterrors.Code(bufferErr), vtrpcpb.Code_UNAVAILABLE; got != want {
		t.Fatalf("wrong error code for evicted buffered request. got = %v, want = %v", got, want)
	}
	if got, want := bufferErr.Error(), bufferFullError.Error(); !strings.Contains(got, want) {
		t.Fatalf("evicted buffered request should return a different error message. got = %v, want substring = %v", got, want)
	}

	// End of failover. Stop buffering.
	b.StatsUpdate(&discovery.LegacyTabletStats{
		Tablet:                              newMaster,
		Target:                              &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
	})
	if err := <-stoppedFirstFailover; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	// Wait for the failover end to avoid races.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, 1); err != nil {
		t.Fatal(err)
	}
	statsKeyJoined := strings.Join([]string{keyspace, shard2, string(skippedBufferFull)}, ".")
	if got, want := requestsSkipped.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("skipped request was not tracked: got = %v, want = %v", got, want)
	}
}

func TestWindow(t *testing.T) {
	resetVariables()
	defer checkVariables(t)

	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", fmt.Sprintf("%v,%v",
		topoproto.KeyspaceShardString(keyspace, shard),
		topoproto.KeyspaceShardString(keyspace, shard2)))
	flag.Set("buffer_size", "1")
	defer resetFlagsForTesting()
	b := New()
	// Set value after constructor to work-around hardcoded minimum values.
	flag.Set("buffer_window", "1ms")

	// Buffer one request.
	t.Logf("first request exceeds its window")
	stopped1 := issueRequest(context.Background(), t, b, failoverErr)

	// Let it go out of the buffering window and expire.
	if err := <-stopped1; err != nil {
		t.Fatalf("buffering should have stopped after exceeding the window without an error: %v", err)
	}
	// Verify that the window was actually exceeded.
	if err := waitForRequestsExceededWindow(1); err != nil {
		t.Fatal(err)
	}

	// Increase the window and buffer a request again
	// (queue becomes not empty a second time).
	flag.Set("buffer_window", "10m")

	// This is a hack. The buffering semaphore gets released asynchronously.
	// Sometimes the next issueRequest tries to acquire before that release
	// and ends up failing. Waiting for the previous goroutines to exit ensures
	// that the sema will get released.
	b.waitForShutdown()

	// This time the request does not go out of window and gets evicted by a third
	// request instead.
	t.Logf("second request does not exceed its window")
	stopped2 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Third request will evict the second one.
	t.Logf("third request evicts the second request")
	stopped3 := issueRequest(context.Background(), t, b, failoverErr)

	// Evicted request will see an error from the buffer.
	if err := isEvictedError(<-stopped2); err != nil {
		t.Fatal(err)
	}
	// Block until the third request is buffered. Avoids data race with *window.
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Verify that the window was not exceeded.
	if got, want := requestsEvicted.Counts()[statsKeyJoinedWindowExceeded], int64(1); got != want {
		t.Fatalf("second or third request should not have exceed its buffering window. got = %v, want = %v", got, want)
	}

	// Reduce the window again.
	flag.Set("buffer_window", "100ms")

	// Fourth request evicts the third
	t.Logf("fourth request exceeds its window (and evicts the third)")
	stopped4 := issueRequest(context.Background(), t, b, failoverErr)
	if err := isEvictedError(<-stopped3); err != nil {
		t.Fatal(err)
	}

	// Fourth request will exceed its window and finish early.
	if err := <-stopped4; err != nil {
		t.Fatalf("buffering should have stopped after 10ms without an error: %v", err)
	}
	// Verify that the window was actually exceeded.
	if err := waitForRequestsExceededWindow(2); err != nil {
		t.Fatal(err)
	}

	// At this point the buffer is empty but buffering is still active.
	// Simulate that the buffering stops because the max duration (10m) was reached.
	b.getOrCreateBuffer(keyspace, shard).stopBufferingDueToMaxDuration()
	// Wait for the failover end to avoid races.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
	if err := waitForPoolSlots(b, 1); err != nil {
		t.Fatal(err)
	}
}

func waitForRequestsExceededWindow(count int) error {
	start := time.Now()
	for {
		got, want := requestsEvicted.Counts()[statsKeyJoinedWindowExceeded], int64(count)
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("wrong number of requests which exceeded their buffering window: got = %v, want = %v", got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// TestShutdown tests that Buffer.Shutdown() unblocks any pending bufferings
// immediately.
func TestShutdown(t *testing.T) {
	resetVariables()
	defer checkVariables(t)

	flag.Set("enable_buffer", "true")
	defer resetFlagsForTesting()
	b := New()

	// Buffer one request.
	stopped1 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Shutdown buffer and unblock buffered request immediately.
	b.Shutdown()

	// Request must have been drained without an error.
	if err := <-stopped1; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}

	if err := waitForPoolSlots(b, *size); err != nil {
		t.Fatal(err)
	}
}

// resetVariables resets the task level variables. The code does not reset these
// with very failover.
func resetVariables() {
	starts.ResetAll()
	stops.ResetAll()

	utilizationSum.ResetAll()
	utilizationDryRunSum.ResetAll()

	requestsBuffered.ResetAll()
	requestsBufferedDryRun.ResetAll()
	requestsDrained.ResetAll()
	requestsEvicted.ResetAll()
	requestsSkipped.ResetAll()
}

// checkVariables makes sure that the invariants described in variables.go
// hold up.
func checkVariables(t *testing.T) {
	for k, buffered := range requestsBuffered.Counts() {
		evicted := int64(0)
		// The evicted count is grouped by Reason i.e. the entries are named
		// "<Keyspace>.<Shard>.<Reason>". Match all reasons for this shard.
		for withReason, v := range requestsEvicted.Counts() {
			if strings.HasPrefix(withReason, fmt.Sprintf("%s.", k)) {
				evicted += v
			}
		}
		drained := requestsDrained.Counts()[k]

		if buffered != evicted+drained {
			t.Fatalf("buffered == evicted + drained is violated: %v != %v + %v", buffered, evicted, drained)
		} else {
			t.Logf("buffered == evicted + drained: %v == %v + %v", buffered, evicted, drained)
		}
	}
}
