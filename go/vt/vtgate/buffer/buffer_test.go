package buffer

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	keyspace = "ks1"
	shard    = "0"
	// shard2 is only used for tests with two concurrent failovers.
	shard2 = "-80"
)

var (
	failoverErr = vterrors.FromError(vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		errors.New("vttablet: rpc error: code = 9 desc = gRPCServerError: retry: operation not allowed in state SHUTTING_DOWN"))
	nonFailoverErr = vterrors.FromError(vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		errors.New("vttablet: rpc error: code = 9 desc = gRPCServerError: retry: TODO(mberlin): Insert here any realistic error not caused by a failover"))

	statsKeyJoined = fmt.Sprintf("%s.%s", keyspace, shard)
)

func TestBuffer(t *testing.T) {
	// Enable the buffer.
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlags()

	// Create the buffer.
	b := New()

	// First request with failover error starts buffering.
	stopped := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
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
	b.StatsUpdate(&discovery.TabletStats{
		Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
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
	durations := failoverDurationMs.Counts()
	if _, ok := durations[statsKeyJoined]; !ok {
		t.Fatalf("a failover time must have been recorded: %v", durations)
	}
	// Recorded max buffer usage should be 3 now.
	if got, want := requestsInFlightMax.Counts()[statsKeyJoined], int64(3); got != want {
		t.Fatalf("wrong value for BufferRequestsInFlightMax: got = %v, want = %v", got, want)
	}
	// Drain will reset the state to "idle" eventually.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}

	// Second failover: Buffering is skipped because last failover is too recent.
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, failoverErr); err != nil || retryDone != nil {
		t.Fatalf("subsequent failovers must be skipped due to -vtgate_buffer_min_time_between_failovers setting. err: %v retryDone: %v", err, retryDone)
	}

	// Second failover is buffered if we reduce the limit.
	flag.Set("vtgate_buffer_min_time_between_failovers", "0s")
	stopped4 := issueRequest(context.Background(), t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}
	// Recorded max buffer usage should be 1 for the second failover.
	if got, want := requestsInFlightMax.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("wrong value for BufferRequestsInFlightMax: got = %v, want = %v", got, want)
	}
	// Stop buffering.
	b.StatsUpdate(&discovery.TabletStats{
		Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 2, // Must be >1.
	})
	if err := <-stopped4; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
}

func resetFlags() {
	flag.Set("enable_vtgate_buffer", "false")
	flag.Set("vtgate_buffer_window", "10s")
	flag.Set("vtgate_buffer_keyspace_shards", "")
	flag.Set("vtgate_buffer_max_failover_duration", "20s")
	flag.Set("vtgate_buffer_min_time_between_failovers", "5m")
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
		defer retryDone()
		defer close(bufferingStopped)
	}()

	return bufferingStopped
}

// waitForRequestsInFlight blocks until the buffer queue has reached "count".
// This check is potentially racy and therefore retried up to a timeout of 2s.
func waitForRequestsInFlight(b *Buffer, count int) error {
	start := time.Now()
	sb := b.getOrCreateBuffer(keyspace, shard)
	for {
		got, want := sb.sizeForTesting(), count
		if got == want {
			return nil
		}

		if time.Since(start) > 2*time.Second {
			return fmt.Errorf("wrong buffered requests in flight: got = %v, want = %v", got, want)
		}
	}
}

// waitForState polls the buffer data for up to 2 seconds and returns an error
// if shardBuffer doesn't have the wanted state by then.
func waitForState(b *Buffer, want bufferState) error {
	sb := b.getOrCreateBuffer(keyspace, shard)
	start := time.Now()
	for {
		got := sb.stateForTesting()
		if got == want {
			return nil
		}

		if time.Since(start) > 2*time.Second {
			return fmt.Errorf("wrong buffer state: got = %v, want = %v", got, want)
		}
	}
}

// TestPassthrough tests the case when no failover is in progress and
// requests have no failover related error.
func TestPassthrough(t *testing.T) {
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlags()
	b := New()

	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, nil); err != nil || retryDone != nil {
		t.Fatalf("requests with no error must never be buffered. err: %v retryDone: %v", err, retryDone)
	}
	if retryDone, err := b.WaitForFailoverEnd(context.Background(), keyspace, shard, nonFailoverErr); err != nil || retryDone != nil {
		t.Fatalf("requests with non-failover errors must never be buffered. err: %v retryDone: %v", err, retryDone)
	}
}

// TestPassthroughDuringDrain tests the behavior of requests while the buffer is
// in the drain phase: They should not be buffered and passed through instead.
func TestPassthroughDuringDrain(t *testing.T) {
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlags()
	b := New()

	// Buffer one request.
	markRetryDone := make(chan struct{})
	stopped := issueRequestAndBlockRetry(context.Background(), t, b, failoverErr, markRetryDone)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Stop buffering and trigger drain.
	b.StatsUpdate(&discovery.TabletStats{
		Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
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
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlags()
	b := New()
	if !explicitEnd {
		// Set value after constructor to work-around hardcoded minimum values.
		flag.Set("vtgate_buffer_window", "100ms")
		flag.Set("vtgate_buffer_max_failover_duration", "100ms")
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
	if got, want := requestsInFlightMax.Counts()[statsKeyJoined], int64(2); got != want {
		t.Fatalf("wrong value for BufferRequestsInFlightMax: got = %v, want = %v", got, want)
	}

	if explicitEnd {
		b.StatsUpdate(&discovery.TabletStats{
			Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
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
}

func TestEviction(t *testing.T) {
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	flag.Set("vtgate_buffer_size", "2")
	defer resetFlags()
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
	b.StatsUpdate(&discovery.TabletStats{
		Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
	})

	if err := <-stopped2; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	if err := <-stopped3; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
}

func isCanceledError(err error) error {
	if err == nil {
		return fmt.Errorf("buffering should have stopped early and returned an error because the request was canceled from the outside")
	}
	if got, want := vterrors.RecoverVtErrorCode(err), vtrpcpb.ErrorCode_TRANSIENT_ERROR; got != want {
		return fmt.Errorf("wrong error code for canceled buffered request. got = %v, want = %v", got, want)
	}
	if got, want := err.Error(), "context was canceled before failover finished (context canceled)"; got != want {
		return fmt.Errorf("canceled buffered request should return a different error message. got = %v, want = %v", got, want)
	}
	return nil
}

// isEvictedError returns nil if "err" is or contains "entryEvictedError".
func isEvictedError(err error) error {
	if err == nil {
		return errors.New("request should have been evicted because the buffer was full")
	}
	if got, want := vterrors.RecoverVtErrorCode(err), vtrpcpb.ErrorCode_TRANSIENT_ERROR; got != want {
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
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", fmt.Sprintf("%v,%v",
		topoproto.KeyspaceShardString(keyspace, shard),
		topoproto.KeyspaceShardString(keyspace, shard2)))
	flag.Set("vtgate_buffer_size", "1")
	defer resetFlags()
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
	if got, want := vterrors.RecoverVtErrorCode(bufferErr), vtrpcpb.ErrorCode_TRANSIENT_ERROR; got != want {
		t.Fatalf("wrong error code for evicted buffered request. got = %v, want = %v", got, want)
	}
	if got, want := bufferErr.Error(), bufferFullError.Error(); !strings.Contains(got, want) {
		t.Fatalf("evicted buffered request should return a different error message. got = %v, want substring = %v", got, want)
	}

	// End of failover. Stop buffering.
	b.StatsUpdate(&discovery.TabletStats{
		Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 1, // Use any value > 0.
	})
	if err := <-stoppedFirstFailover; err != nil {
		t.Fatalf("request should have been buffered and not returned an error: %v", err)
	}
	// Wait for the failover end to avoid races.
	if err := waitForState(b, stateIdle); err != nil {
		t.Fatal(err)
	}
}

func TestWindow(t *testing.T) {
	requestsWindowExceeded.Reset()
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", fmt.Sprintf("%v,%v",
		topoproto.KeyspaceShardString(keyspace, shard),
		topoproto.KeyspaceShardString(keyspace, shard2)))
	flag.Set("vtgate_buffer_size", "1")
	defer resetFlags()
	b := New()
	// Set value after constructor to work-around hardcoded minimum values.
	flag.Set("vtgate_buffer_window", "1ms")

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
	flag.Set("vtgate_buffer_window", "10m")

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
	if got, want := requestsWindowExceeded.Counts()[statsKeyJoined], int64(1); got != want {
		t.Fatalf("second or third request should not have exceed its buffering window. got = %v, want = %v", got, want)
	}

	// Reduce the window again.
	flag.Set("vtgate_buffer_window", "100ms")

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
}

func waitForRequestsExceededWindow(count int) error {
	start := time.Now()
	for {
		got, want := requestsWindowExceeded.Counts()[statsKeyJoined], int64(count)
		if got == want {
			return nil
		}

		if time.Since(start) > 2*time.Second {
			return fmt.Errorf("wrong number of requests which exceeded their buffering window: got = %v, want = %v", got, want)
		}
	}
}
