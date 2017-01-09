package buffer

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	// Drain will reset the state to "idle" eventually.
	waitForState(b, stateIdle)

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
	// Stop buffering.
	b.StatsUpdate(&discovery.TabletStats{
		Target: &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_MASTER},
		TabletExternallyReparentedTimestamp: 2, // Must be >1.
	})
	<-stopped4
}

func resetFlags() {
	flag.Set("enable_vtgate_buffer", "false")
	flag.Set("vtgate_buffer_keyspace_shards", "")
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

// TestRequestCanceled tests the case when a buffered request is canceled
// (more precisively its context) before the failover/buffering ends.
func TestRequestCanceled(t *testing.T) {
	flag.Set("enable_vtgate_buffer", "true")
	flag.Set("vtgate_buffer_keyspace_shards", topoproto.KeyspaceShardString(keyspace, shard))
	defer resetFlags()
	b := New()

	ctx, cancel := context.WithCancel(context.Background())
	stopped := issueRequest(ctx, t, b, failoverErr)
	if err := waitForRequestsInFlight(b, 1); err != nil {
		t.Fatal(err)
	}

	// Cancel request before buffering stops.
	cancel()

	// Canceled request will see an error from the buffer.
	bufferErr := <-stopped
	if bufferErr == nil {
		t.Fatalf("buffering should have stopped early and returned an error because the request was canceled from the outside")
	}
	if got, want := vterrors.RecoverVtErrorCode(bufferErr), vtrpcpb.ErrorCode_TRANSIENT_ERROR; got != want {
		t.Fatalf("wrong error code for canceled buffered request. got = %v, want = %v", got, want)
	}
	if got, want := bufferErr.Error(), "context was canceled before failover finished (context canceled)"; got != want {
		t.Fatalf("canceled buffered request should return a different error message. got = %v, want = %v", got, want)
	}
}
