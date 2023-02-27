package buffer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type failover func(buf *Buffer, tablet *topodatapb.Tablet, keyspace, shard string, now time.Time)

func testAllImplementations(t *testing.T, runTest func(t *testing.T, fail failover)) {
	t.Helper()

	t.Run("HealthCheck", func(t *testing.T) {
		t.Helper()
		runTest(t, func(buf *Buffer, tablet *topodatapb.Tablet, keyspace, shard string, now time.Time) {
			buf.ProcessPrimaryHealth(&discovery.TabletHealth{
				Tablet:               tablet,
				Target:               &query.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
				PrimaryTermStartTime: now.Unix(),
			})
		})
	})

	t.Run("KeyspaceEvent", func(t *testing.T) {
		t.Helper()
		runTest(t, func(buf *Buffer, tablet *topodatapb.Tablet, keyspace, shard string, now time.Time) {
			buf.HandleKeyspaceEvent(&discovery.KeyspaceEvent{
				Keyspace: keyspace,
				Shards: []discovery.ShardEvent{
					{
						Tablet:  tablet.Alias,
						Target:  &query.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
						Serving: true,
					},
				},
			})
		})
	})
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
		got, want := sb.testGetSize(), count
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
		got := sb.testGetState()
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
		got := b.bufferSize
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("not all pool slots were returned: got = %v, want = %v", got, want)
		}
		time.Sleep(1 * time.Millisecond)
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
