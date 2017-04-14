// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD_style
// license that can be found in the LICENSE file.

/*
Package masterbuffer contains experimental logic to buffer master requests in VTGate.
Only statements outside of transactinos will be buffered (including the initial Begin
to start a transaction).

The reason why it might be useful to buffer master requests is during failovers:
the master vttablet can become unavailable for a few seconds. Upstream clients
(e.g., web workers) might not retry on failures, and instead may prefer for VTGate to wait for
a few seconds for the failover to complete. Thiis will block upstream callers for that time,
but will not return transient errors during the buffering time.
*/
package masterbuffer

import (
	"flag"
	"sync"
	"time"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	enableFakeMasterBuffer = flag.Bool("enable_fake_master_buffer", false, "Enable fake master buffering.")
	bufferKeyspace         = flag.String("buffer_keyspace", "", "The name of the keyspace to buffer master requests on.")
	bufferShard            = flag.String("buffer_shard", "", "The name of the shard to buffer master requests on.")
	maxBufferSize          = flag.Int("max_buffer_size", 10, "The maximum number of master requests to buffer at a time.")
	fakeBufferDelay        = flag.Duration("fake_buffer_delay", 1*time.Second, "The amount of time that we should delay all master requests for, to fake a buffer.")

	bufferedRequestsAttempted  = stats.NewInt("BufferedRequestsAttempted")
	bufferedRequestsSuccessful = stats.NewInt("BufferedRequestsSuccessful")
	// Use this lock when adding to the number of currently buffered requests.
	bufferMu         sync.Mutex
	bufferedRequests = stats.NewInt("BufferedRequests")
)

// timeSleep can be mocked out in unit tests
var timeSleep = time.Sleep

// errBufferFull is the error returned a buffer request is rejected because the buffer is full.
var errBufferFull = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "master request buffer full, rejecting request")

// FakeBuffer will pretend to buffer master requests in VTGate.
// Requests *will NOT actually be buffered*, they will just be delayed.
// This can be useful to understand what the impact of master request buffering will be
// on upstream callers. Once the impact is measured, it can be used to tweak parameter values
// for the best behavior.
// FakeBuffer should be called before a potential VtTablet Begin, otherwise it will increase transaction times.
func FakeBuffer(target *querypb.Target, inTransaction bool, attemptNumber int) error {
	if !*enableFakeMasterBuffer {
		return nil
	}
	// Don't buffer non-master traffic, requests that are inside transactions, or retries.
	if target.TabletType != topodatapb.TabletType_MASTER || inTransaction || attemptNumber != 0 {
		return nil
	}
	if target.Keyspace != *bufferKeyspace || target.Shard != *bufferShard {
		return nil
	}
	bufferedRequestsAttempted.Add(1)

	bufferMu.Lock()
	if int(bufferedRequests.Get()) >= *maxBufferSize {
		bufferMu.Unlock()
		return errBufferFull
	}
	bufferedRequests.Add(1)
	bufferMu.Unlock()

	defer bufferedRequestsSuccessful.Add(1)
	timeSleep(*fakeBufferDelay)
	// Don't need to lock for this, as there's no race when decrementing the count
	bufferedRequests.Add(-1)
	return nil
}
