// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD_style
// license that can be found in the LICENSE file.

/*
Package txbuffer contains experimental logic to buffer transactions in VTGate.
Only the Begin statement of transactions will be buffered.

The reason why it might be useful to buffer transactions is during failovers:
the master vttablet can become unavailable for a few seconds. Upstream clients
(e.g., web workers) might not retry on failures, and instead may prefer for VTGate to wait for
a few seconds for the failover to complete. Thiis will block upstream callers for that time,
but will not return transient errors during the buffering time.
*/
package txbuffer

import (
	"flag"
	"sync"
	"time"

	"github.com/youtube/vitess/go/stats"
)

var (
	enableFakeTxBuffer = flag.Bool("enable_fake_tx_buffer", false, "Enable fake transaction buffering.")
	bufferKeyspace     = flag.String("buffer_keyspace", "", "The name of the keyspace to buffer transactions on.")
	bufferShard        = flag.String("buffer_shard", "", "The name of the shard to buffer transactions on.")
	maxBufferSize      = flag.Int("max_buffer_size", 10, "The maximum number of transactions to buffer at a time.")
	fakeBufferDelay    = flag.Duration("fake_buffer_delay", 1*time.Second, "The amount of time that we should delay all transactions for, to fake a transaction buffer.")

	bufferedTransactionsAttempted  = stats.NewInt("BufferedTransactionsAttempted")
	bufferedTransactionsSuccessful = stats.NewInt("BufferedTransactionsSuccessful")
	// Use this lock when adding to the number of currently buffered transactions.
	bufferMu             sync.Mutex
	bufferedTransactions = stats.NewInt("BufferedTransactions")
)

// timeSleep can be mocked out in unit tests
var timeSleep = time.Sleep

// FakeBuffer will pretend to buffer new transactions in VTGate.
// Transactions *will NOT actually be buffered*, they will just have a delayed start time.
// This can be useful to understand what the impact of trasaction buffering will be
// on upstream callers. Once the impact is measured, it can be used to tweak parameter values
// for the best behavior.
// FakeBuffer should be called before the VtTablet Begin, otherwise it will increase transaction times.
func FakeBuffer(keyspace, shard string, attemptNumber int) {
	// Only buffer on the first Begin attempt, not on possible retries.
	if !*enableFakeTxBuffer || attemptNumber != 0 {
		return
	}
	if keyspace != *bufferKeyspace || shard != *bufferShard {
		return
	}
	bufferedTransactionsAttempted.Add(1)

	bufferMu.Lock()
	if int(bufferedTransactions.Get()) >= *maxBufferSize {
		bufferMu.Unlock()
		return
	}
	bufferedTransactions.Add(1)
	bufferMu.Unlock()

	defer bufferedTransactionsSuccessful.Add(1)
	timeSleep(*fakeBufferDelay)
	// Don't need to lock for this, as there's no race when decrementing the count
	bufferedTransactions.Add(-1)
}
