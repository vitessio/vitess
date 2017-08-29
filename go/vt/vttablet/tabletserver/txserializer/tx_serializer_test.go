/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package txserializer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vterrors"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func resetVariables() {
	waits.Reset()
	waitsDryRun.Reset()
	queueExceeded.Reset()
	queueExceededDryRun.Reset()
	globalQueueExceeded.Set(0)
	globalQueueExceededDryRun.Set(0)
}

func TestTxSerializer_NoHotRow(t *testing.T) {
	resetVariables()
	txs := New(false, 1, 1, 5)

	done, waited, err := txs.Wait(context.Background(), "t1 where1", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if waited {
		t.Fatal("non-parallel tx must never wait")
	}
	done()

	// No hot row was recoded.
	if err := testHTTPHandler(txs, 0); err != nil {
		t.Fatal(err)
	}
	// No transaction had to wait.
	if got, want := waits.Counts()["t1"], int64(0); got != want {
		t.Fatalf("wrong Waits variable: got = %v, want = %v", got, want)
	}
}

func TestTxSerializer(t *testing.T) {
	resetVariables()
	txs := New(false, 2, 3, 1)

	// tx1.
	done1, waited1, err1 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err1 != nil {
		t.Fatal(err1)
	}
	if waited1 {
		t.Fatalf("tx1 must never wait: %v", waited1)
	}

	// tx2 (gets queued and must wait).
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		done2, waited2, err2 := txs.Wait(context.Background(), "t1 where1", "t1")
		if err2 != nil {
			t.Fatal(err2)
		}
		if !waited2 {
			t.Fatalf("tx2 must wait: %v", waited2)
		}
		if got, want := waits.Counts()["t1"], int64(1); got != want {
			t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
		}

		done2()
	}()
	// Wait until tx2 is waiting before we try tx3.
	if err := waitForPending(txs, "t1 where1", 2); err != nil {
		t.Fatal(err)
	}

	// tx3 (gets rejected because it would exceed the local queue).
	_, _, err3 := txs.Wait(context.Background(), "t1 where1", "t1")
	if got, want := vterrors.Code(err3), vtrpcpb.Code_RESOURCE_EXHAUSTED; got != want {
		t.Fatalf("wrong error code: got = %v, want = %v", got, want)
	}
	if got, want := err3.Error(), "hot row protection: too many queued transactions (2 >= 2) for the same row (table + WHERE clause: 't1 where1')"; got != want {
		t.Fatalf("transaction rejected with wrong error: got = %v, want = %v", got, want)
	}

	done1()
	// tx2 must have been unblocked.
	wg.Wait()

	if txs.queues["t1 where1"] != nil {
		t.Fatal("queue object was not deleted after last transaction")
	}

	// 2 transactions were recorded.
	if err := testHTTPHandler(txs, 2); err != nil {
		t.Fatal(err)
	}
	// 1 of them had to wait.
	if got, want := waits.Counts()["t1"], int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}
	// 1 (the third one) was rejected because the queue was exceeded.
	if got, want := queueExceeded.Counts()["t1"], int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}
}

func TestTxSerializer_ConcurrentTransactions(t *testing.T) {
	resetVariables()
	// Allow up to 2 concurrent transactions per hot row.
	txs := New(false, 3, 3, 2)

	// tx1.
	done1, waited1, err1 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err1 != nil {
		t.Fatal(err1)
	}
	if waited1 {
		t.Fatalf("tx1 must never wait: %v", waited1)
	}

	// tx2.
	done2, waited2, err2 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err2 != nil {
		t.Fatal(err1)
	}
	if waited2 {
		t.Fatalf("tx2 must not wait: %v", waited1)
	}

	// tx3 (gets queued and must wait).
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		done3, waited3, err3 := txs.Wait(context.Background(), "t1 where1", "t1")
		if err3 != nil {
			t.Fatal(err3)
		}
		if !waited3 {
			t.Fatalf("tx3 must wait: %v", waited2)
		}
		if got, want := waits.Counts()["t1"], int64(1); got != want {
			t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
		}

		done3()
	}()

	// Wait until tx3 is waiting before we finish tx2 and unblock tx3.
	if err := waitForPending(txs, "t1 where1", 3); err != nil {
		t.Fatal(err)
	}
	// Finish tx2 before tx1 to test that the "finish-order" does not matter.
	// Unblocks tx3.
	done2()
	// Wait for tx3 to finish.
	wg.Wait()
	// Finish tx1 to delete the queue object.
	done1()

	if txs.queues["t1 where1"] != nil {
		t.Fatal("queue object was not deleted after last transaction")
	}

	// 3 transactions were recorded.
	if err := testHTTPHandler(txs, 3); err != nil {
		t.Fatal(err)
	}
	// 1 of them had to wait.
	if got, want := waits.Counts()["t1"], int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}
}

func waitForPending(txs *TxSerializer, key string, i int) error {
	start := time.Now()
	for {
		got, want := txs.Pending(key), i
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("wait for TxSerializer.Pending() = %d timed out: got = %v, want = %v", i, got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func testHTTPHandler(txs *TxSerializer, count int) error {
	req, err := http.NewRequest("GET", "/path-is-ignored-in-test", nil)
	if err != nil {
		return err
	}
	rr := httptest.NewRecorder()
	txs.ServeHTTP(rr, req)

	if got, want := rr.Code, http.StatusOK; got != want {
		return fmt.Errorf("wrong status code: got = %v, want = %v", got, want)
	}
	want := fmt.Sprintf(`Length: 1
%d: t1 where1
`, count)
	if count == 0 {
		want = `Length: 0
`
	}
	if got := rr.Body.String(); got != want {
		return fmt.Errorf("wrong content: got = \n%v\n want = \n%v", got, want)
	}

	return nil
}

// TestTxSerializerCancel runs 4 pending transactions.
// tx1 and tx2 are allowed to run concurrently while tx3 and tx4 are queued.
// tx3 will get canceled and tx4 will be unblocked once tx1 is done.
func TestTxSerializerCancel(t *testing.T) {
	resetVariables()
	txs := New(false, 4, 4, 2)

	// tx3 and tx4 will record their number once they're done waiting.
	txDone := make(chan int)

	// tx1.
	done1, waited1, err1 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err1 != nil {
		t.Fatal(err1)
	}
	if waited1 {
		t.Fatalf("tx1 must never wait: %v", waited1)
	}
	// tx2.
	done2, waited2, err2 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err2 != nil {
		t.Fatal(err2)
	}
	if waited2 {
		t.Fatalf("tx2 must not wait: %v", waited2)
	}

	// tx3 (gets queued and must wait).
	ctx3, cancel3 := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, _, err3 := txs.Wait(ctx3, "t1 where1", "t1")
		if err3 != context.Canceled {
			t.Fatal(err3)
		}

		txDone <- 3
	}()
	// Wait until tx3 is waiting before we try tx4.
	if err := waitForPending(txs, "t1 where1", 3); err != nil {
		t.Fatal(err)
	}

	// tx4 (gets queued and must wait as well).
	wg.Add(1)
	go func() {
		defer wg.Done()

		done4, waited4, err4 := txs.Wait(context.Background(), "t1 where1", "t1")
		if err4 != nil {
			t.Fatal(err4)
		}
		if !waited4 {
			t.Fatalf("tx4 must have waited: %v", waited4)
		}

		txDone <- 4

		done4()
	}()
	// Wait until tx4 is waiting before we start to cancel tx3.
	if err := waitForPending(txs, "t1 where1", 4); err != nil {
		t.Fatal(err)
	}

	// Cancel tx3.
	cancel3()
	if got := <-txDone; got != 3 {
		t.Fatalf("tx3 should have been unblocked after the cancel: %v", got)
	}
	// Finish tx1.
	done1()
	// Wait for tx4.
	if got := <-txDone; got != 4 {
		t.Fatalf("wrong tx was unblocked after tx1: %v", got)
	}
	wg.Wait()
	// Finish tx2 (the last transaction) which will delete the queue object.
	done2()

	if txs.queues["t1 where1"] != nil {
		t.Fatal("queue object was not deleted after last transaction")
	}

	// 4 total transactions get recorded.
	if err := testHTTPHandler(txs, 4); err != nil {
		t.Fatal(err)
	}
	// 2 of them had to wait.
	if got, want := waits.Counts()["t1"], int64(2); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}
}

// TestTxSerializerDryRun verifies that the dry-run mode does not serialize
// the two concurrent transactions for the same key.
func TestTxSerializerDryRun(t *testing.T) {
	resetVariables()
	txs := New(true, 1, 2, 1)

	// tx1.
	done1, waited1, err1 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err1 != nil {
		t.Fatal(err1)
	}
	if waited1 {
		t.Fatalf("first transaction must never wait: %v", waited1)
	}

	// tx2 (would wait and exceed the local queue).
	done2, waited2, err2 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err2 != nil {
		t.Fatal(err2)
	}
	if waited2 {
		t.Fatalf("second transaction must never wait in dry-run mode: %v", waited2)
	}
	if got, want := waitsDryRun.Counts()["t1"], int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}
	if got, want := queueExceededDryRun.Counts()["t1"], int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}

	// tx3 (would wait and exceed the global queue).
	done3, waited3, err3 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err3 != nil {
		t.Fatal(err3)
	}
	if waited3 {
		t.Fatalf("any transaction must never wait in dry-run mode: %v", waited3)
	}
	if got, want := waitsDryRun.Counts()["t1"], int64(2); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}
	if got, want := globalQueueExceededDryRun.Get(), int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}

	if got, want := txs.Pending("t1 where1"), 3; got != want {
		t.Fatalf("wrong number of pending transactions: got = %v, want = %v", got, want)
	}

	done1()
	done2()
	done3()

	if txs.queues["t1 where1"] != nil {
		t.Fatal("queue object was not deleted after last transaction")
	}

	if err := testHTTPHandler(txs, 3); err != nil {
		t.Fatal(err)
	}
}

// TestTxSerializerGlobalQueueOverflow shows that the global queue can exceed
// its limit without rejecting errors. This is the case when all transactions
// are the first first one for their row range.
// This is done on purpose to avoid that a too low global queue limit would
// reject transactions although they may succeed within the txpool constraints
// and RPC deadline.
func TestTxSerializerGlobalQueueOverflow(t *testing.T) {
	txs := New(false, 1, 1 /* maxGlobalQueueSize */, 1)

	// tx1.
	done1, waited1, err1 := txs.Wait(context.Background(), "t1 where1", "t1")
	if err1 != nil {
		t.Fatal(err1)
	}
	if waited1 {
		t.Fatalf("first transaction must never wait: %v", waited1)
	}

	// tx2.
	done2, waited2, err2 := txs.Wait(context.Background(), "t1 where2", "t1")
	if err2 != nil {
		t.Fatal(err2)
	}
	if waited2 {
		t.Fatalf("second transaction for different row range must not wait: %v", waited2)
	}

	// tx3 (same row range as tx1).
	_, _, err3 := txs.Wait(context.Background(), "t1 where1", "t1")
	if got, want := vterrors.Code(err3), vtrpcpb.Code_RESOURCE_EXHAUSTED; got != want {
		t.Fatalf("wrong error code: got = %v, want = %v", got, want)
	}
	if got, want := err3.Error(), "hot row protection: too many queued transactions (2 >= 1)"; got != want {
		t.Fatalf("transaction rejected with wrong error: got = %v, want = %v", got, want)
	}
	if got, want := globalQueueExceeded.Get(), int64(1); got != want {
		t.Fatalf("variable not incremented: got = %v, want = %v", got, want)
	}

	done1()
	done2()
}

func TestTxSerializerPending(t *testing.T) {
	txs := New(false, 1, 1, 1)
	if got, want := txs.Pending("t1 where1"), 0; got != want {
		t.Fatalf("there should be no pending transaction: got = %v, want = %v", got, want)
	}
}

func BenchmarkTxSerializer_NoHotRow(b *testing.B) {
	txs := New(false, 1, 1, 5)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		done, waited, err := txs.Wait(context.Background(), "t1 where1", "t1")
		if err != nil {
			b.Fatal(err)
		}
		if waited {
			b.Fatal("non-parallel tx must never wait")
		}
		done()
	}
}
