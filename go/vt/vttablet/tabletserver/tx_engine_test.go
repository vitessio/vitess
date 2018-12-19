/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"testing"
	"time"
	"vitess.io/vitess/go/mysql/fakesqldb"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"golang.org/x/net/context"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTxEngineClose(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	testUtils := newTestUtils()
	dbcfgs := testUtils.newDBConfigs(db)
	ctx := context.Background()
	config := tabletenv.DefaultQsConfig
	config.TransactionCap = 10
	config.TransactionTimeout = 0.5
	config.TxShutDownGracePeriod = 0
	te := NewTxEngine(nil, config)
	te.InitDBConfig(dbcfgs)

	// Normal close.
	te.Open()
	start := time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with timeout wait.
	te.Open()
	c, err := te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff < 500*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.5s", diff)
	}

	// Immediate close.
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.Close(true)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with short grace period.
	te.shutdownGracePeriod = 250 * time.Millisecond
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}
	if diff := time.Now().Sub(start); diff < 250*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.25s", diff)
	}

	// Normal close with short grace period, but pool gets empty early.
	te.shutdownGracePeriod = 250 * time.Millisecond
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, err := te.txPool.Get(c.TransactionID, "return")
		if err != nil {
			t.Error(err)
		}
		te.txPool.LocalConclude(ctx, c)
	}()
	start = time.Now()
	te.Close(false)
	if diff := time.Now().Sub(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Now().Sub(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}

	// Immediate close, but connection is in use.
	te.Open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		te.txPool.LocalConclude(ctx, c)
	}()
	start = time.Now()
	te.Close(true)
	if diff := time.Now().Sub(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Now().Sub(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}
}

func TestTxEngineStopMasterWithTx(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	start := time.Now()

	failIfError(t,
		te.AcceptReadWrite())

	startTransaction(te, t)

	start = time.Now()
	failIfError(t,
		te.Stop())

	go func() {
		// Stop should be idempotent, so let's fire off a second stop request
		time.Sleep(10 * time.Millisecond)
		started := time.Now()
		failIfError(t,
			te.Stop())
		assertWasInstant(started, t)
	}()

	assertTookMoreThan(start, t, 500*time.Millisecond)
}

func TestTxEngineStopWhenNotStarted(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	// Closing down before even starting
	start := time.Now()
	failIfError(t, te.Stop())
	assertWasInstant(start, t)
}

func TestTxEngineStopMasterWithNoTx(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	start := time.Now()
	failIfError(t,
		te.AcceptReadWrite())

	start = time.Now()
	failIfError(t,
		te.Stop())
	assertTookLessThan(start, t, 500*time.Millisecond)
}

func TestTxEngineStopNonMasterNoTransactions(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	start := time.Now()
	failIfError(t,
		te.AcceptReadOnly())

	start = time.Now()
	failIfError(t,
		te.Stop())
	assertTookLessThan(start, t, 500*time.Millisecond)
}

func TestTxEngineStopNonMasterWithTransaction(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	start := time.Now()
	failIfError(t,
		te.AcceptReadOnly())

	startTransaction(te, t)

	start = time.Now()
	failIfError(t,
		te.Stop())
	assertTookLessThan(start, t, 500*time.Millisecond)
}

func TestTxEngineTransitionFromMasterToNonMaster(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	failIfError(t,
		te.AcceptReadWrite())

	start := time.Now()
	failIfError(t,
		te.AcceptReadOnly())

	assertTookLessThan(start, t, 500*time.Millisecond)
}

func TestTxEngineTransitionFromMasterToNonMasterWithTransactions(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	failIfError(t,
		te.AcceptReadWrite())

	startTransaction(te, t)

	start := time.Now()
	failIfError(t,
		te.Stop())
	assertTookMoreThan(start, t, 500*time.Millisecond)
}

func TestTxEngineTransitionFromRW2RO2NS(t *testing.T) {
	// This test tries to test the situation when we have a
	// RW engine being asked to go RO, and before that being has finished,
	// is asked to stop serving. The expected end state is NotServing

	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	failIfError(t,
		te.AcceptReadWrite())

	startTransaction(te, t)

	// In a different goroutine we'll fire the ReadOnly signal so we don't block the testing routine
	go func() {
		failIfError(t,
			te.AcceptReadOnly())
	}()

	// We give the ReadOnly state change a chance to get started
	time.Sleep(10*time.Millisecond)

	failIfError(t,
		te.Stop())

	failIfError(t,
		te.BlockUntilEndOfTransition(context.Background()))

	if te.state != NotServing {
		t.Errorf("expected the end state to be not serving, but it was %v", te.state)
		t.FailNow()
	}
}

func TestTxEngineTransitionFromRW2RO2RW(t *testing.T) {
	// This test tries to test the situation when we have a
	// RW engine being asked to go RO, and before that being has finished,
	// is asked to start serving read/write transactions again. The expected
	// end state is RW

	db := setUpQueryExecutorTest(t)
	defer db.Close()
	te := setupTxEngine(db)

	failIfError(t,
		te.AcceptReadWrite())

	startTransaction(te, t)

	// In a different goroutine we'll fire the ReadOnly signal so we don't block the testing routine
	go func() {
		failIfError(t,
			te.AcceptReadOnly())
	}()

	// We give the ReadOnly state change a chance to get started
	time.Sleep(10*time.Millisecond)

	failIfError(t,
		te.AcceptReadWrite())

	failIfError(t,
		te.BlockUntilEndOfTransition(context.Background()))

	if te.state != AcceptingReadAndWrite {
		t.Errorf("expected the end state to be not serving, but it was %v", te.state)
		t.FailNow()
	}
}

func assertWasInstant(start time.Time, t *testing.T) {
	if diff := time.Now().Sub(start); diff > 1*time.Millisecond {
		t.Errorf("Close time: %v, must be under 1 ms", diff)
	}
}

func setupTxEngine(db *fakesqldb.DB) *TxEngine {
	testUtils := newTestUtils()
	dbcfgs := testUtils.newDBConfigs(db)
	config := tabletenv.DefaultQsConfig
	config.TransactionCap = 10
	config.TransactionTimeout = 0.5
	config.TxShutDownGracePeriod = 0
	te := NewTxEngine(nil, config)
	te.InitDBConfig(dbcfgs)
	return te
}

func failIfError(t *testing.T, err error) {
	if err != nil {
		t.Logf("%+v", err)
		t.FailNow()
	}
}

func assertTookLessThan(start time.Time, t *testing.T, duration time.Duration) {
	if diff := time.Now().Sub(start); diff > duration {
		t.Errorf("Stop time: %v, must be under 0.5s", diff)
	}
}
func assertTookMoreThan(start time.Time, t *testing.T, duration time.Duration) {
	if diff := time.Now().Sub(start); diff < duration {
		t.Errorf("Stop time: %v, must be over 0.5s", diff)
	}
}

func startTransaction(te *TxEngine, t *testing.T) {
	c, err := te.txPool.LocalBegin(context.Background(), &querypb.ExecuteOptions{})
	failIfError(t, err)
	c.Recycle()
}
