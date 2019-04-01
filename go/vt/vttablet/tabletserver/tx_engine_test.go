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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"

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
	te.open()
	start := time.Now()
	te.close(false)
	if diff := time.Since(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with timeout wait.
	te.open()
	c, err := te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.close(false)
	if diff := time.Since(start); diff < 500*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.5s", diff)
	}

	// Immediate close.
	te.open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.close(true)
	if diff := time.Since(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}

	// Normal close with short grace period.
	te.shutdownGracePeriod = 250 * time.Millisecond
	te.open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Recycle()
	start = time.Now()
	te.close(false)
	if diff := time.Since(start); diff > 500*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.5s", diff)
	}
	if diff := time.Since(start); diff < 250*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.25s", diff)
	}

	// Normal close with short grace period, but pool gets empty early.
	te.shutdownGracePeriod = 250 * time.Millisecond
	te.open()
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
	te.close(false)
	if diff := time.Since(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Since(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}

	// Immediate close, but connection is in use.
	te.open()
	c, err = te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		te.txPool.LocalConclude(ctx, c)
	}()
	start = time.Now()
	te.close(true)
	if diff := time.Since(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Since(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}
}

type TxType int

const (
	NoTx TxType = iota
	ReadOnlyAccepted
	WriteAccepted
	ReadOnlyRejected
	WriteRejected
)

func (t TxType) String() string {
	names := [...]string{
		"no transaction",
		"read only transaction accepted",
		"write transaction accepted",
		"read only transaction rejected",
		"write transaction rejected",
	}

	if t < NoTx || t > WriteRejected {
		return "unknown"
	}

	return names[t]
}

type TestCase struct {
	startState     txEngineState
	TxEngineStates []txEngineState
	tx             TxType
	stateAssertion func(state txEngineState) error
}

func (test TestCase) String() string {
	var sb strings.Builder
	sb.WriteString("start from ")
	sb.WriteString(test.startState.String())
	sb.WriteString(" with ")
	sb.WriteString(test.tx.String())

	for _, change := range test.TxEngineStates {
		sb.WriteString(" change state to ")
		sb.WriteString(change.String())
	}

	return sb.String()
}

func changeState(te *TxEngine, state txEngineState) error {
	switch state {
	case AcceptingReadAndWrite:
		return te.AcceptReadWrite()
	case AcceptingReadOnly:
		return te.AcceptReadOnly()
	case NotServing:
		return te.Stop()
	default:
		return fmt.Errorf("don't know how to do that: %v", state)
	}
}

func TestWithInnerTests(outerT *testing.T) {

	tests := []TestCase{
		// Start from RW and test all single hop transitions with and without tx
		{AcceptingReadAndWrite, []txEngineState{
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite},
			WriteAccepted, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing},
			ReadOnlyAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite},
			ReadOnlyAccepted, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly},
			ReadOnlyAccepted, assertEndStateIs(AcceptingReadOnly)},

		// Start from RW and test all transitions with and without tx, plus a concurrent Stop()
		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			WriteAccepted, assertEndStateIs(NotServing)},

		// Start from RW and test all transitions with and without tx, plus a concurrent ReadOnly()
		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			NotServing,
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadAndWrite, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadOnly},
			WriteAccepted, assertEndStateIs(AcceptingReadOnly)},

		// Start from RO and test all single hop transitions with and without tx
		{AcceptingReadOnly, []txEngineState{
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly},
			NoTx, assertEndStateIs(AcceptingReadOnly)},

		{AcceptingReadOnly, []txEngineState{
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly},
			WriteRejected, assertEndStateIs(AcceptingReadOnly)},

		// Start from RO and test all transitions with and without tx, plus a concurrent Stop()
		{AcceptingReadOnly, []txEngineState{
			NotServing,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			NoTx, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			NotServing,
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			NotServing},
			WriteRejected, assertEndStateIs(NotServing)},

		// Start from RO and test all transitions with and without tx, plus a concurrent ReadWrite()
		{AcceptingReadOnly, []txEngineState{
			NotServing,
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadAndWrite},
			NoTx, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			NotServing,
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadAndWrite,
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		{AcceptingReadOnly, []txEngineState{
			AcceptingReadOnly,
			AcceptingReadAndWrite},
			WriteRejected, assertEndStateIs(AcceptingReadAndWrite)},

		// Make sure that all transactions are rejected when we are not serving
		{NotServing, []txEngineState{},
			WriteRejected, assertEndStateIs(NotServing)},

		{NotServing, []txEngineState{},
			ReadOnlyRejected, assertEndStateIs(NotServing)},
	}

	for _, test := range tests {
		outerT.Run(test.String(), func(t *testing.T) {

			db := setUpQueryExecutorTest(t)
			db.AddQuery("set transaction isolation level REPEATABLE READ", &sqltypes.Result{})
			db.AddQuery("start transaction with consistent snapshot, read only", &sqltypes.Result{})
			defer db.Close()
			te := setupTxEngine(db)

			failIfError(t,
				changeState(te, test.startState))

			switch test.tx {
			case NoTx:
				// nothing to do
			case WriteAccepted:
				failIfError(t,
					startTransaction(te, true))
			case ReadOnlyAccepted:
				failIfError(t,
					startTransaction(te, false))
			case WriteRejected:
				err := startTransaction(te, true)
				if err == nil {
					t.Fatalf("expected an error to be returned when opening write transaction, but got nil")
				}
			case ReadOnlyRejected:
				err := startTransaction(te, false)
				if err == nil {
					t.Fatalf("expected an error to be returned when opening read transaction, but got nil")
				}
			default:
				t.Fatalf("don't know how to [%v]", test.tx)
			}

			wg := sync.WaitGroup{}
			for _, newState := range test.TxEngineStates {
				wg.Add(1)
				go func(s txEngineState) {
					defer wg.Done()

					failIfError(t,
						changeState(te, s))
				}(newState)

				// We give the state changes a chance to get started
				time.Sleep(10 * time.Millisecond)
			}

			// Let's wait for all transitions to wrap up
			wg.Wait()

			failIfError(t,
				test.stateAssertion(te.state))
		})
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

func assertEndStateIs(expected txEngineState) func(actual txEngineState) error {
	return func(actual txEngineState) error {
		if actual != expected {
			return fmt.Errorf("expected the end state to be %v, but it was %v", expected, actual)
		}
		return nil
	}
}

func startTransaction(te *TxEngine, writeTransaction bool) error {
	options := &querypb.ExecuteOptions{}
	if writeTransaction {
		options.TransactionIsolation = querypb.ExecuteOptions_DEFAULT
	} else {
		options.TransactionIsolation = querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY
	}
	_, err := te.Begin(context.Background(), options)
	return err
}
