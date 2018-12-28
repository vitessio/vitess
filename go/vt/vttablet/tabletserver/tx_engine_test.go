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
	te.close(false)
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
	te.close(false)
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
	te.close(true)
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
	te.close(false)
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
	te.close(false)
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
	te.close(true)
	if diff := time.Now().Sub(start); diff > 250*time.Millisecond {
		t.Errorf("Close time: %v, must be under 0.25s", diff)
	}
	if diff := time.Now().Sub(start); diff < 100*time.Millisecond {
		t.Errorf("Close time: %v, must be over 0.1", diff)
	}
}

type StateChange struct {
	newState      TxEngineState
	timeAssertion func(startTime time.Time) error
}

type TestCase struct {
	startState     TxEngineState
	stateChanges   []StateChange
	useTransaction bool
	stateAssertion func(state TxEngineState) error
}

func (test TestCase) String() string {
	var sb strings.Builder
	sb.WriteString("start from ")
	sb.WriteString(test.startState.String())
	sb.WriteString(" with")
	if !test.useTransaction {
		sb.WriteString("out")
	}

	sb.WriteString(" transaction")

	for _, change := range test.stateChanges {
		sb.WriteString(" change state to ")
		sb.WriteString(change.newState.String())
	}

	return sb.String()
}

func changeState(te *TxEngine, state TxEngineState) error {
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

	const RW = AcceptingReadAndWrite
	const RO = AcceptingReadOnly
	const NS = NotServing
	const WithTX = true
	const NoTx = false

	tests := []TestCase{
		// Start from RW and test all single hop transitions with and without tx
		{RW, []StateChange{{NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RW, []StateChange{{RW, assertIsInstant()}}, NoTx, assertEndStateIs(RW),},
		{RW, []StateChange{{RO, assertIsInstant()}}, NoTx, assertEndStateIs(RO),},
		{RW, []StateChange{{NS, assertTakesTime()}}, WithTX, assertEndStateIs(NS),},
		{RW, []StateChange{{RW, assertIsInstant()}}, WithTX, assertEndStateIs(RW),},
		{RW, []StateChange{{RO, assertTakesTime()}}, WithTX, assertEndStateIs(RO),},

		// Start from RW and test all transitions with and without tx, plus a concurrent Stop()
		{RW, []StateChange{{NS, assertIsInstant()}, {NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RW, []StateChange{{RW, assertIsInstant()}, {NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RW, []StateChange{{RO, assertIsInstant()}, {NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RW, []StateChange{{NS, assertTakesTime()}, {NS, assertTakesTime()}}, WithTX, assertEndStateIs(NS),},
		{RW, []StateChange{{RW, assertIsInstant()}, {NS, assertTakesTime()}}, WithTX, assertEndStateIs(NS),},
		{RW, []StateChange{{RO, assertTakesTime()}, {NS, assertTakesTime()}}, WithTX, assertEndStateIs(NS),},

		// Start from RW and test all transitions with and without tx, plus a concurrent ReadOnly()
		{RW, []StateChange{{NS, assertIsInstant()}, {RO, assertIsInstant()}}, NoTx, assertEndStateIs(RO),},
		{RW, []StateChange{{RW, assertIsInstant()}, {RO, assertIsInstant()}}, NoTx, assertEndStateIs(RO),},
		{RW, []StateChange{{RO, assertIsInstant()}, {RO, assertIsInstant()}}, NoTx, assertEndStateIs(RO),},
		{RW, []StateChange{{NS, assertTakesTime()}, {RO, assertTakesTime()}}, WithTX, assertEndStateIs(RO),},
		{RW, []StateChange{{RW, assertIsInstant()}, {RO, assertTakesTime()}}, WithTX, assertEndStateIs(RO),},
		{RW, []StateChange{{RO, assertTakesTime()}, {RO, assertTakesTime()}}, WithTX, assertEndStateIs(RO),},

		// Start from RO and test all single hop transitions with and without tx
		{RO, []StateChange{{NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RO, []StateChange{{RW, assertIsInstant()}}, NoTx, assertEndStateIs(RW),},
		{RO, []StateChange{{RO, assertIsInstant()}}, NoTx, assertEndStateIs(RO),},
		{RO, []StateChange{{NS, assertIsInstant()}}, WithTX, assertEndStateIs(NS),},
		{RO, []StateChange{{RW, assertIsInstant()}}, WithTX, assertEndStateIs(RW),},
		{RO, []StateChange{{RO, assertIsInstant()}}, WithTX, assertEndStateIs(RO),},

		// Start from RO and test all transitions with and without tx, plus a concurrent Stop()
		{RO, []StateChange{{NS, assertIsInstant()}, {NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RO, []StateChange{{RW, assertIsInstant()}, {NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RO, []StateChange{{RO, assertIsInstant()}, {NS, assertIsInstant()}}, NoTx, assertEndStateIs(NS),},
		{RO, []StateChange{{NS, assertIsInstant()}, {NS, assertIsInstant()}}, WithTX, assertEndStateIs(NS),},
		{RO, []StateChange{{RW, assertIsInstant()}, {NS, assertIsInstant()}}, WithTX, assertEndStateIs(NS),},
		{RO, []StateChange{{RO, assertIsInstant()}, {NS, assertIsInstant()}}, WithTX, assertEndStateIs(NS),},

		// Start from RO and test all transitions with and without tx, plus a concurrent ReadWrite()
		{RO, []StateChange{{NS, assertIsInstant()}, {RW, assertIsInstant()}}, NoTx, assertEndStateIs(RW),},
		{RO, []StateChange{{RW, assertIsInstant()}, {RW, assertIsInstant()}}, NoTx, assertEndStateIs(RW),},
		{RO, []StateChange{{RO, assertIsInstant()}, {RW, assertIsInstant()}}, NoTx, assertEndStateIs(RW),},
		{RO, []StateChange{{NS, assertIsInstant()}, {RW, assertIsInstant()}}, WithTX, assertEndStateIs(RW),},
		{RO, []StateChange{{RW, assertIsInstant()}, {RW, assertIsInstant()}}, WithTX, assertEndStateIs(RW),},
		{RO, []StateChange{{RO, assertIsInstant()}, {RW, assertIsInstant()}}, WithTX, assertEndStateIs(RW),},
	}

	for _, test := range tests {
		outerT.Run(test.String(), func(t *testing.T) {
			db := setUpQueryExecutorTest(t)
			defer db.Close()
			te := setupTxEngine(db)

			failIfError(t,
				changeState(te, test.startState))

			if test.useTransaction {
				startTransaction(te, t)
			}

			for _, change := range test.stateChanges {
				go func() {
					start := time.Now()

					failIfError(t,
						changeState(te, change.newState))

					ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))

					failIfError(t,
						te.BlockUntilEndOfTransition(ctx))

					cancel()

					failIfError(t,
						change.timeAssertion(start))
				}()

				// We give the state changes a chance to get started
				time.Sleep(10 * time.Millisecond)
			}

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))

			failIfError(t,
				te.BlockUntilEndOfTransition(ctx))

			cancel()

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

func assertIsInstant() func(start time.Time) error {
	return func(start time.Time) error {
		if diff := time.Now().Sub(start); diff > time.Millisecond {
			return fmt.Errorf("stop time: %v, must be instant", diff)
		}
		return nil
	}
}
func assertTakesTime() func(start time.Time) error {
	return func(start time.Time) error {
		if diff := time.Now().Sub(start); diff < 500*time.Millisecond {
			return fmt.Errorf("stop time: %v, should take at least half a second", diff)
		}
		return nil
	}
}

func assertEndStateIs(expected TxEngineState) func(actual TxEngineState) error {
	return func(actual TxEngineState) error {
		if actual != expected {
			return fmt.Errorf("expected the end state to be %v, but it was %v", expected, actual)
		}
		return nil
	}
}

func startTransaction(te *TxEngine, t *testing.T) {
	c, err := te.txPool.LocalBegin(context.Background(), &querypb.ExecuteOptions{})
	failIfError(t, err)
	c.Recycle()
}
