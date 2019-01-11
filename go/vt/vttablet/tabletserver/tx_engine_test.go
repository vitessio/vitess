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

type TxType int

const (
	NoTx TxType = iota
	ReTx
	WrTx
)

func (t TxType) String() string {
	names := [...]string{
		"no",
		"read only",
		"write",}

	if t < NoTx || t > WrTx {
		return "unknown"
	}

	return names[t]
}

type TestCase struct {
	startState     TxEngineState
	stateChanges   []StateChange
	tx             TxType
	stateAssertion func(state TxEngineState) error
}

func (test TestCase) String() string {
	var sb strings.Builder
	sb.WriteString("start from ")
	sb.WriteString(test.startState.String())
	sb.WriteString(" with ")
	sb.WriteString(test.tx.String())

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

	tests := []TestCase{
		// Start from RW and test all single hop transitions with and without tx
		{AcceptingReadAndWrite, []StateChange{
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadOnly, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadAndWrite, []StateChange{
			{NotServing, assertTakesTime()}},
		WrTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()}},
		WrTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadOnly, assertTakesTime()}},
		WrTx, assertEndStateIs(AcceptingReadOnly),},


		// Start from RW and test all transitions with and without tx, plus a concurrent Stop()
		{AcceptingReadAndWrite, []StateChange{
			{NotServing, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadOnly, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{NotServing, assertTakesTime()},
			{NotServing, assertTakesTime()}},
		WrTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{NotServing, assertTakesTime()}},
		WrTx, assertEndStateIs(NotServing),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadOnly, assertTakesTime()},
			{NotServing, assertTakesTime()}},
		WrTx, assertEndStateIs(NotServing),},


		// Start from RW and test all transitions with and without tx, plus a concurrent ReadOnly()
		{AcceptingReadAndWrite, []StateChange{
			{NotServing, assertIsInstant()},
			{AcceptingReadOnly, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{AcceptingReadOnly, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadOnly, assertIsInstant()},
			{AcceptingReadOnly, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadAndWrite, []StateChange{
			{NotServing, assertTakesTime()},
			{AcceptingReadOnly, assertTakesTime()}},
		WrTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{AcceptingReadOnly, assertTakesTime()}},
		WrTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadAndWrite, []StateChange{
			{AcceptingReadOnly, assertTakesTime()},
			{AcceptingReadOnly, assertTakesTime()}},
		WrTx, assertEndStateIs(AcceptingReadOnly),},


		// Start from RO and test all single hop transitions with and without tx
		{AcceptingReadOnly, []StateChange{
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadOnly, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadOnly),},

		{AcceptingReadOnly, []StateChange{
			{NotServing, assertIsInstant()}},
		WrTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()}},
		WrTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadOnly, assertIsInstant()}},
		WrTx, assertEndStateIs(AcceptingReadOnly),},


		// Start from RO and test all transitions with and without tx, plus a concurrent Stop()
		{AcceptingReadOnly, []StateChange{
			{NotServing, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadOnly, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		NoTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{NotServing, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		WrTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		WrTx, assertEndStateIs(NotServing),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadOnly, assertIsInstant()},
			{NotServing, assertIsInstant()}},
		WrTx, assertEndStateIs(NotServing),},


		// Start from RO and test all transitions with and without tx, plus a concurrent ReadWrite()
		{AcceptingReadOnly, []StateChange{
			{NotServing, assertIsInstant()},
			{AcceptingReadAndWrite, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{AcceptingReadAndWrite, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadOnly, assertIsInstant()},
			{AcceptingReadAndWrite, assertIsInstant()}},
		NoTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{NotServing, assertIsInstant()},
			{AcceptingReadAndWrite, assertIsInstant()}},
		WrTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadAndWrite, assertIsInstant()},
			{AcceptingReadAndWrite, assertIsInstant()}},
		WrTx, assertEndStateIs(AcceptingReadAndWrite),},

		{AcceptingReadOnly, []StateChange{
			{AcceptingReadOnly, assertIsInstant()},
			{AcceptingReadAndWrite, assertIsInstant()}},
		WrTx, assertEndStateIs(AcceptingReadAndWrite),},

	}

	for _, test := range tests {
		outerT.Run(test.String(), func(t *testing.T) {

			db := setUpQueryExecutorTest(t)
			defer db.Close()
			te := setupTxEngine(db)

			failIfError(t,
				changeState(te, test.startState))

			switch test.tx {
			case NoTx:
				// nothing to do
			case WrTx:
				startTransaction(te, t, true)
			case ReTx:
				startTransaction(te, t, false)
			}

			wg := sync.WaitGroup{}
			for _, change := range test.stateChanges {
				wg.Add(1)
				go func() {
					defer wg.Done()
					start := time.Now()

					failIfError(t,
						changeState(te, change.newState))

					failIfError(t,
						change.timeAssertion(start))
				}()

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

func startTransaction(te *TxEngine, t *testing.T, writeTransaction bool) {
	options := &querypb.ExecuteOptions{}
	if writeTransaction {
		options.TransactionIsolation = querypb.ExecuteOptions_DEFAULT
	} else {
		options.TransactionIsolation = querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY
	}
	_, err := te.Begin(context.Background(), options)
	failIfError(t, err)
}
