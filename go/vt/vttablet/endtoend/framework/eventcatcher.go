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

package framework

import (
	"errors"
	"time"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// TxCatcher allows you to capture and fetch transactions that are being
// executed by TabletServer.
type TxCatcher struct {
	catcher *eventCatcher
}

// NewTxCatcher sets up the capture and returns a new TxCatcher.
// You must call Close when done.
func NewTxCatcher() TxCatcher {
	return TxCatcher{catcher: newEventCatcher(tabletenv.TxLogger)}
}

// Close closes the TxCatcher.
func (tc *TxCatcher) Close() {
	tc.catcher.Close()
}

// Next fetches the next captured transaction.
// If the wait is longer than one second, it returns an error.
func (tc *TxCatcher) Next() (*tabletserver.TxConnection, error) {
	event, err := tc.catcher.next()
	if err != nil {
		return nil, err
	}
	return event.(*tabletserver.TxConnection), nil
}

// QueryCatcher allows you to capture and fetch queries that are being
// executed by TabletServer.
type QueryCatcher struct {
	catcher *eventCatcher
}

// NewQueryCatcher sets up the capture and returns a QueryCatcher.
// You must call Close when done.
func NewQueryCatcher() QueryCatcher {
	return QueryCatcher{catcher: newEventCatcher(tabletenv.StatsLogger)}
}

// Close closes the QueryCatcher.
func (qc *QueryCatcher) Close() {
	qc.catcher.Close()
}

// Next fetches the next captured query.
// If the wait is longer than one second, it returns an error.
func (qc *QueryCatcher) Next() (*tabletenv.LogStats, error) {
	event, err := qc.catcher.next()
	if err != nil {
		return nil, err
	}
	return event.(*tabletenv.LogStats), nil
}

type eventCatcher struct {
	start   time.Time
	logger  *streamlog.StreamLogger
	in, out chan interface{}
}

func newEventCatcher(logger *streamlog.StreamLogger) *eventCatcher {
	catcher := &eventCatcher{
		start:  time.Now(),
		logger: logger,
		in:     logger.Subscribe("endtoend"),
		out:    make(chan interface{}, 20),
	}
	go func() {
		for event := range catcher.in {
			endTime := event.(interface {
				EventTime() time.Time
			}).EventTime()
			if endTime.Before(catcher.start) {
				continue
			}
			catcher.out <- event
		}
		close(catcher.out)
	}()
	return catcher
}

// Close closes the eventCatcher.
func (catcher *eventCatcher) Close() {
	catcher.logger.Unsubscribe(catcher.in)
	close(catcher.in)
}

func (catcher *eventCatcher) next() (interface{}, error) {
	tmr := time.NewTimer(5 * time.Second)
	defer tmr.Stop()
	for {
		select {
		case event := <-catcher.out:
			return event, nil
		case <-tmr.C:
			return nil, errors.New("error waiting for query event")
		}
	}
}
