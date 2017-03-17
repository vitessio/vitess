// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"errors"
	"time"

	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
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

// NewQueryCatcher sets up the capture and retuns a QueryCatcher.
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
