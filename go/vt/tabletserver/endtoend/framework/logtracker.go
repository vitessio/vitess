// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"errors"
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver"
)

// TxFetcher allows you to capture and fetch info on the next transaction.
type TxFetcher struct {
	start time.Time
	ch    chan interface{}
}

// NewTxFetcher sets up the capture and returns a new TxFetcher.
func NewTxFetcher() *TxFetcher {
	return &TxFetcher{
		start: time.Now(),
		ch:    tabletserver.TxLogger.Subscribe("endtoend"),
	}
}

// Fetch fetches the last captured transaction. It must be called only once.
// If the wait is longer than one second, it returns an error.
func (fetcher *TxFetcher) Fetch() (*tabletserver.TxConnection, error) {
	tmr := time.NewTimer(1 * time.Second)
	defer tmr.Stop()
	for {
		select {
		case itx := <-fetcher.ch:
			tx := itx.(*tabletserver.TxConnection)
			// Skip any events that pre-date start time.
			if tx.EndTime.Before(fetcher.start) {
				continue
			}
			tabletserver.TxLogger.Unsubscribe(fetcher.ch)
			return tx, nil
		case <-tmr.C:
			return nil, errors.New("error waiting for transaction event")
		}
	}
}

// QueryFetcher allows you to capture and fetch queries that are being
// executed by TabletServer.
type QueryFetcher struct {
	start   time.Time
	ch      chan interface{}
	queries chan *tabletserver.LogStats
}

// NewQueryFetcher sets up the capture and retuns a QueryFetcher.
// It has a buffer size of 20. You must call Close once done.
func NewQueryFetcher() *QueryFetcher {
	fetcher := &QueryFetcher{
		start:   time.Now(),
		ch:      tabletserver.StatsLogger.Subscribe("endtoend"),
		queries: make(chan *tabletserver.LogStats, 20),
	}
	go func() {
		for log := range fetcher.ch {
			fetcher.queries <- log.(*tabletserver.LogStats)
		}
		close(fetcher.queries)
	}()
	return fetcher
}

// Close closes the QueryFetcher.
func (fetcher *QueryFetcher) Close() {
	tabletserver.StatsLogger.Unsubscribe(fetcher.ch)
	close(fetcher.ch)
}

// Next fetches the next captured query.
// If the wait is longer than one second, it returns an error.
func (fetcher *QueryFetcher) Next() (*tabletserver.LogStats, error) {
	tmr := time.NewTimer(5 * time.Second)
	defer tmr.Stop()
	for {
		select {
		case query := <-fetcher.queries:
			// Skip any events that pre-date start time.
			if query.EndTime.Before(fetcher.start) {
				continue
			}
			return query, nil
		case <-tmr.C:
			return nil, errors.New("error waiting for query event")
		}
	}
}
