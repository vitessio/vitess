// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver"
)

// TxFetcher alows you to capture and fetch info on the next transaction.
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
func (fetcher *TxFetcher) Fetch() *tabletserver.TxConnection {
	for {
		tx := (<-fetcher.ch).(*tabletserver.TxConnection)
		// Skip any events that pre-date start time.
		if tx.EndTime.After(fetcher.start) {
			tabletserver.TxLogger.Unsubscribe(fetcher.ch)
			return tx
		}
	}
}
