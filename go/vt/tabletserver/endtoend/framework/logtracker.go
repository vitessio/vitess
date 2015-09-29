// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import "github.com/youtube/vitess/go/vt/tabletserver"

// TxFetcher alows you to capture and fetch info on the next transaction.
type TxFetcher chan interface{}

// NewTxFetcher sets up the capture and returns a new TxFetcher.
func NewTxFetcher() TxFetcher {
	return TxFetcher(tabletserver.TxLogger.Subscribe("endtoend"))
}

// Fetch fetches the last captured transaction. It must be called only once.
func (ch TxFetcher) Fetch() *tabletserver.TxConnection {
	tx := (<-ch).(*tabletserver.TxConnection)
	tabletserver.TxLogger.Unsubscribe(ch)
	return tx
}
