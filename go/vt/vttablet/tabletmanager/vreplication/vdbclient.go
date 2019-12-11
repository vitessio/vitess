/*
Copyright 2019 The Vitess Authors.

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

package vreplication

import (
	"io"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
)

// vdbClient is a wrapper on binlogplayer.DBClient.
// It allows us to retry a failed transactions on lock errors.
type vdbClient struct {
	binlogplayer.DBClient
	stats         *binlogplayer.Stats
	InTransaction bool
	startTime     time.Time
	queries       []string
}

func newVDBClient(dbclient binlogplayer.DBClient, stats *binlogplayer.Stats) *vdbClient {
	return &vdbClient{
		DBClient: dbclient,
		stats:    stats,
	}
}

func (vc *vdbClient) Begin() error {
	if vc.InTransaction {
		return nil
	}
	if err := vc.DBClient.Begin(); err != nil {
		return err
	}
	vc.queries = append(vc.queries, "begin")
	vc.InTransaction = true
	vc.startTime = time.Now()
	return nil
}

func (vc *vdbClient) Commit() error {
	if err := vc.DBClient.Commit(); err != nil {
		return err
	}
	vc.InTransaction = false
	vc.queries = nil
	vc.stats.Timings.Record(binlogplayer.BlplTransaction, vc.startTime)
	return nil
}

func (vc *vdbClient) Rollback() error {
	if err := vc.DBClient.Rollback(); err != nil {
		return err
	}
	vc.InTransaction = false
	// Don't reset queries to allow for vplayer to retry.
	return nil
}

func (vc *vdbClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	defer vc.stats.Timings.Record(binlogplayer.BlplQuery, time.Now())

	if !vc.InTransaction {
		vc.queries = []string{query}
	} else {
		vc.queries = append(vc.queries, query)
	}
	return vc.DBClient.ExecuteFetch(query, maxrows)
}

// Execute is ExecuteFetch without the maxrows.
func (vc *vdbClient) Execute(query string) (*sqltypes.Result, error) {
	// Number of rows should never exceed relayLogMaxSize.
	return vc.ExecuteFetch(query, relayLogMaxSize)
}

func (vc *vdbClient) ExecuteWithRetry(ctx context.Context, query string) (*sqltypes.Result, error) {
	qr, err := vc.Execute(query)
	for err != nil {
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.ERLockDeadlock || sqlErr.Number() == mysql.ERLockWaitTimeout {
			log.Infof("retryable error: %v, waiting for %v and retrying", sqlErr, dbLockRetryDelay)
			if err := vc.Rollback(); err != nil {
				return nil, err
			}
			time.Sleep(dbLockRetryDelay)
			// Check context here. Otherwise this can become an infinite loop.
			select {
			case <-ctx.Done():
				return nil, io.EOF
			default:
			}
			qr, err = vc.Retry()
			continue
		}
		return qr, err
	}
	return qr, nil
}

func (vc *vdbClient) Retry() (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	for _, q := range vc.queries {
		if q == "begin" {
			if err := vc.Begin(); err != nil {
				return nil, err
			}
			continue
		}
		// Number of rows should never exceed relayLogMaxSize.
		result, err := vc.DBClient.ExecuteFetch(q, relayLogMaxSize)
		if err != nil {
			return nil, err
		}
		qr = result
	}
	return qr, nil
}
