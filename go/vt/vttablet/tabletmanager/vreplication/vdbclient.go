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
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
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

func (vc *vdbClient) Retry() error {
	for _, q := range vc.queries {
		if q == "begin" {
			if err := vc.Begin(); err != nil {
				return err
			}
			continue
		}
		if _, err := vc.DBClient.ExecuteFetch(q, 10000); err != nil {
			return err
		}
	}
	return nil
}
