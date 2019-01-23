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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
)

// retryableClient is a wrapper on binlogplayer.DBClient.
// It allows us to retry a failed transactions on lock errors.
type retryableClient struct {
	binlogplayer.DBClient
	InTransaction bool
	queries       []string
}

func (rt *retryableClient) Begin() error {
	if rt.InTransaction {
		return nil
	}
	if err := rt.DBClient.Begin(); err != nil {
		return err
	}
	rt.queries = append(rt.queries, "begin")
	rt.InTransaction = true
	return nil
}

func (rt *retryableClient) Commit() error {
	if err := rt.DBClient.Commit(); err != nil {
		return err
	}
	rt.InTransaction = false
	rt.queries = nil
	return nil
}

func (rt *retryableClient) Rollback() error {
	if err := rt.DBClient.Rollback(); err != nil {
		return err
	}
	rt.InTransaction = false
	// Don't reset queries to allow for vplayer to retry.
	return nil
}

func (rt *retryableClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	if !rt.InTransaction {
		rt.queries = []string{query}
	} else {
		rt.queries = append(rt.queries, query)
	}
	return rt.DBClient.ExecuteFetch(query, maxrows)
}

func (rt *retryableClient) Retry() error {
	for _, q := range rt.queries {
		if q == "begin" {
			if err := rt.Begin(); err != nil {
				return err
			}
			continue
		}
		if _, err := rt.DBClient.ExecuteFetch(q, 10000); err != nil {
			return err
		}
	}
	return nil
}
