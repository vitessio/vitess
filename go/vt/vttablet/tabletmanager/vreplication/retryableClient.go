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

type retryableClient struct {
	binlogplayer.DBClient
	inTransaction bool
	queries       []string
}

func (rt *retryableClient) Begin() error {
	if err := rt.DBClient.Begin(); err != nil {
		return err
	}
	rt.inTransaction = true
	return nil
}

func (rt *retryableClient) Commit() error {
	if err := rt.DBClient.Commit(); err != nil {
		return err
	}
	rt.inTransaction = false
	rt.queries = nil
	return nil
}

func (rt *retryableClient) Rollback() error {
	if err := rt.DBClient.Rollback(); err != nil {
		return err
	}
	rt.inTransaction = false
	rt.queries = nil
	return nil
}

func (rt *retryableClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	if !rt.inTransaction {
		rt.queries = []string{query}
	} else {
		rt.queries = append(rt.queries, query)
	}
	return rt.DBClient.ExecuteFetch(query, maxrows)
}

func (rt *retryableClient) Retry() error {
	if !rt.inTransaction {
		_, err := rt.DBClient.ExecuteFetch(rt.queries[0], 10000)
		return err
	}
	if err := rt.DBClient.Rollback(); err != nil {
		return err
	}
	if err := rt.DBClient.Begin(); err != nil {
		return err
	}
	for _, q := range rt.queries {
		if _, err := rt.DBClient.ExecuteFetch(q, 10000); err != nil {
			return err
		}
	}
	return nil
}
