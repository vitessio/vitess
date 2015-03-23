// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
)

// DataSourcer defines how the autoschema system get schema change commands
type DataSourcer interface {
	Open() error
	Read() ([]string, error)
	Close() error
}

// EventHandler defines callbacks for events happen during schema management
type EventHandler interface {
	OnDataSourcerReadSuccess([]string) error
	OnDataSourcerReadFail(error) error
	OnValidationSuccess([]string) error
	OnValidationFail(error) error
	OnExecutorComplete(*ExecuteResult) error
}

// Executor applies schema changes to underlying system
type Executor interface {
	Open() error
	Validate(sqls []string) error
	Execute(sqls []string, shards []string) *ExecuteResult
	Close() error
}

// ExecuteResult contains information about schema management state
type ExecuteResult struct {
	FailedShards  []ShardWithError
	SuccessShards []ShardResult
	CurSqlIndex   int
	Sqls          []string
	ExecutorErr   string
}

// ShardWithError contains information why a shard failed to execute given sql
type ShardWithError struct {
	Shard string
	Err   string
}

// ShardResult contains sql execute information on a particula shard
type ShardResult struct {
	Shard  string
	Result *mproto.QueryResult
}

// Run schema changes on Vitess through VtGate
func Run(sourcer DataSourcer,
	exec Executor,
	handler EventHandler,
	shards []string) error {
	if err := sourcer.Open(); err != nil {
		log.Errorf("failed to open data sourcer: %v", err)
		return err
	}
	defer sourcer.Close()
	sqls, err := sourcer.Read()
	if err != nil {
		log.Errorf("failed to read data from data sourcer: %v", err)
		return handler.OnDataSourcerReadFail(err)
	}
	handler.OnDataSourcerReadSuccess(sqls)
	if err := exec.Open(); err != nil {
		log.Errorf("failed to open executor: %v", err)
		return err
	}
	defer exec.Close()
	if err := exec.Validate(sqls); err != nil {
		log.Errorf("validation fail: %v", err)
		return handler.OnValidationFail(err)
	}
	handler.OnValidationSuccess(sqls)
	result := exec.Execute(sqls, shards)
	handler.OnExecutorComplete(result)
	return nil
}
