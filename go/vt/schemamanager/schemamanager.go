// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"encoding/json"
	"fmt"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
)

const (
	SchemaChangeDirName = "schema_change_dir"
)

// ControllerFactory takes a set params and construct a Controller instance.
type ControllerFactory func(params map[string]string) (Controller, error)

var (
	controllerFactories = make(map[string]ControllerFactory)
)

// Controller is responsible for getting schema change for a
// certain keyspace and also handling various events happened during schema
// change.
type Controller interface {
	Open() error
	Read() (sqls []string, err error)
	Close()
	GetKeyspace() string
	OnReadSuccess() error
	OnReadFail(err error) error
	OnValidationSuccess() error
	OnValidationFail(err error) error
	OnExecutorComplete(*ExecuteResult) error
}

// Executor applies schema changes to underlying system
type Executor interface {
	Open(keyspace string) error
	Validate(sqls []string) error
	Execute(sqls []string) *ExecuteResult
	Close()
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
func Run(controller Controller, executor Executor) error {
	if err := controller.Open(); err != nil {
		log.Errorf("failed to open data sourcer: %v", err)
		return err
	}
	defer controller.Close()
	sqls, err := controller.Read()
	if err != nil {
		log.Errorf("failed to read data from data sourcer: %v", err)
		controller.OnReadFail(err)
		return err
	}

	controller.OnReadSuccess()
	keyspace := controller.GetKeyspace()
	if err := executor.Open(keyspace); err != nil {
		log.Errorf("failed to open executor: %v", err)
		return err
	}
	defer executor.Close()
	if err := executor.Validate(sqls); err != nil {
		log.Errorf("validation fail: %v", err)
		controller.OnValidationFail(err)
		return err
	}
	controller.OnValidationSuccess()
	result := executor.Execute(sqls)
	controller.OnExecutorComplete(result)
	if result.ExecutorErr != "" || len(result.FailedShards) > 0 {
		out, _ := json.MarshalIndent(result, "", "  ")
		return fmt.Errorf("Schema change failed, ExecuteResult: %v\n", string(out))
	}
	return nil
}

// RegisterControllerFactory register a control factory.
func RegisterControllerFactory(name string, factory ControllerFactory) {
	if _, ok := controllerFactories[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	controllerFactories[name] = factory
}

// GetControllerFactory gets a ControllerFactory.
func GetControllerFactory(name string) (ControllerFactory, error) {
	factory, ok := controllerFactories[name]
	if !ok {
		return nil, fmt.Errorf("there is no data sourcer factory with name: %s", name)
	}
	return factory, nil
}
