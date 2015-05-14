// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"encoding/json"
	"fmt"
)

// ConsoleEventHandler prints various schema manager events to the stdout
type ConsoleEventHandler struct{}

// NewConsoleEventHandler creates a new ConsoleEventHandler instance.
func NewConsoleEventHandler() *ConsoleEventHandler {
	return &ConsoleEventHandler{}
}

// OnDataSourcerReadSuccess is called when schemamanager successfully reads all sql statements.
func (handler *ConsoleEventHandler) OnDataSourcerReadSuccess(sql []string) error {
	fmt.Println("Successfully read all schema changes.")
	return nil
}

// OnDataSourcerReadFail is called when schemamanager fails to read all sql statements.
func (handler *ConsoleEventHandler) OnDataSourcerReadFail(err error) error {
	fmt.Printf("Failed to read schema changes, error: %v\n", err)
	return err
}

// OnValidationSuccess is called when schemamanager successfully validates all sql statements.
func (handler *ConsoleEventHandler) OnValidationSuccess([]string) error {
	fmt.Println("Successfully validate all sqls.")
	return nil
}

// OnValidationFail is called when schemamanager fails to validate sql statements.
func (handler *ConsoleEventHandler) OnValidationFail(err error) error {
	fmt.Printf("Failed to validate sqls, error: %v\n", err)
	return err
}

// OnExecutorComplete  is called when schemamanager finishes applying schema changes.
func (handler *ConsoleEventHandler) OnExecutorComplete(result *ExecuteResult) error {
	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Executor finished, result: %s\n", string(out))
	return nil
}

// ConsoleEventHandler have to implement EventHandler interface
var _ EventHandler = (*ConsoleEventHandler)(nil)
