/*
Copyright 2017 Google Inc.

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

package schemamanager

import (
	"encoding/json"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// PlainController implements Controller interface.
type PlainController struct {
	sqls     []string
	keyspace string
}

// NewPlainController creates a new PlainController instance.
func NewPlainController(sqlStr string, keyspace string) *PlainController {
	controller := &PlainController{
		sqls:     make([]string, 0, 32),
		keyspace: keyspace,
	}

	sqls, err := sqlparser.SplitStatementToPieces(sqlStr)
	if err != nil {
		panic(err.Error())
	}

	for _, sql := range sqls {
		s := strings.TrimSpace(sql)
		if s != "" {
			controller.sqls = append(controller.sqls, s)
		}
	}
	return controller
}

// Open is a no-op.
func (controller *PlainController) Open(ctx context.Context) error {
	return nil
}

// Read schema changes
func (controller *PlainController) Read(ctx context.Context) ([]string, error) {
	return controller.sqls, nil
}

// Close is a no-op.
func (controller *PlainController) Close() {
}

// Keyspace returns keyspace to apply schema.
func (controller *PlainController) Keyspace() string {
	return controller.keyspace
}

// OnReadSuccess is called when schemamanager successfully
// reads all sql statements.
func (controller *PlainController) OnReadSuccess(ctx context.Context) error {
	log.Info("Successfully read all schema changes.")
	return nil
}

// OnReadFail is called when schemamanager fails to read all sql statements.
func (controller *PlainController) OnReadFail(ctx context.Context, err error) error {
	log.Errorf("Failed to read schema changes, error: %v\n", err)
	return err
}

// OnValidationSuccess is called when schemamanager successfully validates all sql statements.
func (controller *PlainController) OnValidationSuccess(ctx context.Context) error {
	log.Info("Successfully validated all SQL statements.")
	return nil
}

// OnValidationFail is called when schemamanager fails to validate sql statements.
func (controller *PlainController) OnValidationFail(ctx context.Context, err error) error {
	log.Errorf("Failed to validate SQL statements, error: %v\n", err)
	return err
}

// OnExecutorComplete  is called when schemamanager finishes applying schema changes.
func (controller *PlainController) OnExecutorComplete(ctx context.Context, result *ExecuteResult) error {
	out, _ := json.MarshalIndent(result, "", "  ")
	log.Infof("Executor finished, result: %s\n", string(out))
	return nil
}

var _ Controller = (*PlainController)(nil)
