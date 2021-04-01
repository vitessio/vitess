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

package schemamanager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"context"

	"vitess.io/vitess/go/vt/log"
)

// UIController handles schema events.
type UIController struct {
	sqls     []string
	keyspace string
	writer   http.ResponseWriter
}

// NewUIController creates a UIController instance
func NewUIController(
	sqlStr string, keyspace string, writer http.ResponseWriter) *UIController {
	controller := &UIController{
		sqls:     make([]string, 0, 32),
		keyspace: keyspace,
		writer:   writer,
	}
	for _, sql := range strings.Split(sqlStr, ";") {
		s := strings.TrimSpace(sql)
		if s != "" {
			controller.sqls = append(controller.sqls, s)
		}
	}

	return controller
}

// Open is a no-op.
func (controller *UIController) Open(ctx context.Context) error {
	return nil
}

// Read reads schema changes
func (controller *UIController) Read(ctx context.Context) ([]string, error) {
	return controller.sqls, nil
}

// Close is a no-op.
func (controller *UIController) Close() {
}

// Keyspace returns keyspace to apply schema.
func (controller *UIController) Keyspace() string {
	return controller.keyspace
}

// OnReadSuccess is no-op
func (controller *UIController) OnReadSuccess(ctx context.Context) error {
	controller.writer.Write(
		[]byte(fmt.Sprintf("OnReadSuccess, sqls: %v\n", controller.sqls)))
	return nil
}

// OnReadFail is no-op
func (controller *UIController) OnReadFail(ctx context.Context, err error) error {
	controller.writer.Write(
		[]byte(fmt.Sprintf("OnReadFail, error: %v\n", err)))
	return err
}

// OnValidationSuccess is no-op
func (controller *UIController) OnValidationSuccess(ctx context.Context) error {
	controller.writer.Write(
		[]byte(fmt.Sprintf("OnValidationSuccess, sqls: %v\n", controller.sqls)))
	return nil
}

// OnValidationFail is no-op
func (controller *UIController) OnValidationFail(ctx context.Context, err error) error {
	controller.writer.Write(
		[]byte(fmt.Sprintf("OnValidationFail, error: %v\n", err)))
	return err
}

// OnExecutorComplete is no-op
func (controller *UIController) OnExecutorComplete(ctx context.Context, result *ExecuteResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		log.Errorf("Failed to serialize ExecuteResult: %v", err)
		return err
	}
	controller.writer.Write([]byte(fmt.Sprintf("Executor succeeds: %s", string(data))))
	return nil
}

var _ Controller = (*UIController)(nil)
