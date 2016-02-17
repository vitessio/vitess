// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"encoding/json"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"
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
	for _, sql := range strings.Split(sqlStr, ";") {
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

// Read reads schema changes
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
