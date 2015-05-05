// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package uihandler

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/schemamanager"
)

// UIEventHandler handles schema events
type UIEventHandler struct {
	writer http.ResponseWriter
}

// NewUIEventHandler creates a UIEventHandler instance
func NewUIEventHandler(writer http.ResponseWriter) *UIEventHandler {
	return &UIEventHandler{writer: writer}
}

// OnDataSourcerReadSuccess is no-op
func (handler *UIEventHandler) OnDataSourcerReadSuccess(sqls []string) error {
	handler.writer.Write([]byte(fmt.Sprintf("OnDataSourcerReadSuccess, sqls: %v\n", sqls)))
	return nil
}

// OnDataSourcerReadFail is no-op
func (handler *UIEventHandler) OnDataSourcerReadFail(err error) error {
	handler.writer.Write([]byte(fmt.Sprintf("OnDataSourcerReadFail, error: %v\n", err)))
	return err
}

// OnValidationSuccess is no-op
func (handler *UIEventHandler) OnValidationSuccess(sqls []string) error {
	handler.writer.Write([]byte(fmt.Sprintf("OnValidationSuccess, sqls: %v\n", sqls)))
	return nil
}

// OnValidationFail is no-op
func (handler *UIEventHandler) OnValidationFail(err error) error {
	handler.writer.Write([]byte(fmt.Sprintf("OnValidationFail, error: %v\n", err)))
	return err
}

// OnExecutorComplete is no-op
func (handler *UIEventHandler) OnExecutorComplete(result *schemamanager.ExecuteResult) error {
	str, err := json.Marshal(result)
	if err != nil {
		log.Errorf("Failed to serialize ExecuteResult: %v", err)
		return err
	}
	handler.writer.Write(str)
	return nil
}

var _ schemamanager.EventHandler = (*UIEventHandler)(nil)
