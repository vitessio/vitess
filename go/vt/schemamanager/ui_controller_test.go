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
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"context"
)

func TestUIController(t *testing.T) {
	sql := "CREATE TABLE test_table (pk int)"
	response := httptest.NewRecorder()
	controller := NewUIController(sql, "test_keyspace", response)
	ctx := context.Background()

	err := controller.Open(ctx)
	if err != nil {
		t.Fatalf("controller.Open should succeed, but got error: %v", err)
	}

	keyspace := controller.Keyspace()
	if keyspace != "test_keyspace" {
		t.Fatalf("expect to get keyspace: 'test_keyspace', but got keyspace: '%s'", keyspace)
	}

	sqls, err := controller.Read(ctx)
	if err != nil {
		t.Fatalf("controller.Read should succeed, but got error: %v", err)
	}
	if len(sqls) != 1 {
		t.Fatalf("controller should only get one sql, but got: %v", sqls)
	}
	if sqls[0] != sql {
		t.Fatalf("expect to get sql: '%s', but got: '%s'", sql, sqls[0])
	}
	defer controller.Close()
	err = controller.OnReadSuccess(ctx)
	if err != nil {
		t.Fatalf("OnDataSourcerReadSuccess should succeed")
	}
	if !strings.Contains(response.Body.String(), "OnReadSuccess, sqls") {
		t.Fatalf("controller.OnReadSuccess should write to http response")
	}
	errReadFail := fmt.Errorf("read fail")
	err = controller.OnReadFail(ctx, errReadFail)
	if err != errReadFail {
		t.Fatalf("should get error:%v, but get: %v", errReadFail, err)
	}

	if !strings.Contains(response.Body.String(), "OnReadFail, error") {
		t.Fatalf("controller.OnReadFail should write to http response")
	}

	err = controller.OnValidationSuccess(ctx)
	if err != nil {
		t.Fatalf("OnValidationSuccess should succeed")
	}

	if !strings.Contains(response.Body.String(), "OnValidationSuccess, sqls") {
		t.Fatalf("controller.OnValidationSuccess should write to http response")
	}

	errValidationFail := fmt.Errorf("validation fail")
	err = controller.OnValidationFail(ctx, errValidationFail)
	if err != errValidationFail {
		t.Fatalf("should get error:%v, but get: %v", errValidationFail, err)
	}

	if !strings.Contains(response.Body.String(), "OnValidationFail, error") {
		t.Fatalf("controller.OnValidationFail should write to http response")
	}

	err = controller.OnExecutorComplete(ctx, &ExecuteResult{})
	if err != nil {
		t.Fatalf("OnExecutorComplete should succeed")
	}

	if !strings.Contains(response.Body.String(), "Executor succeeds") {
		t.Fatalf("controller.OnExecutorComplete should write to http response")
	}
}
