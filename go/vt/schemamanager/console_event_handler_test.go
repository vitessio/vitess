// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"fmt"
	"testing"
)

func TestConsoleEventHandler(t *testing.T) {
	sqls := []string{"CREATE TABLE test_table (pk int)"}
	handler := NewConsoleEventHandler()
	err := handler.OnDataSourcerReadSuccess(sqls)
	if err != nil {
		t.Fatalf("OnDataSourcerReadSuccess should succeed")
	}

	errReadFail := fmt.Errorf("read fail")
	err = handler.OnDataSourcerReadFail(errReadFail)
	if err != errReadFail {
		t.Fatalf("should get error:%v, but get: %v", errReadFail, err)
	}

	err = handler.OnValidationSuccess(sqls)
	if err != nil {
		t.Fatalf("OnValidationSuccess should succeed")
	}

	errValidationFail := fmt.Errorf("validation fail")
	err = handler.OnValidationFail(errValidationFail)
	if err != errValidationFail {
		t.Fatalf("should get error:%v, but get: %v", errValidationFail, err)
	}

	err = handler.OnExecutorComplete(&ExecuteResult{})
	if err != nil {
		t.Fatalf("OnExecutorComplete should succeed")
	}
}
