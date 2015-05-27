// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"
)

func TestPlainController(t *testing.T) {
	sql := "CREATE TABLE test_table (pk int)"
	controller := NewPlainController(sql, "test_keyspace")
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

	errReadFail := fmt.Errorf("read fail")
	err = controller.OnReadFail(ctx, errReadFail)
	if err != errReadFail {
		t.Fatalf("should get error:%v, but get: %v", errReadFail, err)
	}

	err = controller.OnValidationSuccess(ctx)
	if err != nil {
		t.Fatalf("OnValidationSuccess should succeed")
	}

	errValidationFail := fmt.Errorf("validation fail")
	err = controller.OnValidationFail(ctx, errValidationFail)
	if err != errValidationFail {
		t.Fatalf("should get error:%v, but get: %v", errValidationFail, err)
	}

	err = controller.OnExecutorComplete(ctx, &ExecuteResult{})
	if err != nil {
		t.Fatalf("OnExecutorComplete should succeed")
	}
}
