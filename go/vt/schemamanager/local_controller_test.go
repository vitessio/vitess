// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestLocalControllerNoSchemaChanges(t *testing.T) {
	schemaChangeDir, err := ioutil.TempDir("", "localcontroller-test")
	defer os.RemoveAll(schemaChangeDir)
	if err != nil {
		t.Fatalf("failed to create temp schema change dir, error: %v", err)
	}
	controller := NewLocalController(schemaChangeDir)
	ctx := context.Background()
	if err := controller.Open(ctx); err != nil {
		t.Fatalf("Open should succeed, but got error: %v", err)
	}
	defer controller.Close()
	data, err := controller.Read(ctx)
	if err != nil {
		t.Fatalf("Read should succeed, but got error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("there is no schema change, Read should return empty data")
	}
}

func TestLocalControllerOpen(t *testing.T) {
	controller := NewLocalController("")
	ctx := context.Background()

	if err := controller.Open(ctx); err == nil {
		t.Fatalf("Open should fail, no such dir")
	}

	schemaChangeDir, err := ioutil.TempDir("", "localcontroller-test")
	defer os.RemoveAll(schemaChangeDir)

	// create a file under schema change dir
	_, err = os.Create(path.Join(schemaChangeDir, "create_test_table.sql"))
	if err != nil {
		t.Fatalf("failed to create sql file, error: %v", err)
	}

	controller = NewLocalController(schemaChangeDir)
	if err := controller.Open(ctx); err != nil {
		t.Fatalf("Open should succeed")
	}
	data, err := controller.Read(ctx)
	if err != nil {
		t.Fatalf("Read should succeed, but got error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("there is no schema change, Read should return empty data")
	}
	controller.Close()

	testKeyspaceDir := path.Join(schemaChangeDir, "test_keyspace")
	if err := os.MkdirAll(testKeyspaceDir, os.ModePerm); err != nil {
		t.Fatalf("failed to create test_keyspace dir, error: %v", err)
	}

	controller = NewLocalController(schemaChangeDir)
	if err := controller.Open(ctx); err != nil {
		t.Fatalf("Open should succeed")
	}
	data, err = controller.Read(ctx)
	if err != nil {
		t.Fatalf("Read should succeed, but got error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("there is no schema change, Read should return empty data")
	}
	controller.Close()
}

func TestLocalControllerSchemaChange(t *testing.T) {
	schemaChangeDir, err := ioutil.TempDir("", "localcontroller-test")
	if err != nil {
		t.Fatalf("failed to create temp schema change dir, error: %v", err)
	}
	defer os.RemoveAll(schemaChangeDir)

	testKeyspaceInputDir := path.Join(schemaChangeDir, "test_keyspace/input")
	if err := os.MkdirAll(testKeyspaceInputDir, os.ModePerm); err != nil {
		t.Fatalf("failed to create test_keyspace dir, error: %v", err)
	}

	file, err := os.Create(path.Join(testKeyspaceInputDir, "create_test_table.sql"))
	if err != nil {
		t.Fatalf("failed to create sql file, error: %v", err)
	}

	sqls := []string{
		"create table test_table_01 (id int)",
		"create table test_table_02 (id string)",
	}

	file.WriteString(strings.Join(sqls, ";"))
	file.Close()

	controller := NewLocalController(schemaChangeDir)
	ctx := context.Background()

	if err := controller.Open(ctx); err != nil {
		t.Fatalf("Open should succeed, but got error: %v", err)
	}

	defer controller.Close()

	data, err := controller.Read(ctx)
	if err != nil {
		t.Fatalf("Read should succeed, but got error: %v", err)
	}

	if !reflect.DeepEqual(sqls, data) {
		t.Fatalf("expect to get sqls: %v, but got: %v", sqls, data)
	}

	if controller.Keyspace() != "test_keyspace" {
		t.Fatalf("expect to get keyspace: 'test_keyspace', but got: '%s'",
			controller.Keyspace())
	}

	// test various callbacks
	if err := controller.OnReadSuccess(ctx); err != nil {
		t.Fatalf("OnReadSuccess should succeed, but got error: %v", err)
	}

	if err := controller.OnReadFail(ctx, fmt.Errorf("read fail")); err != nil {
		t.Fatalf("OnReadFail should succeed, but got error: %v", err)
	}

	errorPath := path.Join(controller.errorDir, controller.sqlFilename)

	if err := controller.OnValidationSuccess(ctx); err != nil {
		t.Fatalf("OnReadSuccess should succeed, but got error: %v", err)
	}

	// move sql file from error dir to input dir for OnValidationFail test
	os.Rename(errorPath, controller.sqlPath)

	if err := controller.OnValidationFail(ctx, fmt.Errorf("validation fail")); err != nil {
		t.Fatalf("OnValidationFail should succeed, but got error: %v", err)
	}

	if _, err := os.Stat(errorPath); os.IsNotExist(err) {
		t.Fatalf("sql file should be moved to error dir, error: %v", err)
	}

	// move sql file from error dir to input dir for OnExecutorComplete test
	os.Rename(errorPath, controller.sqlPath)

	result := &ExecuteResult{
		Sqls: []string{"create table test_table (id int)"},
		SuccessShards: []ShardResult{{
			Shard:  "0",
			Result: &querypb.QueryResult{},
		}},
	}
	logPath := path.Join(controller.logDir, controller.sqlFilename)
	completePath := path.Join(controller.completeDir, controller.sqlFilename)
	if err := controller.OnExecutorComplete(ctx, result); err != nil {
		t.Fatalf("OnExecutorComplete should succeed, but got error: %v", err)
	}
	if _, err := os.Stat(completePath); os.IsNotExist(err) {
		t.Fatalf("sql file should be moved to complete dir, error: %v", err)
	}

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Fatalf("sql file should be moved to log dir, error: %v", err)
	}

	// move sql file from error dir to input dir for OnExecutorComplete test
	os.Rename(completePath, controller.sqlPath)

	result = &ExecuteResult{
		Sqls: []string{"create table test_table (id int)"},
		FailedShards: []ShardWithError{{
			Shard: "0",
			Err:   "execute error",
		}},
	}

	if err := controller.OnExecutorComplete(ctx, result); err != nil {
		t.Fatalf("OnExecutorComplete should succeed, but got error: %v", err)
	}

	if _, err := os.Stat(errorPath); os.IsNotExist(err) {
		t.Fatalf("sql file should be moved to error dir, error: %v", err)
	}
}
