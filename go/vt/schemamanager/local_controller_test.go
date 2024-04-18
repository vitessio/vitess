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
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestLocalControllerNoSchemaChanges(t *testing.T) {
	schemaChangeDir := t.TempDir()
	controller := NewLocalController(schemaChangeDir)
	ctx := context.Background()
	err := controller.Open(ctx)
	require.NoError(t, err)

	defer controller.Close()
	data, err := controller.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, data, "there is no schema change, Read should return empty data")
}

func TestLocalControllerOpen(t *testing.T) {
	controller := NewLocalController("")
	ctx := context.Background()

	err := controller.Open(ctx)
	require.ErrorContains(t, err, "no such file or directory", "Open should fail, no such dir")

	schemaChangeDir := t.TempDir()

	// create a file under schema change dir
	_, err = os.Create(path.Join(schemaChangeDir, "create_test_table.sql"))
	require.NoError(t, err, "failed to create sql file")

	controller = NewLocalController(schemaChangeDir)
	err = controller.Open(ctx)
	require.NoError(t, err)

	data, err := controller.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, data, "there is no schema change, Read should return empty data")

	controller.Close()

	testKeyspaceDir := path.Join(schemaChangeDir, "test_keyspace")
	err = os.MkdirAll(testKeyspaceDir, os.ModePerm)
	require.NoError(t, err, "failed to create test_keyspace dir")

	controller = NewLocalController(schemaChangeDir)
	err = controller.Open(ctx)
	require.NoError(t, err)

	data, err = controller.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, data, "there is no schema change, Read should return empty data")

	controller.Close()
}

func TestLocalControllerSchemaChange(t *testing.T) {
	schemaChangeDir := t.TempDir()

	testKeyspaceInputDir := path.Join(schemaChangeDir, "test_keyspace/input")
	err := os.MkdirAll(testKeyspaceInputDir, os.ModePerm)
	require.NoError(t, err, "failed to create test_keyspace dir")

	file, err := os.Create(path.Join(testKeyspaceInputDir, "create_test_table.sql"))
	require.NoError(t, err, "failed to create sql file")

	sqls := []string{
		"create table test_table_01 (id int)",
		"create table test_table_02 (id string)",
	}

	file.WriteString(strings.Join(sqls, ";"))
	file.Close()

	controller := NewLocalController(schemaChangeDir)
	ctx := context.Background()

	err = controller.Open(ctx)
	require.NoError(t, err)

	defer controller.Close()

	data, err := controller.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, sqls, data)
	require.Equal(t, "test_keyspace", controller.Keyspace())

	// test various callbacks
	err = controller.OnReadSuccess(ctx)
	require.NoError(t, err)

	err = controller.OnReadFail(ctx, fmt.Errorf("read fail"))
	require.NoError(t, err)

	errorPath := path.Join(controller.errorDir, controller.sqlFilename)

	err = controller.OnValidationSuccess(ctx)
	require.NoError(t, err)

	// move sql file from error dir to input dir for OnValidationFail test
	os.Rename(errorPath, controller.sqlPath)

	err = controller.OnValidationFail(ctx, fmt.Errorf("validation fail"))
	require.NoError(t, err)

	_, err = os.Stat(errorPath)
	require.Falsef(t, os.IsNotExist(err), "sql file should be moved to error dir, error: %v", err)

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
	err = controller.OnExecutorComplete(ctx, result)
	require.NoError(t, err)

	_, err = os.Stat(completePath)
	require.Falsef(t, os.IsNotExist(err), "sql file should be moved to complete dir, error: %v", err)

	_, err = os.Stat(logPath)
	require.Falsef(t, os.IsNotExist(err), "sql file should be moved to log dir, error: %v", err)

	// move sql file from error dir to input dir for OnExecutorComplete test
	os.Rename(completePath, controller.sqlPath)

	result = &ExecuteResult{
		Sqls: []string{"create table test_table (id int)"},
		FailedShards: []ShardWithError{{
			Shard: "0",
			Err:   "execute error",
		}},
	}

	err = controller.OnExecutorComplete(ctx, result)
	require.NoError(t, err)

	_, err = os.Stat(errorPath)
	require.Falsef(t, os.IsNotExist(err), "sql file should be moved to error dir, error: %v", err)
}
