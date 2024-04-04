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
	"testing"

	"context"

	"github.com/stretchr/testify/require"
)

func TestPlainController(t *testing.T) {
	sql := "CREATE TABLE test_table (pk int)"
	controller := NewPlainController([]string{sql}, "test_keyspace")
	ctx := context.Background()
	err := controller.Open(ctx)
	require.NoError(t, err)

	keyspace := controller.Keyspace()
	require.Equal(t, "test_keyspace", keyspace)

	sqls, err := controller.Read(ctx)
	require.NoError(t, err)
	require.Len(t, sqls, 1, "controller should only get one sql")
	require.Equal(t, sql, sqls[0])

	defer controller.Close()
	err = controller.OnReadSuccess(ctx)
	require.NoError(t, err)

	errReadFail := fmt.Errorf("read fail")
	err = controller.OnReadFail(ctx, errReadFail)
	require.ErrorIs(t, err, errReadFail)

	err = controller.OnValidationSuccess(ctx)
	require.NoError(t, err)

	errValidationFail := fmt.Errorf("validation fail")
	err = controller.OnValidationFail(ctx, errValidationFail)
	require.ErrorIs(t, err, errValidationFail)

	err = controller.OnExecutorComplete(ctx, &ExecuteResult{})
	require.NoError(t, err)
}
