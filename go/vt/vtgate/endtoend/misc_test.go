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

package endtoend

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestDatabaseFunc(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "use ks")
	qr := exec(t, conn, "select database()")
	require.Equal(t, `[[VARBINARY("ks")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestSysNumericPrecisionScale(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := exec(t, conn, "select numeric_precision, numeric_scale from information_schema.columns where table_schema = 'ks' and table_name = 't1'")
	assert.True(t, qr.Fields[0].Type == qr.Rows[0][0].Type())
	assert.True(t, qr.Fields[1].Type == qr.Rows[0][1].Type())
}
