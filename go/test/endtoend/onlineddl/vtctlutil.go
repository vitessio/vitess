/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
)

var throttlerConfigTimeout = 60 * time.Second

// CheckCancelAllMigrations cancels all pending migrations. There is no validation for affected migrations.
func CheckCancelAllMigrationsViaVtctl(t *testing.T, vtctlclient *cluster.VtctlClientProcess, keyspace string) {
	cancelQuery := "alter vitess_migration cancel all"

	_, err := vtctlclient.ApplySchemaWithOutput(keyspace, cancelQuery, cluster.VtctlClientParams{SkipPreflight: true})
	assert.NoError(t, err)
}
