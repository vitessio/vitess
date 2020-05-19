/*
Copyright 2020 The Vitess Authors.

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

package schema

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

func TestTracker(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t)
	defer cancel()

	tracker := NewTracker(se)
	tracker.Open()

	gtid1 := "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10"
	ddl1 := "create table tracker_test (id int)"
	ts1 := int64(1427325876)
	var query string
	query = "CREATE TABLE IF NOT EXISTS _vt.schema_version.*"
	db.AddQueryPattern(query, &sqltypes.Result{})

	query = fmt.Sprintf("insert into _vt.schema_version.*%s.*%s.*%d.*", gtid1, regexp.QuoteMeta(ddl1), ts1)
	db.AddQueryPattern(query, &sqltypes.Result{})

	require.NoError(t, tracker.SchemaUpdated(gtid1, ddl1, ts1))
	require.Error(t, tracker.SchemaUpdated("", ddl1, ts1))
	require.Error(t, tracker.SchemaUpdated(gtid1, "", ts1))
}
