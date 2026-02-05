/*
Copyright 2024 The Vitess Authors.

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

package tx

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
)

/*
	TestRollbackToSavePointQueryDetails tests the rollback to savepoint query details

s1
q1
s2
r1
q2
q3
s3
s4
q4
q5
r2 -- error
r4
*/
func TestRollbackToSavePointQueryDetails(t *testing.T) {
	p := &Properties{}
	p.RecordSavePointDetail("s1")
	p.RecordQueryDetail("select 1", nil)
	p.RecordSavePointDetail("s2")
	require.NoError(t, p.RollbackToSavepoint("s1"))
	p.RecordQueryDetail("select 2", nil)
	p.RecordQueryDetail("select 3", nil)
	p.RecordSavePointDetail("s3")
	p.RecordSavePointDetail("s4")
	p.RecordQueryDetail("select 4", nil)
	p.RecordQueryDetail("select 5", nil)
	require.ErrorContains(t, p.RollbackToSavepoint("s2"), "savepoint s2 not found")
	require.NoError(t, p.RollbackToSavepoint("s4"))

	utils.MustMatch(t, p.GetQueries(), []Query{
		{Sql: "select 2"},
		{Sql: "select 3"},
	})
}
