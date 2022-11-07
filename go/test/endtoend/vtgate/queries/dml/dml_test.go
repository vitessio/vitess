/*
Copyright 2022 The Vitess Authors.

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

package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiEqual(t *testing.T) {
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("test uses multiple keyspaces, test framework only supports partial keyspace testing for a single keyspace")
	}
	mcmp, closer := start(t)
	defer closer()

	// initial rows
	mcmp.Exec("insert into user_tbl(id, region_id) values (1,2),(3,4)")

	// multi equal query
	qr := mcmp.Exec("update user_tbl set id = 2 where (id, region_id) in ((1,1), (3,4))")
	assert.EqualValues(t, 1, qr.RowsAffected) // multi equal query

	qr = mcmp.Exec("delete from user_tbl where (id, region_id) in ((1,1), (2,4))")
	assert.EqualValues(t, 1, qr.RowsAffected)
}
