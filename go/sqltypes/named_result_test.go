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

package sqltypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestToNamedResult(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: Int64,
		}, {
			Name: "status",
			Type: VarChar,
		}, {
			Name: "uid",
			Type: Uint64,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "0"), TestValue(VarChar, "s0"), TestValue(Uint64, "0")},
			{TestValue(Int64, "1"), TestValue(VarChar, "s1"), TestValue(Uint64, "1")},
			{TestValue(Int64, "2"), TestValue(VarChar, "s2"), TestValue(Uint64, "2")},
		},
	}
	named := in.Named()
	for i := range in.Rows {
		require.Equal(t, in.Rows[i][0], named.Rows[i]["id"])
		require.Equal(t, int64(i), named.Rows[i].AsInt64("id", 0))

		require.Equal(t, in.Rows[i][1], named.Rows[i]["status"])
		require.Equal(t, fmt.Sprintf("s%d", i), named.Rows[i].AsString("status", "notfound"))

		require.Equal(t, in.Rows[i][2], named.Rows[i]["uid"])
		require.Equal(t, uint64(i), named.Rows[i].AsUint64("uid", 0))
	}
}
