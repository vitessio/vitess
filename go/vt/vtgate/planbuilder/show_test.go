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

package planbuilder

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestBuildDBPlan(t *testing.T) {
	vschema := &vschemaWrapper{
		keyspace: &vindexes.Keyspace{Name: "main"},
	}

	testCases := []struct {
		query    string
		expected string
	}{{
		query:    "show databases like 'main'",
		expected: `[[VARCHAR("main")]]`,
	}, {
		query:    "show databases like '%ys%'",
		expected: `[[VARCHAR("mysql")] [VARCHAR("sys")]]`,
	}}

	for _, s := range testCases {
		t.Run(s.query, func(t *testing.T) {
			parserOut, err := sqlparser.Parse(s.query)
			require.NoError(t, err)

			show := parserOut.(*sqlparser.Show)
			primitive, err := buildDBPlan(show.Internal.(*sqlparser.ShowBasic), vschema)
			require.NoError(t, err)

			result, err := primitive.Execute(nil, nil, false)
			require.NoError(t, err)
			require.Equal(t, s.expected, fmt.Sprintf("%v", result.Rows))
		})
	}

}
