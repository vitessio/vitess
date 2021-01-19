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

package endtoend

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

var procSQL = []string{`create procedure proc1()
BEGIN
	select intval from vitess_test;
END;`, `create procedure proc4()
BEGIN
	select intval from vitess_test;
	select intval from vitess_test;
	select intval from vitess_test;
	select intval from vitess_test;
END;`}

func TestCallProcedure(t *testing.T) {
	client := framework.NewClient()
	_, err := client.ReserveExecute("select 1", nil, nil)
	require.NoError(t, err)
	defer client.Release()

	type testcases struct {
		query string
		rc    int
	}
	tcases := []testcases{{
		query: "call proc1()",
		rc:    1 + 1,
	}, {
		query: "call proc4()",
		rc:    4 + 1,
	}}

	for _, tc := range tcases {
		t.Run(tc.query, func(t *testing.T) {
			qr, err := client.Execute(tc.query, nil)
			require.NoError(t, err)
			localCnt := 1
			for qr.More {
				qr, err = client.FetchNext()
				require.NoError(t, err)
				localCnt++
			}
			assert.Equal(t, tc.rc, localCnt)
		})
	}
}
