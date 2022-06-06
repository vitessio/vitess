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

package endtoend

import (
	"regexp"
	"strconv"
	"testing"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

var gtidExp = regexp.MustCompile(`(\w{8}(-\w{4}){3}-\w{12}(:\d+(-\d+)?)+)`)

func TestGtidSet(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval = 5", nil)

	_, err := client.Execute(`insert into vitess_test (intval, floatval, charval, binval) values (5, null, null, null)`, nil)
	require.NoError(t, err)

	qr, err := client.Execute(`select @@global.gtid_executed`, nil)
	require.NoError(t, err)

	gtidSet := qr.Rows[0][0].ToString()

	start := time.Now()
	_, err = client.ExecuteWithOptions(`select 1`, nil, &querypb.ExecuteOptions{GtidSet: gtidSet})
	require.NoError(t, err)
	// no waiting as it is an existing gtid_set
	require.Less(t, time.Now().Sub(start), time.Second)

	// get gtid_set that is not yet executed on the mysql.
	gtidSet = getNextGtidSet(t, gtidSet, 100)

	start = time.Now()
	_, err = client.ExecuteWithOptions(`select 1`, nil, &querypb.ExecuteOptions{GtidSet: gtidSet})
	require.NoError(t, err)
	// will wait for timeout as gtid_set does not exists.
	require.Greater(t, time.Now().Sub(start), time.Second)
}

func getNextGtidSet(t *testing.T, gtidSet string, next int) string {
	m := gtidExp.FindStringSubmatch(gtidSet)

	lv := m[len(m)-1]
	rLen := len(lv)
	i, err := strconv.Atoi(lv)
	require.NoError(t, err)

	i = i - next
	si := strconv.Itoa(i)
	return gtidSet[0:len(gtidSet)-rLen] + si
}
