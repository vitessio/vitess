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

package normalize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
)

func TestNormalizeAllFields(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	insertQuery := `insert into t1 values (1, "chars", "variable chars", x'73757265', 0x676F, 0.33, 9.99, 1, "1976-06-08", "small", "b", "{\"key\":\"value\"}", point(1,5), b'011', 0b0101)`
	normalizedInsertQuery := `insert into t1 values (:vtg1 /* INT64 */, :vtg2 /* VARCHAR */, :vtg3 /* VARCHAR */, :vtg4 /* HEXVAL */, :vtg5 /* HEXNUM */, :vtg6 /* DECIMAL */, :vtg7 /* DECIMAL */, :vtg8 /* INT64 */, :vtg9 /* VARCHAR */, :vtg10 /* VARCHAR */, :vtg11 /* VARCHAR */, :vtg12 /* VARCHAR */, point(:vtg13 /* INT64 */, :vtg14 /* INT64 */), :vtg15 /* BITNUM */, :vtg16 /* BITNUM */)`
	vtgateVersion, err := cluster.GetMajorVersion("vtgate")
	require.NoError(t, err)
	if vtgateVersion < 19 {
		normalizedInsertQuery = `insert into t1 values (:vtg1 /* INT64 */, :vtg2 /* VARCHAR */, :vtg3 /* VARCHAR */, :vtg4 /* HEXVAL */, :vtg5 /* HEXNUM */, :vtg6 /* DECIMAL */, :vtg7 /* DECIMAL */, :vtg8 /* INT64 */, :vtg9 /* VARCHAR */, :vtg10 /* VARCHAR */, :vtg11 /* VARCHAR */, :vtg12 /* VARCHAR */, point(:vtg13 /* INT64 */, :vtg14 /* INT64 */), :vtg15 /* HEXNUM */, :vtg16 /* HEXNUM */)`
	}
	selectQuery := "select * from t1"
	utils.Exec(t, conn, insertQuery)
	qr := utils.Exec(t, conn, selectQuery)
	assert.Equal(t, 1, len(qr.Rows), "wrong number of table rows, expected 1 but had %d. Results: %v", len(qr.Rows), qr.Rows)

	// Now need to figure out the best way to check the normalized query in the planner cache...
	results := getPlanCache(t, fmt.Sprintf("%s:%d", vtParams.Host, clusterInstance.VtgateProcess.Port))
	assert.Contains(t, results, normalizedInsertQuery)
}

func getPlanCache(t *testing.T, vtgateHostPort string) map[string]any {
	var results map[string]any
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/debug/query_plans", vtgateHostPort))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	err = json.Unmarshal(body, &results)
	require.NoErrorf(t, err, "failed to unmarshal results. contents:\n%s\n\n", body)

	return results
}
